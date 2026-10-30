package offload

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// rayPodSetsOf are the PodSets Kueue builds for a Ray owner: the head, one
// worker group of workers replicas and, for a RayJob in K8sJobMode, the
// submitter, each admitted on flavor.
func rayPodSetsOf(workers int32, submitter bool, flavor kueue.ResourceFlavorReference) ([]kueue.PodSet, []kueue.PodSetAssignment) {
	head := podTemplate()
	head.Spec.Containers[0].Name = "ray-head"
	worker := podTemplate()
	worker.Spec.Containers[0].Name = "ray-worker"
	podSets := []kueue.PodSet{{Name: rayHeadPodSet, Count: 1, Template: head}, {Name: "gpu", Count: workers, Template: worker}}
	if submitter {
		sub := corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-job-submitter", Image: "ghcr.io/example/train:1.4"}}}}
		podSets = append(podSets, kueue.PodSet{Name: raySubmitterPodSet, Count: 1, Template: sub})
	}
	var assignments []kueue.PodSetAssignment
	for _, ps := range podSets {
		f := flavor
		if ps.Name == raySubmitterPodSet {
			// The submitter asks for CPU only, which Kueue may admit elsewhere.
			f = "hpc-a-cpu"
		}
		assignments = append(assignments, kueue.PodSetAssignment{Name: ps.Name, Count: ptr.To(ps.Count),
			Flavors: map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": f, "nvidia.com/gpu": f}})
	}
	return podSets, assignments
}

// rayOwned makes the Workload a Ray owner's, stored unstructured as the
// offload controller reads it, with the PodSets Kueue builds for it.
func rayOwned(k ownerKind, workers int32, owner func(u *unstructured.Unstructured)) envOption {
	return func(objs *[]client.Object, funcs *interceptor.Funcs) {
		customOwned(k)(objs, funcs)
		for _, o := range *objs {
			switch o := o.(type) {
			case *unstructured.Unstructured:
				if owner != nil {
					owner(o)
				}
			case *kueue.Workload:
				o.Spec.PodSets, o.Status.Admission.PodSetAssignments = rayPodSetsOf(workers, k == rayJobKind, "hpc-a-gpu")
			}
		}
	}
}

// rayStatus sends the RayJob status patches to the object: KubeRay's CRD has a
// status subresource, which the fake client knows only for registered types.
func rayStatus(_ *[]client.Object, funcs *interceptor.Funcs) {
	funcs.SubResourcePatch = func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
		if u, ok := obj.(*unstructured.Unstructured); ok && sub == "status" && u.GetKind() == rayJobKind.gvk.Kind {
			return c.Patch(ctx, obj, patch)
		}
		return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
	}
}

func rayWorkload(k ownerKind, workers int32, submitter bool) *kueue.Workload {
	wl := workload()
	wl.OwnerReferences = []metav1.OwnerReference{{APIVersion: k.gvk.GroupVersion().String(), Kind: k.gvk.Kind,
		Name: "train", UID: customUID, Controller: ptr.To(true)}}
	wl.Spec.PodSets, wl.Status.Admission.PodSetAssignments = rayPodSetsOf(workers, submitter, "hpc-a-gpu")
	return wl
}

func podSetNames(podSets []v1alpha1.PodSet) []string {
	var out []string
	for _, ps := range podSets {
		out = append(out, string(ps.Name))
	}
	return out
}

func portNumbers(c corev1.Container) []int32 {
	var out []int32
	for _, p := range c.Ports {
		out = append(out, p.ContainerPort)
	}
	return out
}

// TestRayJobPodSets: the submitter, admitted on another flavor, does not run;
// head and workers name the ray-job launcher, and the head keeps its own ports
// and labels.
func TestRayJobPodSets(t *testing.T) {
	wl := rayWorkload(rayJobKind, 2, true)
	wl.Spec.PodSets[0].Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "dash", ContainerPort: 8265}}
	podSets, err := buildPodSets(wl, resolveSite(offloadConfig()), "proj42", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := podSetNames(podSets); !slices.Equal(got, []string{"head", "gpu"}) {
		t.Fatalf("podSets %v, want head and gpu", got)
	}
	for _, ps := range podSets {
		if got := ps.Template.Annotations[launcherAnnotation]; got != RayJobLauncher {
			t.Errorf("podSet %s launcher %q", ps.Name, got)
		}
		if got := ps.Annotations[flagsKey]; got != "--account=proj42 --partition=gpu" {
			t.Errorf("podSet %s flags %q", ps.Name, got)
		}
	}
	if got := portNumbers(podSets[0].Template.Spec.Containers[0]); !slices.Equal(got, []int32{8265}) {
		t.Errorf("head ports %v, want the template's", got)
	}
	if len(podSets[1].Template.Spec.Containers[0].Ports) != 0 {
		t.Errorf("worker ports %v", podSets[1].Template.Spec.Containers[0].Ports)
	}
	if _, ok := podSets[0].Template.Labels["ray.io/cluster"]; ok {
		t.Errorf("a RayJob's head got cluster labels: %v", podSets[0].Template.Labels)
	}
	if len(wl.Spec.PodSets) != 3 || wl.Spec.PodSets[0].Template.Annotations[launcherAnnotation] != "" {
		t.Errorf("the Workload was changed")
	}
}

// TestRayClusterPodSets: the head carries KubeRay's head pod labels, the
// template's own winning except on the cluster, node type and group, and
// declares the dashboard and client ports once; every PodSet names the ray
// launcher, unless its template names another.
func TestRayClusterPodSets(t *testing.T) {
	wl := rayWorkload(rayClusterKind, 1, false)
	wl.Spec.PodSets[0].Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "dash", ContainerPort: 8265}}
	wl.Spec.PodSets[0].Template.Labels = map[string]string{"app.kubernetes.io/name": "mine", "ray.io/node-type": "worker", "team": "ml"}
	wl.Spec.PodSets[1].Template.Annotations[launcherAnnotation] = "ray-ib"
	podSets, err := buildPodSets(wl, resolveSite(offloadConfig()), "proj42", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"ray.io/is-ray-node": "yes", "ray.io/cluster": "train", "ray.io/node-type": "head", "ray.io/group": "headgroup",
		"ray.io/identifier": "train-head", "app.kubernetes.io/name": "mine", "app.kubernetes.io/created-by": "kuberay-operator", "team": "ml",
	}
	if got := podSets[0].Template.Labels; !reflect.DeepEqual(got, want) {
		t.Errorf("head labels %v, want %v", got, want)
	}
	if podSets[1].Template.Labels != nil {
		t.Errorf("worker labels %v", podSets[1].Template.Labels)
	}
	if got := podSets[0].Template.Annotations[launcherAnnotation]; got != RayClusterLauncher {
		t.Errorf("head launcher %q", got)
	}
	if got := podSets[1].Template.Annotations[launcherAnnotation]; got != "ray-ib" {
		t.Errorf("worker launcher %q, want the template's", got)
	}
	if got := portNumbers(podSets[0].Template.Spec.Containers[0]); !slices.Equal(got, []int32{8265, 10001}) {
		t.Errorf("head ports %v", got)
	}
}

func TestKubeRayLabelValue(t *testing.T) {
	long := strings.Repeat("a", 60) + "-head"
	if got := kubeRayLabelValue(long); got != long[2:] {
		t.Errorf("%q, want the last 63 characters", got)
	}
	cut := "abc-" + strings.Repeat("c", 57) + "-head"
	if got := kubeRayLabelValue(cut); got != "r"+cut[4:] {
		t.Errorf("%q, want the leading hyphen replaced", got)
	}
	if got := kubeRayLabelValue("train-head"); got != "train-head" {
		t.Errorf("%q", got)
	}
}

func TestRayHeadAloneRefused(t *testing.T) {
	for _, k := range []ownerKind{rayJobKind, rayClusterKind} {
		t.Run(k.gvk.Kind, func(t *testing.T) {
			wl := rayWorkload(k, 0, k == rayJobKind)
			_, err := buildPodSets(wl, resolveSite(offloadConfig()), "proj42", nil)
			if err == nil || !strings.Contains(err.Error(), "needs a head and at least one worker") {
				t.Errorf("err %v", err)
			}
		})
	}
}

func rayJobObject(spec map[string]any) *unstructured.Unstructured {
	u := newUnstructured(rayJobKind)
	u.SetName("train")
	u.Object["spec"] = spec
	return u
}

// TestOwnerPodSets: every PodSet's first container submits a RayJob's
// entrypoint to the head's job server, with what the RayJob asks of it.
func TestOwnerPodSets(t *testing.T) {
	wl := rayWorkload(rayJobKind, 1, true)
	wl.Spec.PodSets[1].Template.Spec.Containers[0].Args = []string{"--old"}
	podSets, err := buildPodSets(wl, resolveSite(offloadConfig()), "proj42", nil)
	if err != nil {
		t.Fatal(err)
	}
	podSets[0].Template.Spec.Containers = append(podSets[0].Template.Spec.Containers, corev1.Container{Name: "sidecar"})
	owner := rayJobObject(map[string]any{
		"entrypoint":          "python /data/train.py --epochs 2 && echo $(hostname) $$",
		"runtimeEnvYAML":      "env_vars:\n  RAY_DEDUP_LOGS: \"0\"\nconda: torch\nconfig: {setup_timeout_seconds: 600}\n",
		"metadata":            map[string]any{"team": "ml"},
		"jobId":               "train-1",
		"entrypointNumCpus":   int64(2),
		"entrypointNumGpus":   0.5,
		"entrypointResources": `{"head": 1}`,
	})
	if err := ownerPodSets(owner, podSets); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"entrypoint":           "python /data/train.py --epochs 2 && echo $(hostname) $$",
		"runtime_env":          map[string]any{"env_vars": map[string]any{"RAY_DEDUP_LOGS": "0"}, "conda": "torch", "config": map[string]any{"setup_timeout_seconds": 600.0}},
		"metadata":             map[string]any{"team": "ml"},
		"submission_id":        "train-1",
		"entrypoint_num_cpus":  2.0,
		"entrypoint_num_gpus":  0.5,
		"entrypoint_resources": map[string]any{"head": 1.0},
	}
	for _, ps := range podSets {
		c := ps.Template.Spec.Containers[0]
		// The $(VAR) expansion of a container's command.
		command := slices.Clone(c.Command)
		for i := range command {
			command[i] = strings.ReplaceAll(command[i], "$$", "$")
		}
		if len(command) != 6 || !slices.Equal(command[:5], []string{"bash", "-lc", `exec python -c "$1" "$2"`, "ray-job-submitter", rayJobSubmitter}) || c.Args != nil {
			t.Fatalf("podSet %s command %q args %v", ps.Name, c.Command, c.Args)
		}
		var got map[string]any
		if err := json.Unmarshal([]byte(command[5]), &got); err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("podSet %s submits %v (%v), want %v", ps.Name, got, err, want)
		}
	}
	if side := podSets[0].Template.Spec.Containers[1]; side.Command != nil {
		t.Errorf("sidecar command %v", side.Command)
	}

	if err := ownerPodSets(ownerJob(), podSets); err != nil {
		t.Errorf("a Job: %v", err)
	}
	cluster := newUnstructured(rayClusterKind)
	if err := ownerPodSets(cluster, podSets); err != nil {
		t.Errorf("a RayCluster: %v", err)
	}
}

// TestRayJobSubmitter runs a RayJob's command against a Ray job client that
// prints what it is asked: the job is submitted as it is, its logs are
// printed, and the command fails when the job did.
func TestRayJobSubmitter(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("no python3")
	}
	dir := t.TempDir()
	files := map[string]string{
		"bin/python":      "#!/bin/sh\nexec python3 \"$@\"\n",
		"ray/__init__.py": "",
		"ray/job_submission.py": `import enum, json, os, time

# The job is RUNNING once: no wait between two polls.
time.sleep = lambda seconds: None


class JobStatus(str, enum.Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

    def is_terminal(self):
        return self != JobStatus.RUNNING


class JobSubmissionClient:
    statuses = ["RUNNING"] + [os.environ["FAKE_STATUS"]]

    def submit_job(self, **kwargs):
        print(json.dumps(kwargs, sort_keys=True))
        return "raysubmit_1"

    async def tail_job_logs(self, job):
        yield "driver output\n"

    def get_job_status(self, job):
        return JobStatus(self.statuses.pop(0))
`,
	}
	for name, body := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	podSets := []v1alpha1.PodSet{{Name: "head", Count: 1, Template: podTemplate()}}
	owner := rayJobObject(map[string]any{"entrypoint": `python -c "print('it''s $HOME')" && echo $$`})
	if err := ownerPodSets(owner, podSets); err != nil {
		t.Fatal(err)
	}
	command := slices.Clone(podSets[0].Template.Spec.Containers[0].Command)
	for i := range command {
		command[i] = strings.ReplaceAll(command[i], "$$", "$")
	}
	// -c: the test host's login profile is not the image's.
	command[1] = "-c"
	for _, tc := range []struct {
		status string
		fails  bool
	}{{"SUCCEEDED", false}, {"FAILED", true}} {
		cmd := exec.Command(command[0], command[1:]...)
		cmd.Env = []string{"PATH=" + filepath.Join(dir, "bin") + ":/usr/bin:/bin", "PYTHONPATH=" + dir, "FAKE_STATUS=" + tc.status}
		var stderr strings.Builder
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if (err != nil) != tc.fails {
			t.Logf("stderr %s", stderr.String())
			t.Errorf("%s: err %v, out %s", tc.status, err, out)
		}
		want := `{"entrypoint": "python -c \"print('it''s $HOME')\" && echo $$"}` + "\ndriver output\nRay job raysubmit_1 " + tc.status + "\n"
		if string(out) != want {
			t.Errorf("%s: out\n%s\nwant\n%s", tc.status, out, want)
		}
	}
}

func TestOwnerPodSetsRefused(t *testing.T) {
	cases := []struct {
		name string
		spec map[string]any
		want string
	}{
		{"no entrypoint", map[string]any{"submissionMode": "InteractiveMode"}, "no spec.entrypoint"},
		{"not yaml", map[string]any{"entrypoint": "python x.py", "runtimeEnvYAML": "env_vars: ["}, "spec.runtimeEnvYAML"},
		{"not a map", map[string]any{"entrypoint": "python x.py", "runtimeEnvYAML": "- pip"}, "spec.runtimeEnvYAML"},
		{"resources", map[string]any{"entrypoint": "python x.py", "entrypointResources": "head"}, "spec.entrypointResources"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			podSets := []v1alpha1.PodSet{{Name: "head", Count: 1, Template: podTemplate()}}
			err := ownerPodSets(rayJobObject(tc.spec), podSets)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err %v, want %q", err, tc.want)
			}
		})
	}
}

// TestRayJobFinished: the RayJob gets the status KubeRay gives a job that
// ended, which Kueue's integration finishes the Workload on; it stays
// suspended and is not patched again.
func TestRayJobFinished(t *testing.T) {
	cases := []struct {
		name                    string
		finish                  func(e *env)
		deployment, job, reason string
		message                 string
	}{
		{"completed", func(e *env) { e.report(1, e.finished(true, "")) }, "Complete", "SUCCEEDED", "", "job 123 completed"},
		{"failed", func(e *env) { e.report(1, e.finished(false, "rank 1 exited 3")) }, "Failed", "FAILED", "AppFailed", "job 123 failed: rank 1 exited 3"},
		{"rejected", func(e *env) {
			e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
				e.condition(st, v1alpha1.OffloadRequestAccepted, metav1.ConditionFalse, "Refused", "invalid account")
			})
		}, "Failed", "FAILED", "SubmissionFailed", "invalid account"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, rayOwned(rayJobKind, 1, func(u *unstructured.Unstructured) {
				u.Object["spec"] = map[string]any{"suspend": true, "entrypoint": "python x.py"}
				u.Object["status"] = map[string]any{"jobDeploymentStatus": "Suspended"}
			}), rayStatus)
			e.submitted()
			if got := podSetNames(e.request(1).Spec.PodSets); !slices.Equal(got, []string{"head", "gpu"}) {
				t.Errorf("request podSets %v", got)
			}
			tc.finish(e)
			e.reconcile()
			u := e.custom(rayJobKind)
			st, _, _ := unstructured.NestedStringMap(u.Object, "status")
			if st["jobDeploymentStatus"] != tc.deployment || st["jobStatus"] != tc.job || st["reason"] != tc.reason || st["message"] != tc.message ||
				st["endTime"] != "2026-10-29T10:00:00Z" {
				t.Errorf("status %v, want %s %s %q %q", st, tc.deployment, tc.job, tc.reason, tc.message)
			}
			if suspend, _, _ := unstructured.NestedBool(u.Object, "spec", "suspend"); !suspend {
				t.Errorf("the RayJob was resumed")
			}
			rv := u.GetResourceVersion()
			e.now = e.now.Add(time.Minute)
			e.reconcile()
			if u := e.custom(rayJobKind); u.GetResourceVersion() != rv {
				t.Errorf("a terminal RayJob was patched again")
			}
		})
	}
}

// TestRayClusterRunEnds: a RayCluster's Workload is deactivated when its job
// ends; a rejected one is left to Kueue, which deactivates it on the check.
func TestRayClusterRunEnds(t *testing.T) {
	e := newEnv(t, rayOwned(rayClusterKind, 1, nil))
	e.submitted()
	e.report(1, e.finished(false, "DUE TO TIME LIMIT"))
	e.reconcile()
	if wl := e.workload(); ptr.Deref(wl.Spec.Active, true) {
		t.Errorf("Workload still active after its job ended")
	}
	if conds, _, _ := unstructured.NestedSlice(e.custom(rayClusterKind).Object, "status", "conditions"); len(conds) != 0 {
		t.Errorf("RayCluster patched: %v", conds)
	}

	e = newEnv(t, rayOwned(rayClusterKind, 1, nil))
	e.reconcile()
	e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
		e.condition(st, v1alpha1.OffloadRequestAccepted, metav1.ConditionFalse, "Refused", "invalid account")
	})
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateRejected {
		t.Errorf("check %s", st.State)
	}
	if wl := e.workload(); !ptr.Deref(wl.Spec.Active, true) {
		t.Errorf("a rejected RayCluster's Workload deactivated by the offload controller")
	}
}
