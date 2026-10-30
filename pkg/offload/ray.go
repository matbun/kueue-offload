package offload

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/yaml"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// KubeRay's RayJob and RayCluster run as one batch job. KubeRay runs nothing:
// Kueue keeps the owner suspended until its Workload is admitted, which the
// offload check never lets happen, and the site's launcher builds the Ray
// cluster from the allocation, head on the first node.

// +kubebuilder:rbac:groups=ray.io,resources=rayjobs;rayclusters,verbs=get
// +kubebuilder:rbac:groups=ray.io,resources=rayjobs/status,verbs=patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=patch

// Site launchers (interlink.eu/launcher) Ray owners run with, unless their pod
// templates name another. RayJobLauncher starts the cluster and runs the head's
// command on it; RayClusterLauncher starts the cluster and blocks on the head.
const (
	RayJobLauncher     = "ray-job"
	RayClusterLauncher = "ray"
)

const (
	launcherAnnotation = "interlink.eu/launcher"
	// Kueue's PodSet names (pkg/controller/jobs/rayjob, raycluster).
	rayHeadPodSet      = kueue.PodSetReference("head")
	raySubmitterPodSet = kueue.PodSetReference("submitter")
)

// rayOwnerOf returns the Workload's controller when it is a RayJob or a
// RayCluster.
func rayOwnerOf(wl *kueue.Workload) *metav1.OwnerReference {
	c := metav1.GetControllerOf(wl)
	if c == nil {
		return nil
	}
	if k, ok := kindOf(c); ok && (k == rayJobKind || k == rayClusterKind) {
		return c
	}
	return nil
}

// runPodSets are the Workload's PodSets that run, with their assignments: all
// of them, less a RayJob's submitter, which would submit the entrypoint from the
// cluster to the head; the head runs it itself.
func runPodSets(wl *kueue.Workload) ([]kueue.PodSet, []kueue.PodSetAssignment) {
	var assignments []kueue.PodSetAssignment
	if wl.Status.Admission != nil {
		assignments = wl.Status.Admission.PodSetAssignments
	}
	if c := rayOwnerOf(wl); c == nil || c.Kind != rayJobKind.gvk.Kind {
		return wl.Spec.PodSets, assignments
	}
	podSets := slices.DeleteFunc(slices.Clone(wl.Spec.PodSets), func(ps kueue.PodSet) bool { return ps.Name == raySubmitterPodSet })
	assignments = slices.DeleteFunc(slices.Clone(assignments), func(a kueue.PodSetAssignment) bool { return a.Name == raySubmitterPodSet })
	return podSets, assignments
}

// Ports of a RayCluster's head that the shadow forwards: KubeRay's head
// Service exposes them, and a head template declares none of them by default.
// A RayJob's head gets none, so that it runs where shadows cannot tunnel; its
// template declares the dashboard port to reach it.
var rayHeadPorts = []corev1.ContainerPort{
	{Name: "dashboard", ContainerPort: 8265, Protocol: corev1.ProtocolTCP},
	{Name: "client", ContainerPort: 10001, Protocol: corev1.ProtocolTCP},
}

// rayPodSets prepares the PodSets of a Ray owner: every template names the
// site launcher, unless it names one. A RayCluster's head declares the
// dashboard and client ports in its first container, which the shadow then
// forwards, and carries the labels KubeRay gives the head pod, so that the
// shadow is published to its head Service <name>-head-svc. A Ray owner of its
// head alone is refused: the executor makes a pod group, which the launcher
// needs, of two members or more.
func rayPodSets(c *metav1.OwnerReference, podSets []v1alpha1.PodSet) error {
	launcher := RayJobLauncher
	if c.Kind == rayClusterKind.gvk.Kind {
		launcher = RayClusterLauncher
	}
	var members int32
	for i := range podSets {
		members += podSets[i].Count
		t := &podSets[i].Template
		if t.Annotations[launcherAnnotation] == "" {
			if t.Annotations == nil {
				t.Annotations = map[string]string{}
			}
			t.Annotations[launcherAnnotation] = launcher
		}
		if c.Kind != rayClusterKind.gvk.Kind || podSets[i].Name != rayHeadPodSet || len(t.Spec.Containers) == 0 {
			continue
		}
		ctr := &t.Spec.Containers[0]
		for _, p := range rayHeadPorts {
			if !slices.ContainsFunc(ctr.Ports, func(q corev1.ContainerPort) bool { return q.ContainerPort == p.ContainerPort }) {
				ctr.Ports = append(ctr.Ports, p)
			}
		}
		t.Labels = rayHeadLabels(c.Name, t.Labels)
	}
	if members < 2 {
		return fmt.Errorf("%s %s runs as one batch job through the site's %q launcher, which needs a head and at least one worker: give it a worker group of one replica or more",
			c.Kind, c.Name, launcher)
	}
	return nil
}

// rayHeadLabels are the labels KubeRay gives the head pod of cluster
// (ray-operator common.labelPod): the template's, over KubeRay's own except
// ray.io/cluster, ray.io/node-type and ray.io/group.
func rayHeadLabels(cluster string, template map[string]string) map[string]string {
	out := map[string]string{
		"ray.io/is-ray-node":           "yes",
		"ray.io/identifier":            kubeRayLabelValue(cluster + "-head"),
		"app.kubernetes.io/name":       "kuberay",
		"app.kubernetes.io/created-by": "kuberay-operator",
	}
	maps.Copy(out, template)
	out["ray.io/cluster"] = cluster
	out["ray.io/node-type"] = "head"
	out["ray.io/group"] = "headgroup"
	return out
}

// kubeRayLabelValue shortens a label value as KubeRay does (utils.CheckLabel):
// its last 63 characters, a leading punctuation mark replaced by r.
func kubeRayLabelValue(s string) string {
	if len(s) > 63 {
		s = s[len(s)-63:]
	}
	if s != "" && unicode.IsPunct(rune(s[0])) {
		s = "r" + s[1:]
	}
	return s
}

// ownerPodSets completes podSets with what only the owner object holds. A
// RayJob's entrypoint is submitted to the Ray job server of the head, as
// KubeRay submits it in HTTPMode: the entrypoint as it is, with the runtime
// environment, metadata, submission ID and entrypoint resources of the RayJob.
// Every PodSet's first container runs rayJobSubmitter: the launcher runs the
// head's on the head node with RAY_ADDRESS set, through which Ray's job client
// finds the job server, and wants one command for every PodSet. bash -lc, as
// KubeRay starts Ray containers, so that the image's login environment holds.
// Every $ is doubled, which the $(VAR) expansion of a container's command
// turns back into one. An error means the Workload can never run.
func ownerPodSets(owner metav1.Object, podSets []v1alpha1.PodSet) error {
	u, ok := owner.(*unstructured.Unstructured)
	if !ok || u.GetAPIVersion() != rayJobKind.gvk.GroupVersion().String() || u.GetKind() != rayJobKind.gvk.Kind {
		return nil
	}
	job, err := rayJobSubmission(u)
	if err != nil {
		return fmt.Errorf("RayJob %s: %w", u.GetName(), err)
	}
	for i := range podSets {
		spec := &podSets[i].Template.Spec
		if len(spec.Containers) == 0 {
			continue
		}
		command := []string{"bash", "-lc", `exec python -c "$1" "$2"`, "ray-job-submitter", rayJobSubmitter, job}
		for j := range command {
			command[j] = strings.ReplaceAll(command[j], "$", "$$")
		}
		spec.Containers[0].Command = command
		spec.Containers[0].Args = nil
	}
	return nil
}

// rayJobSubmitter submits the job of its argument, the keyword arguments of
// JobSubmissionClient.submit_job, once the job server answers, prints its logs
// until it ends, and exits 0 if it succeeded.
const rayJobSubmitter = `import asyncio, json, sys, time
from ray.job_submission import JobStatus, JobSubmissionClient

for attempt in range(60):
    try:
        client = JobSubmissionClient()
        break
    except Exception:
        if attempt == 59:
            raise
        time.sleep(2)
job = client.submit_job(**json.loads(sys.argv[1]))


async def tail():
    async for lines in client.tail_job_logs(job):
        print(lines, end="", flush=True)


asyncio.new_event_loop().run_until_complete(tail())
status = client.get_job_status(job)
while not status.is_terminal():
    time.sleep(5)
    status = client.get_job_status(job)
print(f"Ray job {job} {status.value}", flush=True)
sys.exit(0 if status == JobStatus.SUCCEEDED else 1)
`

// rayJobSubmission is the submit_job arguments of a RayJob, as JSON.
func rayJobSubmission(u *unstructured.Unstructured) (string, error) {
	entrypoint, _, _ := unstructured.NestedString(u.Object, "spec", "entrypoint")
	if strings.TrimSpace(entrypoint) == "" {
		return "", fmt.Errorf("no spec.entrypoint: a RayJob runs its entrypoint on the head; for a Ray cluster to submit to, use a RayCluster")
	}
	job := map[string]any{"entrypoint": strings.TrimSpace(entrypoint)}
	if runtimeEnv, _, _ := unstructured.NestedString(u.Object, "spec", "runtimeEnvYAML"); strings.TrimSpace(runtimeEnv) != "" {
		var parsed map[string]any
		if err := yaml.Unmarshal([]byte(runtimeEnv), &parsed); err != nil {
			return "", fmt.Errorf("spec.runtimeEnvYAML: %w", err)
		}
		if len(parsed) > 0 {
			job["runtime_env"] = parsed
		}
	}
	if metadata, _, _ := unstructured.NestedStringMap(u.Object, "spec", "metadata"); len(metadata) > 0 {
		job["metadata"] = metadata
	}
	if id, _, _ := unstructured.NestedString(u.Object, "spec", "jobId"); id != "" {
		job["submission_id"] = id
	}
	for field, arg := range map[string]string{"entrypointNumCpus": "entrypoint_num_cpus", "entrypointNumGpus": "entrypoint_num_gpus"} {
		if v, ok, _ := unstructured.NestedFieldNoCopy(u.Object, "spec", field); ok && v != nil && fmt.Sprint(v) != "0" {
			job[arg] = v
		}
	}
	if resources, _, _ := unstructured.NestedString(u.Object, "spec", "entrypointResources"); strings.TrimSpace(resources) != "" {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(resources), &parsed); err != nil {
			return "", fmt.Errorf("spec.entrypointResources: %w", err)
		}
		job["entrypoint_resources"] = parsed
	}
	raw, err := json.Marshal(job)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// finishRayJob patches a RayJob's status as KubeRay reports a job that ended:
// jobDeploymentStatus Complete or Failed, which Kueue's integration finishes
// the Workload on, and jobStatus SUCCEEDED or FAILED, which it reads for
// success. KubeRay keeps that status on a suspended RayJob and counts
// ttlSecondsAfterFinished from endTime. RayJobs already terminal or gone are
// skipped.
func finishRayJob(ctx context.Context, c client.Client, r client.Reader, key types.NamespacedName, uid types.UID, o jobOutcome, now time.Time) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(rayJobKind.gvk)
	if err := r.Get(ctx, key, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if uid != "" && obj.GetUID() != uid {
		return nil
	}
	if s, _, _ := unstructured.NestedString(obj.Object, "status", "jobDeploymentStatus"); s == "Complete" || s == "Failed" {
		return nil
	}
	status := map[string]any{"jobDeploymentStatus": "Complete", "jobStatus": "SUCCEEDED"}
	switch {
	case o.reason == ReasonRejected:
		status = map[string]any{"jobDeploymentStatus": "Failed", "jobStatus": "FAILED", "reason": "SubmissionFailed"}
	case !o.succeeded:
		status = map[string]any{"jobDeploymentStatus": "Failed", "jobStatus": "FAILED", "reason": "AppFailed"}
	}
	msg := o.message
	if len(msg) > 32768 {
		msg = msg[:32768]
	}
	status["message"] = msg
	end := now
	if o.completionTime != nil {
		end = o.completionTime.Time
	}
	status["endTime"] = end.UTC().Format(time.RFC3339)
	if o.startTime != nil {
		status["startTime"] = o.startTime.UTC().Format(time.RFC3339)
	}
	done := obj.DeepCopy()
	for k, v := range status {
		if err := unstructured.SetNestedField(done.Object, v, "status", k); err != nil {
			return err
		}
	}
	return client.IgnoreNotFound(c.Status().Patch(ctx, done, client.MergeFrom(obj)))
}

// deactivateWorkload ends the run of a RayCluster, which Kueue's integration
// never finishes: the Workload is deactivated, as a user stops one, and Kueue
// evicts it and releases its quota; the request goes with the eviction, and
// with it the shadow. Setting spec.active again starts the cluster anew.
func deactivateWorkload(ctx context.Context, c client.Client, r client.Reader, key types.NamespacedName, uid types.UID) error {
	wl := &kueue.Workload{}
	if err := r.Get(ctx, key, wl); err != nil {
		return client.IgnoreNotFound(err)
	}
	if wl.UID != uid || !ptr.Deref(wl.Spec.Active, true) {
		return nil
	}
	inactive := wl.DeepCopy()
	inactive.Spec.Active = ptr.To(false)
	err := c.Patch(ctx, inactive, client.MergeFrom(wl))
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
