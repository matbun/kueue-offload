package offload

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

var groupPods = []string{"train-0", "train-1"}

func TestPodGroupSubmitsOnce(t *testing.T) {
	e := newEnv(t, podOwned(2))
	e.reconcile()
	e.reconcile()
	e.reconcile()
	if n := len(e.requests()); n != 1 {
		t.Fatalf("requests = %d, want 1", n)
	}
	sp := e.request(1).Spec
	if sp.Owner != (v1alpha1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: podGroup, UID: "0b6c-pod-0"}) {
		t.Errorf("owner = %+v, want the group name and the first pod's UID", sp.Owner)
	}
	if len(sp.PodSets) != 1 || sp.PodSets[0].Name != podSetKey || sp.PodSets[0].Count != 2 {
		t.Fatalf("podSets = %+v, want one PodSet of count 2", sp.PodSets)
	}
	if want := map[string]string{"slurm-job.vk.io/flags": "--time=01:00:00", "interlink.eu/mode": "owner"}; !reflect.DeepEqual(sp.OwnerAnnotations, want) {
		t.Errorf("ownerAnnotations = %v, want %v", sp.OwnerAnnotations, want)
	}
	for _, name := range groupPods {
		if p := e.pod(name); p.Status.Phase != corev1.PodPending || len(p.Spec.SchedulingGates) != 1 {
			t.Errorf("pod %s changed while queued: %s, gates %v", name, p.Status.Phase, p.Spec.SchedulingGates)
		}
	}
}

func TestPodOwnersTerminalPhase(t *testing.T) {
	cases := []struct {
		name      string
		succeeded bool
		detail    string
		phase     corev1.PodPhase
		message   string
	}{
		{"completed", true, "", corev1.PodSucceeded, "job 123 completed"},
		{"failed", false, "rank 1 exited 3", corev1.PodFailed, "job 123 failed: rank 1 exited 3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, podOwned(2))
			e.submitted()
			e.report(1, e.finished(tc.succeeded, tc.detail))
			e.reconcile()
			for _, name := range groupPods {
				p := e.pod(name)
				if p.Status.Phase != tc.phase || p.Status.Reason != ReasonOffload || p.Status.Message != tc.message {
					t.Errorf("pod %s = %s %s %q, want %s %s %q", name, p.Status.Phase, p.Status.Reason, p.Status.Message,
						tc.phase, ReasonOffload, tc.message)
				}
				if p.Spec.NodeName != "" || len(p.Spec.SchedulingGates) != 1 {
					t.Errorf("pod %s spec changed: node %q, gates %v", name, p.Spec.NodeName, p.Spec.SchedulingGates)
				}
			}
			if st := e.check("hpc-a"); st.State != kueue.CheckStatePending || st.Message != tc.message {
				t.Errorf("check = %s %q, want Pending until the Workload finishes", st.State, st.Message)
			}
			if len(e.job().Status.Conditions) != 0 {
				t.Errorf("an unrelated Job was patched")
			}
		})
	}
}

// recordPodPatches records the metadata and status patches sent for pods, in order.
func recordPodPatches(calls *[]string) envOption {
	return func(_ *[]client.Object, f *interceptor.Funcs) {
		f.Patch = func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if p, ok := obj.(*corev1.Pod); ok {
				*calls = append(*calls, "metadata "+p.Name)
			}
			return c.Patch(ctx, obj, patch, opts...)
		}
		f.SubResourcePatch = func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if p, ok := obj.(*corev1.Pod); ok {
				*calls = append(*calls, sub+" "+p.Name)
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		}
	}
}

// Kueue finishes a pod group only when every pod succeeded, or when no pod is
// active and one carries retriable-in-group "false"; without the annotation it
// waits for replacement pods.
func TestFailedPodGroupNotRetriable(t *testing.T) {
	var calls []string
	e := newEnv(t, podOwned(2), recordPodPatches(&calls))
	e.submitted()
	e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
		st.Ranks = []v1alpha1.RankStatus{{Rank: 0, State: v1alpha1.RankFailed, ExitCode: 3, Containers: []corev1.ContainerStatus{
			{Name: "train", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 3}}},
		}}}
		e.finished(false, "rank 0 exited 3")(st)
	})
	e.reconcile()
	for _, name := range groupPods {
		p := e.pod(name)
		if p.Annotations[podconstants.RetriableInGroupAnnotationKey] != podconstants.RetriableInGroupAnnotationValue {
			t.Errorf("pod %s annotations %v, want %s=%s", name, p.Annotations,
				podconstants.RetriableInGroupAnnotationKey, podconstants.RetriableInGroupAnnotationValue)
		}
		if p.Annotations["slurm-job.vk.io/flags"] != "--time=01:00:00" {
			t.Errorf("pod %s lost its annotations: %v", name, p.Annotations)
		}
		if p.Status.Phase != corev1.PodFailed || p.Status.ContainerStatuses[0].State.Terminated.ExitCode != 3 {
			t.Errorf("pod %s = %s %+v, want Failed with exit 3", name, p.Status.Phase, p.Status.ContainerStatuses)
		}
	}
	// The annotation goes first: a pod left Failed without it is skipped later.
	want := []string{"metadata train-0", "status train-0", "metadata train-1", "status train-1"}
	if !reflect.DeepEqual(calls, want) {
		t.Errorf("pod patches %v, want %v", calls, want)
	}
	calls = nil
	e.reconcile()
	if len(calls) != 0 {
		t.Errorf("pods patched again: %v", calls)
	}
}

func TestRetriableInGroupOnlyOnFailedGroups(t *testing.T) {
	cases := []struct {
		name      string
		pods      int
		succeeded bool
	}{
		{"completed group", 2, true},
		{"failed single pod", 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			e := newEnv(t, podOwned(tc.pods), recordPodPatches(&calls))
			e.submitted()
			e.report(1, e.finished(tc.succeeded, ""))
			e.reconcile()
			for _, c := range calls {
				if strings.HasPrefix(c, "metadata ") {
					t.Errorf("unexpected %s", c)
				}
			}
			if _, ok := e.pod("train-0").Annotations[podconstants.RetriableInGroupAnnotationKey]; ok {
				t.Errorf("train-0 annotated retriable-in-group")
			}
		})
	}
}

func TestTerminalPodNotPatchedAgain(t *testing.T) {
	var patched []string
	e := newEnv(t, podOwned(2), func(objs *[]client.Object, f *interceptor.Funcs) {
		for _, o := range *objs {
			if p, ok := o.(*corev1.Pod); ok && p.Name == "train-0" {
				p.Status = corev1.PodStatus{Phase: corev1.PodSucceeded, Reason: "Earlier", Message: "ended before"}
			}
		}
		f.SubResourcePatch = func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if p, ok := obj.(*corev1.Pod); ok {
				patched = append(patched, p.Name)
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		}
	})
	e.submitted()
	e.report(1, e.finished(false, "rank 1 exited 3"))
	e.reconcile()
	if !reflect.DeepEqual(patched, []string{"train-1"}) {
		t.Errorf("patched pods %v, want only train-1", patched)
	}
	if p := e.pod("train-0"); p.Status.Phase != corev1.PodSucceeded || p.Status.Message != "ended before" {
		t.Errorf("terminal pod changed: %s %q", p.Status.Phase, p.Status.Message)
	}
	if p := e.pod("train-1"); p.Status.Phase != corev1.PodFailed {
		t.Errorf("train-1 = %s, want Failed", p.Status.Phase)
	}
}

func TestDeletedOwnerPodSkipped(t *testing.T) {
	e := newEnv(t, podOwned(2))
	e.submitted()
	if err := e.c.Delete(context.Background(), e.pod("train-1")); err != nil {
		t.Fatal(err)
	}
	e.report(1, e.finished(true, ""))
	e.reconcile() // fails the test on a reconcile error
	if p := e.pod("train-0"); p.Status.Phase != corev1.PodSucceeded {
		t.Errorf("train-0 = %s, want Succeeded", p.Status.Phase)
	}
}

func TestSinglePodOwner(t *testing.T) {
	e := newEnv(t, podOwned(1))
	e.submitted()
	sp := e.request(1).Spec
	if sp.Owner != (v1alpha1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "train-0", UID: "0b6c-pod-0"}) {
		t.Errorf("owner = %+v, want the pod", sp.Owner)
	}
	if sp.PodSets[0].Count != 1 {
		t.Errorf("count = %d, want 1", sp.PodSets[0].Count)
	}
	e.report(1, e.finished(true, ""))
	e.reconcile()
	if p := e.pod("train-0"); p.Status.Phase != corev1.PodSucceeded || p.Status.Reason != ReasonOffload {
		t.Errorf("pod = %s %s, want Succeeded %s", p.Status.Phase, p.Status.Reason, ReasonOffload)
	}
}

func TestPodOwnersRejectedOnPermanentError(t *testing.T) {
	e := newEnv(t, podOwned(2))
	const msg = "sbatch: error: Invalid account or account/partition combination specified"
	e.reconcile()
	e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
		e.condition(st, v1alpha1.OffloadRequestAccepted, metav1.ConditionFalse, "Refused", msg)
	})
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateRejected || st.Message != msg {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
	for _, name := range groupPods {
		if p := e.pod(name); p.Status.Phase != corev1.PodFailed || p.Status.Reason != ReasonRejected || p.Status.Message != msg {
			t.Errorf("pod %s = %s %s %q, want Failed %s", name, p.Status.Phase, p.Status.Reason, p.Status.Message, ReasonRejected)
		}
		if p := e.pod(name); p.Annotations[podconstants.RetriableInGroupAnnotationKey] != podconstants.RetriableInGroupAnnotationValue {
			t.Errorf("rejected pod %s not annotated retriable-in-group false", name)
		}
	}
}

func TestUnknownOwnerKindRejected(t *testing.T) {
	ray := metav1.OwnerReference{APIVersion: "ray.io/v1", Kind: "RayCluster", Name: "train", UID: "rc"}
	pod := metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "train-0", UID: "0b6c-pod-0"}
	cases := []struct {
		name   string
		owners []metav1.OwnerReference
		want   string
	}{
		{"controller", []metav1.OwnerReference{{APIVersion: ray.APIVersion, Kind: ray.Kind, Name: ray.Name, UID: ray.UID, Controller: ptr.To(true)}}, "owner ray.io/v1 RayCluster"},
		{"next to a pod", []metav1.OwnerReference{pod, ray}, "owner ray.io/v1 RayCluster"},
		{"none", nil, "no owner"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, podOwned(1), func(objs *[]client.Object, _ *interceptor.Funcs) {
				for _, o := range *objs {
					if wl, ok := o.(*kueue.Workload); ok {
						wl.OwnerReferences = tc.owners
					}
				}
			})
			e.reconcile()
			st := e.check("hpc-a")
			if st.State != kueue.CheckStateRejected || !strings.Contains(st.Message, "TrainJob and v1 Pod") || !strings.Contains(st.Message, tc.want) {
				t.Errorf("check = %s %q, want Rejected naming %q", st.State, st.Message, tc.want)
			}
			if len(e.requests()) != 0 {
				t.Errorf("a request for a workload with %s", tc.want)
			}
			if p := e.pod("train-0"); p.Status.Phase != corev1.PodPending {
				t.Errorf("a pod of an unsupported workload was patched: %s", p.Status.Phase)
			}
		})
	}
}

// argoShaped gives the owner pods an init container and a second container,
// the shape of a workflow step.
func argoShaped(objs *[]client.Object, _ *interceptor.Funcs) {
	for _, o := range *objs {
		if p, ok := o.(*corev1.Pod); ok {
			p.Spec.InitContainers = []corev1.Container{{Name: "init", Image: "argoexec:v3"}}
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "wait", Image: "argoexec:v3"})
		}
	}
}

func TestPodOwnersContainerStatuses(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
	end := metav1.NewTime(time.Date(2026, 10, 29, 9, 30, 0, 0, time.UTC))
	terminated := func(name string, code int32) corev1.ContainerStatus {
		return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code}}}
	}
	cases := []struct {
		name      string
		succeeded bool
		ranks     []v1alpha1.RankStatus
		want      map[string]int32
	}{
		{"completed without ranks", true, nil, map[string]int32{"train": 0, "wait": 0}},
		{"failed without ranks", false, nil, map[string]int32{"train": 1, "wait": 1}},
		{"failed with exit codes", false,
			[]v1alpha1.RankStatus{{Rank: 0, Containers: []corev1.ContainerStatus{terminated("train", 3)}}, {Rank: 1, Containers: []corev1.ContainerStatus{terminated("wait", 9)}}},
			map[string]int32{"train": 3, "wait": 1}},
		{"completed with exit codes", true,
			[]v1alpha1.RankStatus{{Rank: 0, Containers: []corev1.ContainerStatus{terminated("train", 0), terminated("wait", 0)}}},
			map[string]int32{"train": 0, "wait": 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, podOwned(2), argoShaped)
			e.submitted()
			e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
				st.Ranks, st.StartTime, st.CompletionTime = tc.ranks, &start, &end
				e.finished(tc.succeeded, "")(st)
			})
			e.reconcile()
			for _, name := range groupPods {
				p := e.pod(name)
				if p.Spec.NodeName != "" {
					t.Errorf("pod %s got node %q", name, p.Spec.NodeName)
				}
				if len(p.Status.InitContainerStatuses) != 1 {
					t.Fatalf("pod %s init statuses %+v", name, p.Status.InitContainerStatuses)
				}
				statuses := append([]corev1.ContainerStatus{}, p.Status.InitContainerStatuses...)
				statuses = append(statuses, p.Status.ContainerStatuses...)
				want := map[string]int32{"init": 0}
				for k, v := range tc.want {
					want[k] = v
				}
				if len(statuses) != len(want) {
					t.Fatalf("pod %s statuses %+v, want %v", name, statuses, want)
				}
				for _, cs := range statuses {
					term := cs.State.Terminated
					if term == nil {
						t.Errorf("pod %s container %s not terminated: %+v", name, cs.Name, cs.State)
						continue
					}
					reason := "Completed"
					if want[cs.Name] != 0 {
						reason = "Error"
					}
					img := map[string]string{"init": "argoexec:v3", "wait": "argoexec:v3", "train": "ghcr.io/example/train:1.4"}[cs.Name]
					if term.ExitCode != want[cs.Name] || term.Reason != reason || cs.Image != img || cs.Ready || cs.RestartCount != 0 {
						t.Errorf("pod %s container %s = exit %d %s image %q ready %v, want exit %d %s image %q",
							name, cs.Name, term.ExitCode, term.Reason, cs.Image, cs.Ready, want[cs.Name], reason, img)
					}
					if !term.StartedAt.Equal(&start) || !term.FinishedAt.Equal(&end) {
						t.Errorf("pod %s container %s times %v %v", name, cs.Name, term.StartedAt, term.FinishedAt)
					}
				}
			}
		})
	}
}

// notebookPod makes the single owner pod a Kubeflow Notebook's, as the API
// server holds it gated: its StatefulSet's labels and the PodScheduled
// condition of a gated pod.
func notebookPod(objs *[]client.Object, _ *interceptor.Funcs) {
	for _, o := range *objs {
		if p, ok := o.(*corev1.Pod); ok {
			p.Labels["statefulset"] = "train"
			p.Labels["statefulset.kubernetes.io/pod-name"] = p.Name
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "SchedulingGated"}}
		}
	}
}

// countPodStatusPatches counts the status patches sent for pods.
func countPodStatusPatches(n *int) envOption {
	return func(_ *[]client.Object, f *interceptor.Funcs) {
		f.SubResourcePatch = func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				*n++
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		}
	}
}

func (e *env) shadow(ip string, ready bool) func(st *v1alpha1.OffloadRequestStatus) {
	return func(st *v1alpha1.OffloadRequestStatus) {
		st.Shadow = &v1alpha1.ShadowReference{Deployment: "shadow-train-0-alice", Service: "shadow-train-0-alice", PodIP: ip, Ready: ready}
	}
}

func podCondition(p *corev1.Pod, ty corev1.PodConditionType) *corev1.PodCondition {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == ty {
			return &p.Status.Conditions[i]
		}
	}
	return nil
}

// TestSinglePodShowsRunningJob: while the job runs the gated pod reports
// Running, as a kubelet would, Ready while the shadow is, with the shadow's IP,
// so its StatefulSet counts it ready.
func TestSinglePodShowsRunningJob(t *testing.T) {
	patches := 0
	e := newEnv(t, podOwned(1), notebookPod, argoShaped, countPodStatusPatches(&patches))
	e.submitted()
	if got := e.request(1).Spec.OwnerLabels; got["statefulset"] != "train" || got["kueue.x-k8s.io/queue-name"] != "hpc-a" {
		t.Errorf("ownerLabels %v, want the pod's", got)
	}
	if p := e.pod("train-0"); p.Status.Phase != corev1.PodPending || patches != 0 {
		t.Fatalf("queued job: pod %s, %d status patches", p.Status.Phase, patches)
	}

	start := metav1.NewTime(time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
	e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
		e.running("node07", &start)(st)
		e.shadow("10.244.1.7", false)(st)
	})
	e.reconcile()
	p := e.pod("train-0")
	if p.Status.Phase != corev1.PodRunning || p.Status.PodIP != "10.244.1.7" || len(p.Status.PodIPs) != 1 || p.Status.PodIPs[0].IP != "10.244.1.7" ||
		!p.Status.StartTime.Equal(&start) {
		t.Fatalf("pod %s ip %q %v start %v, want Running on the shadow's IP since the job's start", p.Status.Phase, p.Status.PodIP, p.Status.PodIPs, p.Status.StartTime)
	}
	if p.Spec.NodeName != "" || len(p.Spec.SchedulingGates) != 1 {
		t.Errorf("pod spec changed: node %q, gates %v", p.Spec.NodeName, p.Spec.SchedulingGates)
	}
	if c := podCondition(p, corev1.PodScheduled); c == nil || c.Reason != "SchedulingGated" {
		t.Errorf("PodScheduled %+v, want the gated pod's", c)
	}
	if c := podCondition(p, corev1.PodInitialized); c == nil || c.Status != corev1.ConditionTrue {
		t.Errorf("Initialized %+v", c)
	}
	for _, ty := range []corev1.PodConditionType{corev1.ContainersReady, corev1.PodReady} {
		if c := podCondition(p, ty); c == nil || c.Status != corev1.ConditionFalse || !strings.Contains(c.Message, "shadow of job 123 is not ready") {
			t.Errorf("%s %+v, want False until the shadow is ready", ty, c)
		}
	}
	if len(p.Status.ContainerStatuses) != 2 || len(p.Status.InitContainerStatuses) != 1 {
		t.Fatalf("statuses %+v %+v", p.Status.ContainerStatuses, p.Status.InitContainerStatuses)
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Running == nil || !cs.State.Running.StartedAt.Equal(&start) || cs.Ready || !ptr.Deref(cs.Started, false) {
			t.Errorf("container %s %+v ready %v, want running since the job's start, not ready", cs.Name, cs.State, cs.Ready)
		}
	}
	if cs := p.Status.InitContainerStatuses[0]; cs.State.Terminated == nil || cs.State.Terminated.ExitCode != 0 {
		t.Errorf("init container %+v, want terminated with 0", cs.State)
	}

	e.now = e.now.Add(time.Minute)
	e.report(1, e.shadow("10.244.1.7", true))
	e.reconcile()
	p = e.pod("train-0")
	for _, ty := range []corev1.PodConditionType{corev1.ContainersReady, corev1.PodReady} {
		if c := podCondition(p, ty); c == nil || c.Status != corev1.ConditionTrue || c.Message != "" || !c.LastTransitionTime.Time.Equal(e.now) {
			t.Errorf("%s %+v, want True since now", ty, c)
		}
	}
	if c := podCondition(p, corev1.PodInitialized); c.LastTransitionTime.Time.Equal(e.now) {
		t.Errorf("Initialized moved without a change: %+v", c)
	}
	for _, cs := range p.Status.ContainerStatuses {
		if !cs.Ready {
			t.Errorf("container %s not ready", cs.Name)
		}
	}
	before := patches
	e.reconcile()
	if patches != before {
		t.Errorf("an unchanged job patched the pod again")
	}

	// Staging out, the job's containers ended: not ready.
	e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
		e.condition(st, v1alpha1.OffloadRequestStagingOut, metav1.ConditionTrue, "Running", "")
	})
	e.reconcile()
	if c := podCondition(e.pod("train-0"), corev1.PodReady); c.Status != corev1.ConditionFalse || !strings.Contains(c.Message, "staging out") {
		t.Errorf("Ready %+v while staging out", c)
	}

	e.report(1, e.finished(true, ""))
	e.reconcile()
	p = e.pod("train-0")
	if p.Status.Phase != corev1.PodSucceeded || p.Status.ContainerStatuses[0].State.Terminated == nil {
		t.Fatalf("pod %s %+v, want Succeeded", p.Status.Phase, p.Status.ContainerStatuses)
	}
	for _, ty := range []corev1.PodConditionType{corev1.ContainersReady, corev1.PodReady} {
		if c := podCondition(p, ty); c == nil || c.Status != corev1.ConditionFalse {
			t.Errorf("%s %+v on the ended pod", ty, c)
		}
	}
}

// TestSinglePodPendingAgainWithoutJob: a pod shown running whose job stops
// running without ending goes back to a gated pod's status.
func TestSinglePodPendingAgainWithoutJob(t *testing.T) {
	cases := []struct {
		name string
		stop func(e *env)
	}{
		{"requeued at the site", func(e *env) {
			e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
				e.condition(st, v1alpha1.OffloadRequestAdmitted, metav1.ConditionFalse, "Queued", "")
			})
		}},
		{"cancelled", func(e *env) { e.report(1, e.evicted("scancel by root")) }},
		{"reservation lost", func(e *env) { e.loseReservation() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, podOwned(1), notebookPod)
			e.submitted()
			start := metav1.NewTime(e.now)
			e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
				e.running("node07", &start)(st)
				e.shadow("10.244.1.7", true)(st)
			})
			e.reconcile()
			if p := e.pod("train-0"); p.Status.Phase != corev1.PodRunning {
				t.Fatalf("pod %s, want Running", p.Status.Phase)
			}
			tc.stop(e)
			e.reconcile()
			p := e.pod("train-0")
			if p.Status.Phase != corev1.PodPending || p.Status.PodIP != "" || p.Status.PodIPs != nil || p.Status.StartTime != nil ||
				p.Status.ContainerStatuses != nil || p.Status.InitContainerStatuses != nil {
				t.Errorf("pod %+v, want a gated pod's status", p.Status)
			}
			if len(p.Status.Conditions) != 1 || p.Status.Conditions[0].Type != corev1.PodScheduled {
				t.Errorf("conditions %+v, want PodScheduled alone", p.Status.Conditions)
			}
			if len(p.Spec.SchedulingGates) != 1 {
				t.Errorf("gates %v", p.Spec.SchedulingGates)
			}
		})
	}
}

// TestRunningShownOnSinglePodsOnly: Jobs and pod groups get nothing while their
// job runs, and no owner labels.
func TestRunningShownOnSinglePodsOnly(t *testing.T) {
	cases := []struct {
		name string
		opts []envOption
	}{
		{"Job", nil},
		{"pod group", []envOption{podOwned(2)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			patches := 0
			e := newEnv(t, append(tc.opts, countPodStatusPatches(&patches))...)
			e.submitted()
			if got := e.request(1).Spec.OwnerLabels; got != nil {
				t.Errorf("ownerLabels %v", got)
			}
			start := metav1.NewTime(e.now)
			e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
				e.running("node07", &start)(st)
				e.shadow("10.244.1.7", true)(st)
			})
			e.reconcile()
			if patches != 0 {
				t.Errorf("%d pod status patches", patches)
			}
			if j := e.job(); j.Status.Active != 0 || len(j.Status.Conditions) != 0 || j.Status.StartTime != nil {
				t.Errorf("Job status %+v", j.Status)
			}
		})
	}
}
