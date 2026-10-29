package dispatcher

import (
	"context"
	"net/http"
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

	"github.com/interlink-hq/interlink-dispatch/pkg/agentapi"
)

var groupPods = []string{"train-0", "train-1"}

func TestPodGroupSubmitsOnce(t *testing.T) {
	e := newEnv(t, podOwned(2))
	e.reconcile()
	e.reconcile()
	e.reconcile()
	if s, _, _ := e.agent.counts(); s != 1 {
		t.Fatalf("submits = %d, want 1", s)
	}
	req := e.agent.submits[0]
	if req.Owner != (agentapi.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: podGroup, UID: "0b6c-pod-0"}) {
		t.Errorf("owner = %+v, want the group name and the first pod's UID", req.Owner)
	}
	if len(req.PodSets) != 1 || req.PodSets[0].Name != podSetKey || req.PodSets[0].Count != 2 {
		t.Fatalf("podSets = %+v, want one PodSet of count 2", req.PodSets)
	}
	ann := req.PodSets[0].Template.Annotations
	if ann["slurm-job.vk.io/flags"] != "--time=01:00:00" {
		t.Errorf("pod annotations not merged: %v", ann)
	}
	if ann["interlink.eu/mode"] != "template" {
		t.Errorf("template annotation must win over the pod's, got %q", ann["interlink.eu/mode"])
	}
	if _, ok := ann["example.org/unrelated"]; ok {
		t.Errorf("annotation outside the forwarded prefixes was merged")
	}
	for _, name := range groupPods {
		if p := e.pod(name); p.Status.Phase != corev1.PodPending || len(p.Spec.SchedulingGates) != 1 {
			t.Errorf("pod %s changed while queued: %s, gates %v", name, p.Status.Phase, p.Spec.SchedulingGates)
		}
	}
}

func TestPodOwnersTerminalPhase(t *testing.T) {
	cases := []struct {
		name    string
		job     agentapi.JobStatus
		phase   corev1.PodPhase
		message string
	}{
		{"completed", agentapi.JobStatus{State: agentapi.StateCompleted}, corev1.PodSucceeded, "Slurm job 123 completed"},
		{"failed", agentapi.JobStatus{State: agentapi.StateFailed, Message: "rank 1 exited 3"}, corev1.PodFailed, "Slurm job 123 failed: rank 1 exited 3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, podOwned(2))
			e.reconcile()
			tc.job.APIVersion, tc.job.JobID = agentapi.APIVersion, "123"
			e.agent.set(func(a *fakeAgent) { a.job = tc.job })
			e.reconcile()
			for _, name := range groupPods {
				p := e.pod(name)
				if p.Status.Phase != tc.phase || p.Status.Reason != ReasonDispatch || p.Status.Message != tc.message {
					t.Errorf("pod %s = %s %s %q, want %s %s %q", name, p.Status.Phase, p.Status.Reason, p.Status.Message,
						tc.phase, ReasonDispatch, tc.message)
				}
				if p.Spec.NodeName != "" || len(p.Spec.SchedulingGates) != 1 {
					t.Errorf("pod %s spec changed: node %q, gates %v", name, p.Spec.NodeName, p.Spec.SchedulingGates)
				}
			}
			if st := e.check("hpc-a"); st.State != kueue.CheckStatePending {
				t.Errorf("check = %s, want Pending (D1)", st.State)
			}
			if rec := e.record(); rec.Status.State != tc.job.State {
				t.Errorf("record state = %q", rec.Status.State)
			}
			if len(e.job().Status.Conditions) != 0 {
				t.Errorf("an unrelated Job was patched")
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
	e.reconcile()
	e.agent.set(func(a *fakeAgent) { a.job.State, a.job.Message = agentapi.StateFailed, "rank 1 exited 3" })
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
	e.reconcile()
	if err := e.c.Delete(context.Background(), e.pod("train-1")); err != nil {
		t.Fatal(err)
	}
	e.agent.set(func(a *fakeAgent) { a.job.State = agentapi.StateCompleted })
	e.reconcile() // fails the test on a reconcile error
	if p := e.pod("train-0"); p.Status.Phase != corev1.PodSucceeded {
		t.Errorf("train-0 = %s, want Succeeded", p.Status.Phase)
	}
	if rec := e.record(); rec.Status.State != agentapi.StateCompleted {
		t.Errorf("record state = %q", rec.Status.State)
	}
}

func TestSinglePodOwner(t *testing.T) {
	e := newEnv(t, podOwned(1))
	e.reconcile()
	if s, _, _ := e.agent.counts(); s != 1 {
		t.Fatalf("submits = %d, want 1", s)
	}
	req := e.agent.submits[0]
	if req.Owner != (agentapi.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "train-0", UID: "0b6c-pod-0"}) {
		t.Errorf("owner = %+v, want the pod", req.Owner)
	}
	if req.PodSets[0].Count != 1 {
		t.Errorf("count = %d, want 1", req.PodSets[0].Count)
	}
	e.agent.set(func(a *fakeAgent) { a.job.State = agentapi.StateCompleted })
	e.reconcile()
	if p := e.pod("train-0"); p.Status.Phase != corev1.PodSucceeded || p.Status.Reason != ReasonDispatch {
		t.Errorf("pod = %s %s, want Succeeded %s", p.Status.Phase, p.Status.Reason, ReasonDispatch)
	}
}

func TestPodOwnersRejectedOnPermanentError(t *testing.T) {
	e := newEnv(t, podOwned(2))
	const msg = "sbatch: error: Invalid account or account/partition combination specified"
	e.agent.set(func(a *fakeAgent) {
		a.submitCode = http.StatusBadRequest
		a.submitBody = agentapi.ErrorResponse{APIVersion: agentapi.APIVersion, Permanent: true, Message: msg}
	})
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateRejected || st.Message != msg {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
	for _, name := range groupPods {
		if p := e.pod(name); p.Status.Phase != corev1.PodFailed || p.Status.Reason != ReasonRejected || p.Status.Message != msg {
			t.Errorf("pod %s = %s %s %q, want Failed %s", name, p.Status.Phase, p.Status.Reason, p.Status.Message, ReasonRejected)
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
			if s, _, _ := e.agent.counts(); s != 0 {
				t.Errorf("submitted a workload with %s", tc.want)
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
		name  string
		state string
		ranks []agentapi.RankStatus
		want  map[string]int32
	}{
		{"completed without ranks", agentapi.StateCompleted, nil, map[string]int32{"train": 0, "wait": 0}},
		{"failed without ranks", agentapi.StateFailed, nil, map[string]int32{"train": 1, "wait": 1}},
		{"failed with exit codes", agentapi.StateFailed,
			[]agentapi.RankStatus{{Rank: 0, Containers: []corev1.ContainerStatus{terminated("train", 3)}}, {Rank: 1, Containers: []corev1.ContainerStatus{terminated("wait", 9)}}},
			map[string]int32{"train": 3, "wait": 1}},
		{"completed with exit codes", agentapi.StateCompleted,
			[]agentapi.RankStatus{{Rank: 0, Containers: []corev1.ContainerStatus{terminated("train", 0), terminated("wait", 0)}}},
			map[string]int32{"train": 0, "wait": 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, podOwned(2), argoShaped)
			e.reconcile()
			e.agent.set(func(a *fakeAgent) {
				a.job.State, a.job.Ranks, a.job.StartTime, a.job.CompletionTime = tc.state, tc.ranks, &start, &end
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
