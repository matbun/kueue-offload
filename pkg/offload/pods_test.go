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
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != "job 123 completed" {
		t.Errorf("check = %s %q, want Ready: the node ends the pod", st.State, st.Message)
	}
}

// countPodWrites counts the writes sent for pods, metadata and status.
func countPodWrites(n *int) envOption {
	return func(_ *[]client.Object, f *interceptor.Funcs) {
		f.Patch = func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				*n++
			}
			return c.Patch(ctx, obj, patch, opts...)
		}
		f.SubResourcePatch = func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				*n++
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		}
	}
}

// The Workload reconciler never writes a pod: the node shows the job on the
// owner's pods, for every kind and outcome.
func TestNoPodWrites(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []envOption
	}{
		{"Job", nil},
		{"pod", []envOption{podOwned(1)}},
		{"pod group", []envOption{podOwned(2)}},
	} {
		for _, succeeded := range []bool{true, false} {
			t.Run(tc.name, func(t *testing.T) {
				writes := 0
				e := newEnv(t, append(tc.opts, countPodWrites(&writes))...)
				e.submitted()
				e.report(1, e.startedAt(e.now))
				e.reconcile()
				e.report(1, e.finished(succeeded, "rank 1 exited 3"))
				e.reconcile()
				if writes != 0 {
					t.Errorf("%d pod writes", writes)
				}
				if st := e.check("hpc-a"); st.State != kueue.CheckStateReady {
					t.Errorf("check %s", st.State)
				}
			})
		}
	}
}

// A refused pod workload fails: annotated, the check Ready; the node fails the
// pods, the controller writes none.
func TestPodOwnersFailOnPermanentError(t *testing.T) {
	writes := 0
	e := newEnv(t, podOwned(2), countPodWrites(&writes))
	const msg = "sbatch: error: Invalid account or account/partition combination specified"
	e.reconcile()
	e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
		e.condition(st, v1alpha1.OffloadRequestAccepted, metav1.ConditionFalse, "Refused", msg)
	})
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != msg || e.failed() != msg {
		t.Errorf("check = %s %q, failed %q", st.State, st.Message, e.failed())
	}
	if e.readyWrites() != 1 || writes != 0 {
		t.Errorf("%d Ready writes, %d pod writes", e.readyWrites(), writes)
	}
}

func TestUnknownOwnerKindRejected(t *testing.T) {
	other := metav1.OwnerReference{APIVersion: "kubeflow.org/v1", Kind: "PyTorchJob", Name: "train", UID: "pt"}
	pod := metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "train-0", UID: "0b6c-pod-0"}
	cases := []struct {
		name   string
		owners []metav1.OwnerReference
		want   string
	}{
		{"controller", []metav1.OwnerReference{{APIVersion: other.APIVersion, Kind: other.Kind, Name: other.Name, UID: other.UID, Controller: ptr.To(true)}}, "owner kubeflow.org/v1 PyTorchJob"},
		{"next to a pod", []metav1.OwnerReference{pod, other}, "owner kubeflow.org/v1 PyTorchJob"},
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
			if st.State != kueue.CheckStateRejected || !strings.Contains(st.Message, "RayJob and RayCluster, and v1 Pod") || !strings.Contains(st.Message, tc.want) {
				t.Errorf("check = %s %q, want Rejected naming %q", st.State, st.Message, tc.want)
			}
			if len(e.requests()) != 0 || e.failed() != "" {
				t.Errorf("a request or %s for a workload with %s", v1alpha1.FailedAnnotation, tc.want)
			}
		})
	}
}

// startedPods runs the first attempt of n owner pods until Kueue admitted
// them: the job started, the check Ready.
func startedPods(t *testing.T, n int, opts ...envOption) *env {
	t.Helper()
	e := newEnv(t, append([]envOption{podOwned(n)}, opts...)...)
	e.submitted()
	e.report(1, e.startedAt(e.now))
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady {
		t.Fatalf("check %s once started, want Ready", st.State)
	}
	return e
}

// A pod workload Kueue admitted retries in place: evicting it would delete
// its pods for good. The request goes, the check stays Ready with the quota,
// and the next attempt comes after Kueue's backoff for the attempt, until the
// retries are spent and the Workload fails.
func TestRetryInPlace(t *testing.T) {
	for _, n := range []int{1, 2} {
		t.Run(map[int]string{1: "pod", 2: "pod group"}[n], func(t *testing.T) {
			e := startedPods(t, n) // backoffLimitCount 2, backoffBaseSeconds 60
			for attempt := int32(1); attempt <= 2; attempt++ {
				delay := time.Duration(60<<(attempt-1)) * time.Second
				at := e.now.Add(delay).UTC().Format(time.RFC3339)
				e.report(attempt, e.evicted("scancel by root"))
				e.drainEvents()
				if res := e.reconcile(); res.RequeueAfter != delay {
					t.Errorf("attempt %d: requeued after %v, want %v", attempt, res.RequeueAfter, delay)
				}
				want := "job 123 cancelled: scancel by root; next attempt at " + at
				if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != want || st.RetryCount != nil {
					t.Fatalf("attempt %d: check %s %q retryCount %v, want Ready %q", attempt, st.State, st.Message, st.RetryCount, want)
				}
				if got := e.workload().Annotations[NextAttemptAnnotation]; got != at {
					t.Errorf("attempt %d: %s %q, want %s", attempt, NextAttemptAnnotation, got, at)
				}
				if !hasEvent(e.drainEvents(), "Warning Retry job 123 cancelled: scancel by root; retry ") {
					t.Errorf("attempt %d: no Retry event", attempt)
				}
				if q := e.request(attempt); q.DeletionTimestamp.IsZero() {
					t.Fatalf("attempt %d: request not deleted", attempt)
				}
				e.executorLetsGo(attempt)
				e.now = e.now.Add(delay - time.Second)
				if res := e.reconcile(); res.RequeueAfter != time.Second {
					t.Errorf("attempt %d: requeued after %v before the backoff ended, want 1s", attempt, res.RequeueAfter)
				}
				if _, ok := e.requests()[requestOf(attempt+1)]; ok {
					t.Fatalf("attempt %d: next attempt before the backoff ended", attempt)
				}
				e.now = e.now.Add(time.Second)
				e.reconcile()
				next := e.request(attempt + 1)
				if next.Spec.Deferrals != 0 || !next.DeletionTimestamp.IsZero() {
					t.Fatalf("attempt %d: next request %+v", attempt, next.Spec)
				}
				if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != "waiting for the executor in namespace alice" {
					t.Errorf("attempt %d: check %s %q after the next attempt", attempt, st.State, st.Message)
				}
				if _, ok := e.workload().Annotations[NextAttemptAnnotation]; ok {
					t.Errorf("attempt %d: %s kept", attempt, NextAttemptAnnotation)
				}
				e.takes(attempt + 1)
				e.report(attempt+1, e.startedAt(e.now))
				e.reconcile()
			}
			e.report(3, e.evicted("scancel by root"))
			e.reconcile()
			const want = "job 123 cancelled: scancel by root; giving up after 2 retries"
			if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != want || e.failed() != want {
				t.Fatalf("check %s %q, failed %q, want Ready and failed", st.State, st.Message, e.failed())
			}
			e.now = e.now.Add(time.Hour)
			e.reconcile()
			if _, ok := e.requests()[requestOf(4)]; ok {
				t.Errorf("an attempt after the retries were spent")
			}
			for _, w := range e.checks.writes {
				if w.State != kueue.CheckStatePending && w.State != kueue.CheckStateReady {
					t.Errorf("check written %s %q: the Workload must stay admitted", w.State, w.Message)
				}
			}
			e.readyWrites()
		})
	}
}

// staleWorkloads reads Workloads as a cache that has not seen
// NextAttemptAnnotation yet.
type staleWorkloads struct{ client.Client }

func (s staleWorkloads) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := s.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if wl, ok := obj.(*kueue.Workload); ok {
		delete(wl.Annotations, NextAttemptAnnotation)
	}
	return nil
}

// The cache can show the request gone before the annotation written ahead of
// it: the backoff is read from the API server.
func TestRetryInPlaceReadsTheBackoffFromTheServer(t *testing.T) {
	e := startedPods(t, 1)
	e.report(1, e.evicted("scancel by root"))
	e.reconcile()
	e.r.Client = staleWorkloads{e.c}
	e.now = e.now.Add(30 * time.Second)
	if res := e.reconcile(); res.RequeueAfter != 30*time.Second {
		t.Errorf("requeued after %v, want the 30 s left", res.RequeueAfter)
	}
	if _, ok := e.requests()[requestOf(2)]; ok {
		t.Errorf("next attempt during the backoff")
	}
}

// An attempt made in place that the start estimate defers waits deferInterval
// in place, never deactivating the Workload, and counts as a deferral.
func TestDeferInPlace(t *testing.T) {
	e := startedPods(t, 1, withMaxStartDelay(5*time.Minute))
	e.report(1, e.evicted("node failure"))
	e.reconcile()
	e.executorLetsGo(1)
	e.now = e.now.Add(time.Minute)
	e.reconcile()
	e.takes(2)
	e.report(2, e.estimated(time.Hour))
	e.drainEvents()
	if res := e.reconcile(); res.RequeueAfter != 30*time.Second {
		t.Errorf("requeued after %v, want deferInterval", res.RequeueAfter)
	}
	at := e.now.Add(30 * time.Second).UTC().Format(time.RFC3339)
	want := "Slurm expects to start 1 node(s) at 2026-10-29T11:01:00Z, in 3600 s: next attempt at " + at + ", quota kept"
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != want {
		t.Errorf("check %s %q, want Ready %q", st.State, st.Message, want)
	}
	wl := e.workload()
	if !ptr.Deref(wl.Spec.Active, true) || wl.Annotations[NextAttemptAnnotation] != at || wl.Annotations[DeferredUntilAnnotation] != "" {
		t.Errorf("Workload active %v, annotations %v", wl.Spec.Active, wl.Annotations)
	}
	if !hasEvent(e.drainEvents(), "Normal Deferred "+want) {
		t.Errorf("no Deferred event")
	}
	if q := e.request(2); q.DeletionTimestamp.IsZero() {
		t.Errorf("deferred request not deleted")
	}
	e.executorLetsGo(2)
	e.now = e.now.Add(30 * time.Second)
	e.reconcile()
	if q := e.request(3); q.Spec.Deferrals != 1 {
		t.Errorf("attempt 3: deferrals %d, want 1", q.Spec.Deferrals)
	}
	e.readyWrites()
}
