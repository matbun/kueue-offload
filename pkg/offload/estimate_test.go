package offload

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

const limitText = "sbatch: error: QOSMaxSubmitJobPerUserLimit"

// withDeferInterval sets the OffloadConfig's deferInterval.
func withDeferInterval(d time.Duration) envOption {
	return withConfig(func(sc *v1alpha1.OffloadConfig) { sc.Spec.DeferInterval = &metav1.Duration{Duration: d} })
}

// withMaxStartDelay sets the OffloadConfig's maxStartDelay.
func withMaxStartDelay(d time.Duration) envOption {
	return withConfig(func(sc *v1alpha1.OffloadConfig) { sc.Spec.MaxStartDelay = &metav1.Duration{Duration: d} })
}

// estimated is the executor recording, before the submit, that the site
// expects to start one node in `in`, or gives no start time when in is
// negative.
func (e *env) estimated(in time.Duration) func(st *v1alpha1.OffloadRequestStatus) {
	return func(st *v1alpha1.OffloadRequestStatus) {
		st.Estimate = &v1alpha1.StartEstimate{ProbeTime: metav1.NewTime(e.now), Nodes: 1}
		if in < 0 {
			st.Estimate.Reason, st.Estimate.Message = "QOSMaxSubmitJobPerUserLimit", limitText
			return
		}
		st.Estimate.StartTime = ptr.To(metav1.NewTime(e.now.Add(in)))
	}
}

// Without maxStartDelay the estimate only shows in the check message, until the
// job is submitted.
func TestEstimateInCheckMessage(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	e.takes(1)
	e.report(1, e.estimated(3*time.Hour))
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending || st.Message != "Slurm expects to start 1 node(s) at 2026-10-29T13:00:00Z" {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
	e.report(1, e.estimated(-1))
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending ||
		st.Message != "no start time from Slurm (QOSMaxSubmitJobPerUserLimit): "+limitText {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
	e.report(1, e.queued("123", "Priority", ""))
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending || st.Message != "job 123 queued (Priority)" {
		t.Errorf("check = %s %q after the submit", st.State, st.Message)
	}
	if _, ok := e.requests()[requestOf(1)]; !ok {
		t.Error("request deleted")
	}
}

// A start within maxStartDelay keeps the attempt; one further away, or none
// while a submit limit holds, deactivates the Workload for deferInterval before
// the submit, which releases the quota at once, never counted as a retry. The
// check stays Pending with the reason.
func TestEstimateDefers(t *testing.T) {
	for _, tt := range []struct {
		name     string
		in       time.Duration
		deferred bool
		msg      string
	}{
		{"within", 300 * time.Second, false, "Slurm expects to start 1 node(s) at 2026-10-29T10:05:00Z"},
		{"beyond", 301 * time.Second, true, "Slurm expects to start 1 node(s) at 2026-10-29T10:05:01Z, in 301 s: Workload deactivated until 2026-10-29T10:00:30Z, quota released"},
		{"submit limit", -1, true, "no start time from Slurm (QOSMaxSubmitJobPerUserLimit): " + limitText + "; Workload deactivated until 2026-10-29T10:00:30Z, quota released"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// backoffLimitCount 0 rejects at the first lost attempt, but not here.
			e := newEnv(t, withMaxStartDelay(5*time.Minute), withRetryStrategy(&kueue.ProvisioningRequestRetryStrategy{
				BackoffLimitCount: ptr.To[int32](0), BackoffBaseSeconds: ptr.To[int32](60), BackoffMaxSeconds: ptr.To[int32](1800),
			}))
			e.reconcile()
			e.takes(1)
			e.report(1, e.estimated(tt.in))
			e.reconcile()
			st := e.check("hpc-a")
			wl := e.workload()
			deleted := !e.request(1).DeletionTimestamp.IsZero()
			if !tt.deferred {
				if st.State != kueue.CheckStatePending || st.Message != tt.msg || deleted || !ptr.Deref(wl.Spec.Active, true) {
					t.Fatalf("check = %s %q, request deleted %v, active %v", st.State, st.Message, deleted, wl.Spec.Active)
				}
				return
			}
			if st.State != kueue.CheckStatePending || st.Message != tt.msg || st.RequeueAfterSeconds != nil || !deleted {
				t.Fatalf("check = %s %q after %v, request deleted %v", st.State, st.Message, st.RequeueAfterSeconds, deleted)
			}
			if ptr.Deref(wl.Spec.Active, true) || wl.Annotations[DeferredUntilAnnotation] != "2026-10-29T10:00:30Z" {
				t.Fatalf("Workload active %v, %s %q", wl.Spec.Active, DeferredUntilAnnotation, wl.Annotations[DeferredUntilAnnotation])
			}
			if ev := fmt.Sprintf("Normal Deferred %s; requeued in 30s", tt.msg); !hasEvent(e.drainEvents(), ev) {
				t.Errorf("no event %q", ev)
			}
			if res := e.reconcile(); res.RequeueAfter != 30*time.Second {
				t.Errorf("requeued after %v, want deferInterval's 30s", res.RequeueAfter)
			}
		})
	}
}

// Kueue evicts the deactivated Workload; the controller reactivates it when the
// backoff is over and Kueue queues it again. The next reservation is the next
// attempt, with the deferred one in its deferrals and Kueue's retryCount
// untouched; neither the check nor the owner, of any kind, fails meanwhile.
func TestDeferralReactivates(t *testing.T) {
	rayJob := rayOwned(rayJobKind, 1, func(u *unstructured.Unstructured) {
		u.Object["spec"] = map[string]any{"suspend": true, "entrypoint": "python x.py"}
		u.Object["status"] = map[string]any{"jobDeploymentStatus": "Suspended"}
	})
	for _, tc := range []struct {
		name string
		opts []envOption
	}{
		{"Job", nil},
		{"pod", []envOption{podOwned(1)}},
		{"pod group", []envOption{podOwned(2)}},
		{"JobSet", []envOption{customOwned(jobSetKind)}},
		{"TrainJob", []envOption{customOwned(trainJobKind)}},
		{"RayJob", []envOption{rayJob, rayStatus}},
		{"RayCluster", []envOption{rayOwned(rayClusterKind, 1, nil)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, append(tc.opts, withMaxStartDelay(5*time.Minute), withRetryStrategy(&kueue.ProvisioningRequestRetryStrategy{
				BackoffLimitCount: ptr.To[int32](0), BackoffBaseSeconds: ptr.To[int32](60), BackoffMaxSeconds: ptr.To[int32](1800),
			}))...)
			e.reconcile()
			owners := e.ownerStatuses()
			e.takes(1)
			e.report(1, e.estimated(-1))
			e.reconcile()
			e.loseReservation()
			e.executorLetsGo(1)
			if res := e.reconcile(); res.RequeueAfter != 30*time.Second {
				t.Errorf("requeued after %v, want 30s", res.RequeueAfter)
			}
			if q := e.request(1); !reflect.DeepEqual(q.Finalizers, []string{v1alpha1.AttemptFinalizer}) {
				t.Errorf("finalizers %v during the deferral, want the attempt's", q.Finalizers)
			}
			e.now = e.now.Add(29 * time.Second)
			if res := e.reconcile(); res.RequeueAfter != time.Second || ptr.Deref(e.workload().Spec.Active, true) {
				t.Fatalf("reactivated %v before the deferral ended, requeued after %v", e.workload().Spec.Active, res.RequeueAfter)
			}
			e.now = e.now.Add(time.Second)
			e.drainEvents()
			e.reconcile()
			wl := e.workload()
			if !ptr.Deref(wl.Spec.Active, true) || wl.Annotations[DeferredUntilAnnotation] != "" {
				t.Fatalf("Workload active %v, annotations %v after the deferral", wl.Spec.Active, wl.Annotations)
			}
			if ev := "Normal Reactivated deferral over"; !hasEvent(e.drainEvents(), ev) {
				t.Errorf("no event %q", ev)
			}
			e.reserveAgain()
			e.reconcile()
			if q := e.request(2); q.Spec.Deferrals != 1 {
				t.Errorf("attempt 2: deferrals %d, want 1", q.Spec.Deferrals)
			}
			st := e.check("hpc-a")
			if st.State != kueue.CheckStatePending || st.RetryCount != nil {
				t.Errorf("check %s, retryCount %v, want Pending and none", st.State, st.RetryCount)
			}
			for _, w := range e.checks.writes {
				if w.State != kueue.CheckStatePending {
					t.Errorf("check written %s %q", w.State, w.Message)
				}
			}
			if got := e.ownerStatuses(); !reflect.DeepEqual(got, owners) {
				t.Errorf("owner status %v, want it untouched: %v", got, owners)
			}
		})
	}
}

// A deferral waits for the site, it is not a failure: every one deactivates the
// Workload for the same deferInterval, never the retryStrategy's doubling
// backoff, so a job queued behind a long one starts soon after a slot frees.
func TestDeferIntervalDoesNotBackOff(t *testing.T) {
	e := newEnv(t, withMaxStartDelay(5*time.Minute), withDeferInterval(45*time.Second), withRetryStrategy(&kueue.ProvisioningRequestRetryStrategy{
		BackoffLimitCount: ptr.To[int32](3), BackoffBaseSeconds: ptr.To[int32](60), BackoffMaxSeconds: ptr.To[int32](1800),
	}))
	e.reconcile()
	for attempt := int32(1); attempt <= 3; attempt++ {
		e.takes(attempt)
		e.report(attempt, e.estimated(-1))
		e.reconcile()
		want := e.now.Add(45 * time.Second).UTC().Format(time.RFC3339)
		if wl := e.workload(); ptr.Deref(wl.Spec.Active, true) || wl.Annotations[DeferredUntilAnnotation] != want {
			t.Fatalf("attempt %d: active %v, deferred until %q, want %s", attempt, wl.Spec.Active, wl.Annotations[DeferredUntilAnnotation], want)
		}
		e.loseReservation()
		e.executorLetsGo(attempt)
		if res := e.reconcile(); res.RequeueAfter != 45*time.Second {
			t.Fatalf("attempt %d: requeued after %v, want 45s", attempt, res.RequeueAfter)
		}
		e.now = e.now.Add(45 * time.Second)
		e.reconcile()
		e.reserveAgain()
		e.reconcile()
	}
}

// ownerStatuses reads the status of each of the Workload's owners.
func (e *env) ownerStatuses() map[string]any {
	e.t.Helper()
	out := map[string]any{}
	for _, ref := range e.workload().OwnerReferences {
		u := &unstructured.Unstructured{}
		u.SetAPIVersion(ref.APIVersion)
		u.SetKind(ref.Kind)
		if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: ref.Name}, u); err != nil {
			e.t.Fatal(err)
		}
		out[ref.Kind+"/"+ref.Name] = u.Object["status"]
	}
	return out
}

// A Workload a user reactivates during the backoff only loses the annotation;
// one deactivated for another reason stays inactive.
func TestDeferralReactivatesOnlyItsOwn(t *testing.T) {
	e := newEnv(t, withMaxStartDelay(5*time.Minute))
	e.reconcile()
	e.takes(1)
	e.report(1, e.estimated(time.Hour))
	e.reconcile()
	wl := e.workload()
	if ptr.Deref(wl.Spec.Active, true) || wl.Annotations[DeferredUntilAnnotation] == "" {
		t.Fatalf("Workload active %v, annotations %v, want deferred", wl.Spec.Active, wl.Annotations)
	}
	wl.Spec.Active = ptr.To(true)
	if err := e.c.Update(context.Background(), wl); err != nil {
		t.Fatal(err)
	}
	e.drainEvents()
	e.reconcile()
	if wl := e.workload(); wl.Annotations[DeferredUntilAnnotation] != "" {
		t.Errorf("annotation %q kept on a Workload active again", wl.Annotations[DeferredUntilAnnotation])
	}
	if evs := e.drainEvents(); hasEvent(evs, "Normal Reactivated") {
		t.Errorf("events %v", evs)
	}
	wl = e.workload()
	wl.Spec.Active = ptr.To(false)
	if err := e.c.Update(context.Background(), wl); err != nil {
		t.Fatal(err)
	}
	e.now = e.now.Add(24 * time.Hour)
	e.reconcile()
	if ptr.Deref(e.workload().Spec.Active, true) {
		t.Error("a Workload deactivated by a user was reactivated")
	}
}

// A job submitted before the offload controller read a far estimate goes on:
// only the site's queue holds it.
func TestEstimateAfterSubmitIsKept(t *testing.T) {
	e := newEnv(t, withMaxStartDelay(5*time.Minute))
	e.reconcile()
	e.takes(1)
	e.report(1, e.estimated(time.Hour))
	e.report(1, e.queued("123", "Priority", ""))
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending || st.Message != "job 123 queued (Priority)" {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
}

// So does an attempt whose stage-in started before the offload controller read
// a far estimate: deferring it would throw the copy away.
func TestEstimateAfterStageInIsKept(t *testing.T) {
	e := newEnv(t, withMaxStartDelay(5*time.Minute))
	e.reconcile()
	e.takes(1)
	e.report(1, e.estimated(time.Hour))
	e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
		st.Message = "Job stage-in-0b6c-1 is running"
		e.condition(st, v1alpha1.OffloadRequestStagingIn, metav1.ConditionTrue, "Running", "")
	})
	e.reconcile()
	st := e.check("hpc-a")
	if st.State != kueue.CheckStatePending || st.Message != "staging in: Job stage-in-0b6c-1 is running" || !e.request(1).DeletionTimestamp.IsZero() {
		t.Errorf("check = %s %q, request deleted %v", st.State, st.Message, !e.request(1).DeletionTimestamp.IsZero())
	}
}

// Each deferred attempt adds one to the next attempt's deferrals, which the
// retries of lost jobs do not count; an attempt whose job was submitted adds
// none.
func TestDeferralsCountedAcrossAttempts(t *testing.T) {
	e := newEnv(t, withMaxStartDelay(5*time.Minute), withRetryStrategy(&kueue.ProvisioningRequestRetryStrategy{
		BackoffLimitCount: ptr.To[int32](1), BackoffBaseSeconds: ptr.To[int32](60), BackoffMaxSeconds: ptr.To[int32](1800),
	}))
	e.reconcile()
	next := func(i int32) {
		t.Helper()
		e.executorLetsGo(i)
		e.loseReservation()
		e.reconcile()
		e.reserveAgain()
		e.evictAndReserve("hpc-a")
		e.reconcile()
	}
	for i, in := range []time.Duration{time.Hour, -1} {
		attempt := int32(i + 1)
		e.takes(attempt)
		e.report(attempt, e.estimated(in))
		e.reconcile()
		if ptr.Deref(e.workload().Spec.Active, true) {
			t.Fatalf("attempt %d: Workload active, want deactivated", attempt)
		}
		// Kueue evicts the Workload, the backoff passes, the controller
		// reactivates it and Kueue reserves quota again.
		e.executorLetsGo(attempt)
		e.loseReservation()
		e.reconcile()
		e.now = e.now.Add(time.Hour)
		e.reconcile()
		e.reserveAgain()
		e.reconcile()
		if q := e.request(attempt + 1); q.Spec.Deferrals != attempt {
			t.Fatalf("attempt %d: deferrals %d, want %d", attempt+1, q.Spec.Deferrals, attempt)
		}
	}
	// Attempt 3 is lost after its submit: retry 1 of 1, and its job counts.
	e.takes(3)
	e.report(3, e.estimated(time.Minute))
	e.report(3, e.queued("125", "", ""))
	e.report(3, e.evicted("scancel by an operator"))
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateRetry || !strings.Contains(st.Message, "job 125 cancelled") {
		t.Fatalf("attempt 3: check %s %q, want Retry", st.State, st.Message)
	}
	next(3)
	if q := e.request(4); q.Spec.Deferrals != 2 {
		t.Fatalf("attempt 4: deferrals %d, want 2", q.Spec.Deferrals)
	}
	e.takes(4)
	e.report(4, e.queued("126", "", ""))
	e.report(4, e.evicted("scancel by an operator"))
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateRejected || !strings.Contains(st.Message, "giving up after 1 retries") {
		t.Fatalf("attempt 4: check %s %q, want Rejected after 1 retry", st.State, st.Message)
	}
}
