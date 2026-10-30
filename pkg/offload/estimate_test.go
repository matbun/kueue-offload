package offload

import (
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

const limitText = "sbatch: error: QOSMaxSubmitJobPerUserLimit"

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
// while a submit limit holds, sets Retry before the submit and releases the
// quota, never counted as a retry.
func TestEstimateDefers(t *testing.T) {
	for _, tt := range []struct {
		name  string
		in    time.Duration
		retry bool
		msg   string
	}{
		{"within", 300 * time.Second, false, "Slurm expects to start 1 node(s) at 2026-10-29T10:05:00Z"},
		{"beyond", 301 * time.Second, true, "Slurm expects to start 1 node(s) at 2026-10-29T10:05:01Z, in 301 s: quota released until then"},
		{"submit limit", -1, true, "no start time from Slurm (QOSMaxSubmitJobPerUserLimit): " + limitText + "; quota released until then"},
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
			deleted := !e.request(1).DeletionTimestamp.IsZero()
			if !tt.retry {
				if st.State != kueue.CheckStatePending || st.Message != tt.msg || deleted {
					t.Fatalf("check = %s %q, request deleted %v", st.State, st.Message, deleted)
				}
				return
			}
			if st.State != kueue.CheckStateRetry || st.Message != tt.msg || ptr.Deref(st.RequeueAfterSeconds, -1) != 60 || !deleted {
				t.Fatalf("check = %s %q after %v, request deleted %v", st.State, st.Message, st.RequeueAfterSeconds, deleted)
			}
			if ev := fmt.Sprintf("Normal Deferred %s; requeued in 60s", tt.msg); !hasEvent(e.drainEvents(), ev) {
				t.Errorf("no event %q", ev)
			}
		})
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
		if st := e.check("hpc-a"); st.State != kueue.CheckStateRetry {
			t.Fatalf("attempt %d: check %s %q, want Retry", attempt, st.State, st.Message)
		}
		next(attempt)
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
