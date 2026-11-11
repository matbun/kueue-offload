package offload

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/executorapi"
)

func TestRequestPerAttemptWithResolvedPodSets(t *testing.T) {
	e := newEnv(t)
	if res := e.reconcile(); res.RequeueAfter != 15*time.Second {
		t.Errorf("requeue after the request = %v, want the poll interval", res.RequeueAfter)
	}
	e.reconcile()
	e.reconcile()

	reqs := e.requests()
	if len(reqs) != 1 {
		t.Fatalf("requests %v, want one", reqs)
	}
	q := e.request(1)
	if q.Name != wlName+"-hpc-a-1" {
		t.Errorf("name %q, want <workload>-<check>-<attempt>", q.Name)
	}
	sp := q.Spec
	if sp.Workload != (v1alpha1.WorkloadReference{Name: wlName, UID: wlUID}) || sp.AdmissionCheck != "hpc-a" || sp.Attempt != 1 ||
		sp.Site != testSite || sp.Account != "proj42" {
		t.Errorf("spec header fields wrong: %+v", sp)
	}
	if sp.Owner != (v1alpha1.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: "train", UID: jobUID}) {
		t.Errorf("owner = %+v", sp.Owner)
	}
	if want := map[string]string{"slurm-job.vk.io/flags": "--time=04:00:00 --gres=gpu:1", "interlink.eu/mode": "owner"}; !reflect.DeepEqual(sp.OwnerAnnotations, want) {
		t.Errorf("ownerAnnotations = %v, want the forwarded prefixes only %v", sp.OwnerAnnotations, want)
	}
	if len(sp.PodSets) != 1 {
		t.Fatalf("podSets = %d, want 1", len(sp.PodSets))
	}
	ps := sp.PodSets[0]
	if ps.Name != "main" || ps.Count != 4 || ps.Annotations[flagsKey] != "--account=proj42 --partition=gpu" || len(ps.Annotations) != 1 {
		t.Errorf("podSet = %s count %d annotations %v, want main 4 with the gpu flavor's flags", ps.Name, ps.Count, ps.Annotations)
	}
	if ps.Flavors["nvidia.com/gpu"] != "hpc-a-gpu" || ps.Flavors["cpu"] != "hpc-a-gpu" {
		t.Errorf("flavors = %v", ps.Flavors)
	}
	if want := []string{"hf-token", "s3-creds"}; !reflect.DeepEqual(ps.SecretRefs, want) {
		t.Errorf("secretRefs = %v, want %v", ps.SecretRefs, want)
	}
	if want := []string{"train-config", "train-env"}; !reflect.DeepEqual(ps.ConfigMapRefs, want) {
		t.Errorf("configMapRefs = %v, want %v", ps.ConfigMapRefs, want)
	}
	if !equality.Semantic.DeepEqual(ps.Template, podTemplate()) {
		t.Errorf("template differs from the Workload's: the executor merges ownerAnnotations itself")
	}
	ref := metav1.GetControllerOf(q)
	if ref == nil || ref.Kind != "Workload" || ref.Name != wlName || ref.UID != wlUID || ref.BlockOwnerDeletion != nil && *ref.BlockOwnerDeletion {
		t.Errorf("controller reference %+v, want the Workload without blockOwnerDeletion", ref)
	}
	if !reflect.DeepEqual(q.Finalizers, []string{v1alpha1.AttemptFinalizer}) || q.Labels[v1alpha1.SiteLabel] != testSite {
		t.Errorf("finalizers %v labels %v", q.Finalizers, q.Labels)
	}
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending || st.Message != "waiting for the executor in namespace alice" {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
	if st := e.check("other-check"); st.Message != "other" {
		t.Errorf("another controller's check was touched: %+v", st)
	}
	for _, w := range e.checks.writes {
		if w.Name != "hpc-a" {
			t.Errorf("wrote check %s", w.Name)
		}
	}
	if !hasEvent(e.drainEvents(), "Normal Submitted attempt 1 as OffloadRequest "+q.Name+" to site hpc-a, account proj42") {
		t.Errorf("no Submitted event")
	}
}

// Every row of the state table of kueue-native.md 3.2 for a live request: the
// check stays Pending until the job has started, Ready from then on, also when
// it ended before the offload controller saw it run, and the owner is never
// written.
func TestRequestStatusMapping(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 10, 29, 10, 14, 2, 0, time.UTC))
	end := metav1.NewTime(time.Date(2026, 10, 29, 11, 0, 0, 0, time.UTC))
	cases := []struct {
		name    string
		report  func(e *env) func(st *v1alpha1.OffloadRequestStatus)
		state   kueue.CheckState
		message string
		event   string
		requeue bool
	}{
		{
			name:    "accepted",
			report:  func(e *env) func(*v1alpha1.OffloadRequestStatus) { return func(*v1alpha1.OffloadRequestStatus) {} },
			state:   kueue.CheckStatePending,
			message: "accepted by the executor in namespace alice",
			requeue: true,
		},
		{
			name: "staging in",
			report: func(e *env) func(*v1alpha1.OffloadRequestStatus) {
				return func(st *v1alpha1.OffloadRequestStatus) {
					st.Message = "stage-in Job stage-in-0b6c-wl: 1 active"
					e.condition(st, v1alpha1.OffloadRequestStagingIn, metav1.ConditionTrue, "StagingIn", "")
				}
			},
			state:   kueue.CheckStatePending,
			message: "staging in: stage-in Job stage-in-0b6c-wl: 1 active",
			requeue: true,
		},
		{
			name: "submit refused for now",
			report: func(e *env) func(*v1alpha1.OffloadRequestStatus) {
				return func(st *v1alpha1.OffloadRequestStatus) {
					st.Reason, st.Message = "SubmitLimit", "QOSMaxSubmitJobPerUserLimit: the user's other jobs fill the QOS"
				}
			},
			state:   kueue.CheckStatePending,
			message: "waiting to submit (SubmitLimit): QOSMaxSubmitJobPerUserLimit: the user's other jobs fill the QOS",
			requeue: true,
		},
		{
			name:    "queued",
			report:  func(e *env) func(*v1alpha1.OffloadRequestStatus) { return e.queued("123", "Resources", "") },
			state:   kueue.CheckStatePending,
			message: "job 123 queued (Resources)",
			requeue: true,
		},
		{
			name: "independent pods: the executor names every job",
			report: func(e *env) func(*v1alpha1.OffloadRequestStatus) {
				return e.queued("123,124", "WaitingForSite", "rank 2 waits for the site: QOSMaxSubmitJobPerUserLimit")
			},
			state:   kueue.CheckStatePending,
			message: "job 123,124 queued (WaitingForSite): rank 2 waits for the site: QOSMaxSubmitJobPerUserLimit",
			requeue: true,
		},
		{
			name: "running",
			report: func(e *env) func(*v1alpha1.OffloadRequestStatus) {
				return func(st *v1alpha1.OffloadRequestStatus) { e.queued("123", "", "")(st); e.running("node07", &start)(st) }
			},
			state:   kueue.CheckStateReady,
			message: "job 123 running on node07 since 2026-10-29T10:14:02Z",
			event:   "Normal Running job 123 running on node07",
			requeue: true,
		},
		{
			name: "start time without Admitted",
			report: func(e *env) func(*v1alpha1.OffloadRequestStatus) {
				return func(st *v1alpha1.OffloadRequestStatus) { e.queued("123", "", "")(st); st.StartTime = &start }
			},
			state:   kueue.CheckStateReady,
			message: "job 123 queued",
			requeue: true,
		},
		{
			name: "staging out",
			report: func(e *env) func(*v1alpha1.OffloadRequestStatus) {
				return func(st *v1alpha1.OffloadRequestStatus) {
					e.queued("123", "", "")(st)
					e.running("node07", &start)(st)
					st.Message = "stage-out Job stage-out-0b6c-wl: 1 active"
					e.condition(st, v1alpha1.OffloadRequestStagingOut, metav1.ConditionTrue, "StagingOut", "")
				}
			},
			state:   kueue.CheckStateReady,
			message: "job 123 ended, staging out: stage-out Job stage-out-0b6c-wl: 1 active",
			requeue: true,
		},
		{
			name: "completed before it was seen running",
			report: func(e *env) func(*v1alpha1.OffloadRequestStatus) {
				return func(st *v1alpha1.OffloadRequestStatus) {
					e.queued("123", "", "")(st)
					st.StartTime, st.CompletionTime = &start, &end
					e.finished(true, "")(st)
				}
			},
			state:   kueue.CheckStateReady,
			message: "job 123 completed",
			event:   "Normal Completed job 123 completed",
		},
		{
			name: "failed, a failed stage-out included",
			report: func(e *env) func(*v1alpha1.OffloadRequestStatus) {
				return func(st *v1alpha1.OffloadRequestStatus) {
					e.queued("123", "", "")(st)
					e.finished(false, "rank 2 exited 3")(st)
				}
			},
			state:   kueue.CheckStateReady,
			message: "job 123 failed: rank 2 exited 3",
			event:   "Warning Failed job 123 failed: rank 2 exited 3",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.reconcile()
			e.takes(1)
			e.report(1, tc.report(e))
			e.drainEvents()
			res := e.reconcile()

			if st := e.check("hpc-a"); st.State != tc.state || st.Message != tc.message {
				t.Errorf("check = %s %q, want %s %q", st.State, st.Message, tc.state, tc.message)
			}
			if (res.RequeueAfter > 0) != tc.requeue {
				t.Errorf("requeue = %v, want %v", res.RequeueAfter, tc.requeue)
			}
			if n := e.readyWrites(); (n > 0) != (tc.state == kueue.CheckStateReady) {
				t.Errorf("%d Ready writes for a %s check", n, tc.state)
			}
			if j := e.job(); len(j.Status.Conditions) != 0 || j.Status.StartTime != nil {
				t.Errorf("owner status changed: %+v", j.Status)
			}
			evs := e.drainEvents()
			if tc.event != "" && !hasEvent(evs, tc.event) {
				t.Errorf("events %v, want %q", evs, tc.event)
			}
			// Kueue holds the quota: the request stays, also once the job ended.
			if q := e.request(1); !q.DeletionTimestamp.IsZero() {
				t.Errorf("request deleted")
			}
			if len(e.requests()) != 1 {
				t.Errorf("requests %v, want the one attempt", e.requests())
			}
		})
	}
}

// The podSetUpdates of a Ready check send every PodSet that runs to the
// check's node, and stay the same on every later write: Kueue refuses a change
// while the check is Ready (recordingChecks refuses it too).
func TestReadyCarriesPodSetUpdates(t *testing.T) {
	e := newEnv(t)
	e.submitted()
	if n := e.readyWrites(); n != 0 {
		t.Fatalf("%d Ready writes while queued", n)
	}
	e.report(1, e.startedAt(e.now))
	e.reconcile()
	st := e.check("hpc-a")
	want := []kueue.PodSetUpdate{{
		Name:         "main",
		Labels:       map[string]string{v1alpha1.NodeLabel: "hpc-a-vk"},
		Annotations:  map[string]string{v1alpha1.WorkloadAnnotation: wlName},
		NodeSelector: map[string]string{v1alpha1.NodeLabel: "hpc-a-vk"},
		Tolerations: []corev1.Toleration{
			{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
			{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
		},
	}}
	if st.State != kueue.CheckStateReady || !reflect.DeepEqual(st.PodSetUpdates, want) {
		t.Fatalf("check %s with podSetUpdates %+v, want Ready with %+v", st.State, st.PodSetUpdates, want)
	}
	// Progress, an unreachable executor and the end change the message only.
	e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
		st.Message = "stage-out Job stage-out-0b6c-wl: 1 active"
		e.condition(st, v1alpha1.OffloadRequestStagingOut, metav1.ConditionTrue, "StagingOut", "")
	})
	e.reconcile()
	e.now = e.now.Add(time.Minute)
	e.reconcile()
	e.report(1, e.finished(true, ""))
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != "job 123 completed" {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
	if n := e.readyWrites(); n != 4 {
		t.Errorf("%d Ready writes, want running, staging out, unreachable and completed", n)
	}
}

func TestRunningEventsOnce(t *testing.T) {
	e := newEnv(t)
	e.submitted()
	e.report(1, e.running("node07", nil))
	e.drainEvents()
	e.reconcile()
	e.report(1, func(*v1alpha1.OffloadRequestStatus) {})
	e.reconcile()
	n := 0
	for _, ev := range e.drainEvents() {
		if strings.HasPrefix(ev, "Normal Running") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("Running events = %d, want 1", n)
	}
	if st := e.check("hpc-a"); st.Message != "job 123 running on node07" {
		t.Errorf("check message %q", st.Message)
	}
}

// A job cancelled outside the executor goes through Kueue's Retry: the
// attempt's request is deleted, the executor's finalizer cancels what is left,
// and the next reservation makes the next attempt, until the retries are spent
// and the Workload fails.
func TestCancelledRetriesThenFails(t *testing.T) {
	e := newEnv(t) // backoffLimitCount 2, backoffBaseSeconds 60
	e.reconcile()
	for i := int32(1); i <= 2; i++ {
		e.takes(i)
		e.report(i, e.queued(fmt.Sprint(122+i), "", ""))
		e.report(i, e.evicted("scancel by an operator"))
		e.reconcile()
		job := 122 + i
		st := e.check("hpc-a")
		if st.State != kueue.CheckStateRetry || st.Message != fmt.Sprintf("job %d cancelled: scancel by an operator", job) {
			t.Fatalf("attempt %d: check %s %q", i, st.State, st.Message)
		}
		if want := int32(60 << (i - 1)); ptr.Deref(st.RequeueAfterSeconds, -1) != want {
			t.Errorf("attempt %d: requeueAfterSeconds = %v, want %d", i, st.RequeueAfterSeconds, want)
		}
		if st.RetryCount != nil && *st.RetryCount != i-1 {
			t.Errorf("attempt %d: retryCount changed to %v; it is Kueue's", i, *st.RetryCount)
		}
		if !hasEvent(e.drainEvents(), fmt.Sprintf("Warning Retry job %d cancelled: scancel by an operator; retry %d of 2 in %ds", job, i, 60<<(i-1))) {
			t.Errorf("attempt %d: no Retry event", i)
		}
		q := e.request(i)
		if q.DeletionTimestamp.IsZero() {
			t.Fatalf("attempt %d: request not deleted", i)
		}
		// Retry while still reserved makes no new attempt.
		e.reconcile()
		if _, ok := e.requests()[requestOf(i+1)]; ok {
			t.Fatalf("attempt %d: a new attempt while the check is Retry", i)
		}
		// The executor cancels and lets go; the attempt finalizer keeps the
		// request, and with it the attempt number.
		e.executorLetsGo(i)
		// Kueue evicts, counts the retry, requeues and reserves again.
		e.loseReservation()
		e.reconcile()
		e.reserveAgain()
		e.evictAndReserve("hpc-a")
		e.reconcile()
		if st := e.check("hpc-a"); st.State != kueue.CheckStatePending || ptr.Deref(st.RetryCount, 0) != i {
			t.Fatalf("attempt %d: check after the new attempt %s, retryCount %v", i, st.State, st.RetryCount)
		}
		next := e.request(i + 1)
		if next.Spec.Attempt != i+1 || !next.DeletionTimestamp.IsZero() {
			t.Fatalf("attempt %d: next request %+v", i, next.Spec)
		}
		// The next attempt releases the previous one.
		e.reconcile()
		if _, ok := e.requests()[requestOf(i)]; ok {
			t.Errorf("attempt %d: superseded request still there", i)
		}
	}

	e.takes(3)
	e.report(3, e.queued("125", "", ""))
	e.report(3, e.evicted("scancel by an operator"))
	e.reconcile()
	const want = "job 125 cancelled: scancel by an operator; giving up after 2 retries"
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != want || e.failed() != want {
		t.Fatalf("check = %s %q, %s %q, want Ready and the Workload failed with %q", st.State, st.Message, v1alpha1.FailedAnnotation, e.failed(), want)
	}
	if e.readyWrites() != 1 {
		t.Errorf("Ready written %d times", e.readyWrites())
	}
	if len(e.job().Status.Conditions) != 0 {
		t.Errorf("owner status written: %+v", e.job().Status)
	}
	if q := e.request(3); q.DeletionTimestamp.IsZero() {
		t.Errorf("last attempt's request not deleted")
	}
	e.reconcile()
	if len(e.requests()) != 1 {
		t.Errorf("requests %v after failing, want the last one only", e.requests())
	}
}

// A Job whose job is lost after the admission retries through Kueue as before
// it: Retry, which evicts the Workload and suspends the Job, whose pods go.
func TestRetryAfterAdmission(t *testing.T) {
	e := newEnv(t)
	e.submitted()
	e.report(1, e.startedAt(e.now))
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady {
		t.Fatalf("check %s, want Ready once started", st.State)
	}
	e.report(1, e.evicted("node failure"))
	e.reconcile()
	st := e.check("hpc-a")
	if st.State != kueue.CheckStateRetry || st.Message != "job 123 cancelled: node failure" || ptr.Deref(st.RequeueAfterSeconds, -1) != 60 {
		t.Errorf("check = %s %q after %v, want Retry after 60", st.State, st.Message, st.RequeueAfterSeconds)
	}
	if st.PodSetUpdates != nil {
		t.Errorf("podSetUpdates %+v kept on Retry", st.PodSetUpdates)
	}
	if q := e.request(1); q.DeletionTimestamp.IsZero() {
		t.Errorf("request not deleted")
	}
	if _, ok := e.workload().Annotations[NextAttemptAnnotation]; ok {
		t.Errorf("a Job retried in place")
	}
}

// executorLetsGo is the executor done cancelling a deleted request.
func (e *env) executorLetsGo(attempt int32) {
	e.t.Helper()
	q := e.request(attempt)
	var keep []string
	for _, f := range q.Finalizers {
		if f != v1alpha1.ExecutorFinalizer {
			keep = append(keep, f)
		}
	}
	q.Finalizers = keep
	if err := e.c.Update(context.Background(), q); err != nil {
		e.t.Fatal(err)
	}
}

// Kueue drops a per-flavor check's state, retryCount included, when it evicts
// the Workload: the attempt on the request still counts the retries.
func TestAttemptSurvivesPerFlavorCheckReset(t *testing.T) {
	e := newEnv(t) // backoffLimitCount 2
	e.reconcile()
	for i := int32(1); i <= 2; i++ {
		e.takes(i)
		e.report(i, e.evicted("lost"))
		e.reconcile()
		if st := e.check("hpc-a"); st.State != kueue.CheckStateRetry || ptr.Deref(st.RequeueAfterSeconds, -1) != 60<<(i-1) {
			t.Fatalf("attempt %d: check %s after %v", i, st.State, st.RequeueAfterSeconds)
		}
		e.executorLetsGo(i)
		e.loseReservation()
		e.reconcile()
		e.reserveAgain()
		e.evictPerFlavor("hpc-a")
		e.reconcile()
		if q := e.request(i + 1); q.Spec.Attempt != i+1 {
			t.Fatalf("attempt %d: next attempt %d", i, q.Spec.Attempt)
		}
	}
	e.takes(3)
	e.report(3, e.evicted("lost"))
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || !strings.Contains(e.failed(), "giving up after 2 retries") {
		t.Errorf("check = %s %q, want the Workload failed after the retries the requests counted", st.State, st.Message)
	}
}

// withRetryStrategy replaces the OffloadConfig's retryStrategy.
func withRetryStrategy(rs *kueue.ProvisioningRequestRetryStrategy) envOption {
	return func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if sc, ok := o.(*v1alpha1.OffloadConfig); ok {
				sc.Spec.RetryStrategy = rs
			}
		}
	}
}

// withConfig changes the OffloadConfig.
func withConfig(f func(sc *v1alpha1.OffloadConfig)) envOption {
	return func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if sc, ok := o.(*v1alpha1.OffloadConfig); ok {
				f(sc)
			}
		}
	}
}

func TestRetryBackoffFromAttempt(t *testing.T) {
	cases := []struct {
		retryCount *int32
		want       int32
	}{
		{nil, 30},
		{ptr.To[int32](1), 60},
		{ptr.To[int32](3), 240},
		{ptr.To[int32](4), 300},
		{ptr.To[int32](8), 300},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(ptr.Deref(tc.retryCount, 0)), func(t *testing.T) {
			e := newEnv(t, withRetryStrategy(&kueue.ProvisioningRequestRetryStrategy{
				BackoffLimitCount: ptr.To[int32](10), BackoffBaseSeconds: ptr.To[int32](30), BackoffMaxSeconds: ptr.To[int32](300),
			}))
			// Kueue's retryCount sets the first attempt here, as after retries
			// whose requests are gone.
			e.mutateWorkload(func(wl *kueue.Workload) { findCheck(wl, "hpc-a").RetryCount = tc.retryCount })
			e.reconcile()
			attempt := ptr.Deref(tc.retryCount, 0) + 1
			e.takes(attempt)
			e.report(attempt, e.evicted(""))
			e.now = e.now.Add(time.Minute)
			e.reconcile()
			st := e.check("hpc-a")
			if st.State != kueue.CheckStateRetry || ptr.Deref(st.RequeueAfterSeconds, -1) != tc.want {
				t.Errorf("check %s, requeueAfterSeconds %v, want Retry after %d", st.State, st.RequeueAfterSeconds, tc.want)
			}
			if !st.LastTransitionTime.Time.Equal(e.now) {
				t.Errorf("lastTransitionTime %v, want %v: Kueue counts the delay from it", st.LastTransitionTime, e.now)
			}
			if !reflect.DeepEqual(st.RetryCount, tc.retryCount) {
				t.Errorf("retryCount changed to %v; it is Kueue's", st.RetryCount)
			}
		})
	}
}

func TestRetryLimitZeroFailsAtFirstLoss(t *testing.T) {
	e := newEnv(t, withRetryStrategy(&kueue.ProvisioningRequestRetryStrategy{BackoffLimitCount: ptr.To[int32](0)}))
	e.reconcile()
	e.takes(1)
	e.report(1, e.evicted("rank 0 missing from the site's status for 10m0s"))
	e.reconcile()
	const want = "the job cancelled: rank 0 missing from the site's status for 10m0s; giving up after 0 retries"
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != want || e.failed() != want {
		t.Errorf("check = %s %q, failed %q, want Ready and failed", st.State, st.Message, e.failed())
	}
}

func TestRetryStrategyDefaults(t *testing.T) {
	s := resolveSite(&v1alpha1.OffloadConfig{})
	if s.backoffLimitCount != 3 || s.backoffBaseSeconds != 60 || s.backoffMaxSeconds != 1800 {
		t.Errorf("defaults = %d %d %d, want ProvisioningRequestConfig's 3 60 1800",
			s.backoffLimitCount, s.backoffBaseSeconds, s.backoffMaxSeconds)
	}
	if s.pollInterval != 15*time.Second || s.executorLostTimeout != 10*time.Minute || s.maxQueueTime != 0 {
		t.Errorf("defaults poll %v lost %v queue %v", s.pollInterval, s.executorLostTimeout, s.maxQueueTime)
	}
	s = resolveSite(&v1alpha1.OffloadConfig{Spec: v1alpha1.OffloadConfigSpec{
		RetryStrategy: &kueue.ProvisioningRequestRetryStrategy{BackoffLimitCount: ptr.To[int32](1)},
	}})
	if s.backoffLimitCount != 1 || s.backoffBaseSeconds != 60 || s.backoffMaxSeconds != 1800 {
		t.Errorf("partial strategy = %d %d %d, want 1 60 1800", s.backoffLimitCount, s.backoffBaseSeconds, s.backoffMaxSeconds)
	}
}

// A restarted offload controller finds the attempt's request and goes on with
// it.
func TestRestartKeepsTheAttempt(t *testing.T) {
	e := newEnv(t)
	e.submitted()
	configs, err := NewConfigHelper(e.c)
	if err != nil {
		t.Fatal(err)
	}
	e.r = &WorkloadReconciler{Client: e.c, APIReader: e.c, Checks: e.checks, Configs: configs, Recorder: e.events,
		Now: func() time.Time { return e.now }}
	e.report(1, e.running("node07", nil))
	e.reconcile()
	if len(e.requests()) != 1 {
		t.Fatalf("requests %v after a restart, want the one attempt", e.requests())
	}
	if st := e.check("hpc-a"); st.Message != "job 123 running on node07" {
		t.Errorf("check message %q", st.Message)
	}
}

// A Workload that loses its reservation gets its request deleted, which the
// executor's finalizer turns into a cancelled job; the next reservation is a
// new attempt.
func TestLostReservationDeletesTheRequest(t *testing.T) {
	e := newEnv(t)
	e.submitted()
	e.loseReservation()
	e.reconcile()
	e.reconcile()
	q := e.request(1)
	if q.DeletionTimestamp.IsZero() {
		t.Fatalf("request not deleted")
	}
	if !reflect.DeepEqual(q.Finalizers, []string{v1alpha1.AttemptFinalizer, v1alpha1.ExecutorFinalizer}) {
		t.Errorf("finalizers %v, want the attempt's and the executor's", q.Finalizers)
	}
	e.reserveAgain()
	e.reconcile()
	if q := e.request(2); q.Spec.Attempt != 2 {
		t.Errorf("attempt %d, want 2", q.Spec.Attempt)
	}
	e.reconcile()
	if q := e.request(1); len(q.Finalizers) != 1 || q.Finalizers[0] != v1alpha1.ExecutorFinalizer {
		t.Errorf("finalizers of the superseded request %v, want the executor's only", q.Finalizers)
	}
}

func TestDeactivatedDeletesTheRequest(t *testing.T) {
	e := newEnv(t)
	e.submitted()
	wl := e.workload()
	wl.Spec.Active = ptr.To(false)
	if err := e.c.Update(context.Background(), wl); err != nil {
		t.Fatal(err)
	}
	e.reconcile()
	q := e.request(1)
	if q.DeletionTimestamp.IsZero() {
		t.Errorf("request not deleted")
	}
	// A deactivated Workload makes no next attempt: the executor alone holds it.
	if !reflect.DeepEqual(q.Finalizers, []string{v1alpha1.ExecutorFinalizer}) {
		t.Errorf("finalizers %v, want the executor's only", q.Finalizers)
	}
}

// A finished job's request stays until the Workload goes, so the executor
// keeps the job directory and the shadow, and the logs stay readable.
func TestFinishedWorkloadKeepsTheRequest(t *testing.T) {
	e := newEnv(t)
	e.submitted()
	e.report(1, e.finished(true, ""))
	e.reconcile()
	e.mutateWorkload(func(wl *kueue.Workload) {
		wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
			Type: kueue.WorkloadFinished, Status: metav1.ConditionTrue, Reason: "Succeeded", LastTransitionTime: metav1.Now()})
	})
	writes := len(e.checks.writes)
	e.now = e.now.Add(time.Hour)
	e.reconcile()
	q := e.request(1)
	if !q.DeletionTimestamp.IsZero() || len(e.checks.writes) != writes {
		t.Errorf("finished workload touched: request deleted %v, %d writes", !q.DeletionTimestamp.IsZero(), len(e.checks.writes)-writes)
	}
}

// A Workload that finished while its job runs, a RayJob KubeRay completed or a
// pod group whose pods a user deleted, cancels the job: its request is deleted.
// One whose job was cancelled at the site keeps it, as one that finished.
func TestFinishedWorkloadCancelsItsJob(t *testing.T) {
	for _, tc := range []struct {
		name    string
		report  func(e *env) func(st *v1alpha1.OffloadRequestStatus)
		deleted bool
	}{
		{"running", func(e *env) func(*v1alpha1.OffloadRequestStatus) { return e.startedAt(e.now) }, true},
		{"queued", func(e *env) func(*v1alpha1.OffloadRequestStatus) { return e.queued("123", "", "") }, true},
		{"cancelled at the site", func(e *env) func(*v1alpha1.OffloadRequestStatus) { return e.evicted("scancel") }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.submitted()
			e.report(1, tc.report(e))
			e.mutateWorkload(func(wl *kueue.Workload) {
				wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
					Type: kueue.WorkloadFinished, Status: metav1.ConditionTrue, Reason: "Succeeded", LastTransitionTime: metav1.Now()})
			})
			writes := len(e.checks.writes)
			e.reconcile()
			if deleted := !e.request(1).DeletionTimestamp.IsZero(); deleted != tc.deleted {
				t.Errorf("request deleted %v, want %v", deleted, tc.deleted)
			}
			if len(e.checks.writes) != writes {
				t.Errorf("check written on a finished Workload")
			}
		})
	}
}

// Garbage collection deletes the requests of a deleted Workload; the attempt
// finalizer goes with the Workload, also from the requests of an earlier
// Workload of the same name.
func TestDeletedWorkloadReleasesItsRequests(t *testing.T) {
	e := newEnv(t)
	e.submitted()
	ctx := context.Background()
	old := newRequest(&kueue.Workload{ObjectMeta: metav1.ObjectMeta{Name: wlName, Namespace: testNS, UID: "0b6c-old"}},
		"hpc-a", 7, testSite, "proj42", v1alpha1.OwnerReference{}, nil, nil)
	old.Name = "older-workload-request"
	if err := e.c.Create(ctx, old); err != nil {
		t.Fatal(err)
	}
	e.reconcile()
	if q := e.requests()["older-workload-request"]; q == nil || len(q.Finalizers) != 0 {
		t.Errorf("request of the earlier Workload %+v, want it without the attempt finalizer", q)
	}
	if q := e.request(1); !reflect.DeepEqual(q.Finalizers, []string{v1alpha1.AttemptFinalizer, v1alpha1.ExecutorFinalizer}) {
		t.Errorf("finalizers %v of the live Workload's request", q.Finalizers)
	}
	if err := e.c.Delete(ctx, e.workload()); err != nil {
		t.Fatal(err)
	}
	e.reconcile()
	if q := e.request(1); !reflect.DeepEqual(q.Finalizers, []string{v1alpha1.ExecutorFinalizer}) {
		t.Errorf("finalizers %v after the Workload went, want the executor's only", q.Finalizers)
	}
}

func TestNonJobOwnerRejected(t *testing.T) {
	e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if wl, ok := o.(*kueue.Workload); ok {
				wl.OwnerReferences[0] = metav1.OwnerReference{APIVersion: "kubeflow.org/v1", Kind: "MPIJob",
					Name: "train", UID: "mj", Controller: ptr.To(true)}
			}
		}
	})
	e.reconcile()
	st := e.check("hpc-a")
	if st.State != kueue.CheckStateRejected || !strings.Contains(st.Message, "only batch/v1 Job") || !strings.Contains(st.Message, "kubeflow.org/v1 MPIJob") {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
	if len(e.requests()) != 0 {
		t.Errorf("a request for an MPIJob workload")
	}
	if len(e.job().Status.Conditions) != 0 {
		t.Errorf("an unrelated Job was patched")
	}
}

func TestClusterQueueWithoutAccountFails(t *testing.T) {
	e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if cq, ok := o.(*kueue.ClusterQueue); ok {
				cq.Labels = nil
			}
		}
	})
	e.reconcile()
	want := "ClusterQueue hpc-a-proj42 has no label hpc.interlink.eu/account naming the account to charge"
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != want || e.failed() != want {
		t.Errorf("check = %s %q, failed %q, want Ready and failed with %q", st.State, st.Message, e.failed(), want)
	}
	if len(e.requests()) != 0 {
		t.Errorf("a request without an account")
	}
}

func TestWorkloadOfAnotherControllerIgnored(t *testing.T) {
	e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if wl, ok := o.(*kueue.Workload); ok {
				wl.Status.AdmissionChecks = wl.Status.AdmissionChecks[:1]
			}
		}
	})
	e.reconcile()
	if len(e.requests()) != 0 || len(e.checks.writes) != 0 {
		t.Errorf("acted on a workload whose checks are another controller's")
	}
}

// A check whose OffloadConfig is gone is inactive; its Workloads wait.
func TestCheckWithoutOffloadConfigWaits(t *testing.T) {
	e := newEnv(t)
	if err := e.c.Delete(context.Background(), offloadConfig()); err != nil {
		t.Fatal(err)
	}
	if res := e.reconcile(); res.RequeueAfter == 0 {
		t.Errorf("no requeue without an OffloadConfig")
	}
	if len(e.requests()) != 0 || len(e.checks.writes) != 0 {
		t.Errorf("acted without an OffloadConfig")
	}
}

// One controller serves every offload check, each with its own OffloadConfig:
// a Workload that carries two gets one request per check, each with its
// site, flavors and timing.
func TestChecksWithSeparateParameters(t *testing.T) {
	e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
		b := offloadConfig()
		b.Name, b.Spec.Workers = "hpc-b-config", []string{"hpc-b"}
		b.Spec.PollInterval = &metav1.Duration{Duration: time.Minute}
		b.Spec.Flavors = map[string]v1alpha1.FlavorTarget{
			"hpc-a-gpu": {Annotations: map[string]string{flagsKey: "--account={{.Account}} --clusters=b"}},
		}
		*objs = append(*objs, b, admissionCheck("hpc-b", ControllerName, "hpc-b-config"))
		for _, o := range *objs {
			if wl, ok := o.(*kueue.Workload); ok {
				wl.Status.AdmissionChecks = append(wl.Status.AdmissionChecks,
					kueue.AdmissionCheckState{Name: "hpc-b", State: kueue.CheckStatePending, LastTransitionTime: metav1.Now()})
			}
		}
	})
	if res := e.reconcile(); res.RequeueAfter != 15*time.Second {
		t.Errorf("requeue %v, want the shorter poll interval", res.RequeueAfter)
	}
	a, b := e.request(1), e.requests()[requestName(wlName, "hpc-b", 1)]
	if b == nil {
		t.Fatalf("requests %v, want one per check", e.requests())
	}
	if a.Spec.Site != "hpc-a" || a.Spec.PodSets[0].Annotations[flagsKey] != "--account=proj42 --partition=gpu" {
		t.Errorf("hpc-a request: site %s flags %q", a.Spec.Site, a.Spec.PodSets[0].Annotations[flagsKey])
	}
	if b.Spec.Site != "hpc-b" || b.Spec.AdmissionCheck != "hpc-b" || b.Spec.PodSets[0].Annotations[flagsKey] != "--account=proj42 --clusters=b" {
		t.Errorf("hpc-b request: site %s check %s flags %q", b.Spec.Site, b.Spec.AdmissionCheck, b.Spec.PodSets[0].Annotations[flagsKey])
	}
	// The executor of hpc-b refuses: the Workload fails through that check.
	b.Status.Conditions = []metav1.Condition{{Type: v1alpha1.OffloadRequestAccepted, Status: metav1.ConditionFalse, Reason: "Refused",
		Message: "account proj42 is not allowed at hpc-b", LastTransitionTime: metav1.NewTime(e.now)}}
	if err := e.c.Status().Update(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	e.takes(1)
	e.report(1, e.queued("123", "", ""))
	e.reconcile()
	if st := e.check("hpc-b"); st.State != kueue.CheckStateReady || st.Message != "account proj42 is not allowed at hpc-b" ||
		e.failed() != "account proj42 is not allowed at hpc-b" {
		t.Errorf("hpc-b check = %s %q, failed %q", st.State, st.Message, e.failed())
	}
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending || st.Message != "job 123 queued" {
		t.Errorf("hpc-a check = %s %q", st.State, st.Message)
	}
}

// The executor refusing a request for good fails the Workload: annotated, and
// the check Ready, so that Kueue admits it and the node fails its pods with
// the message, as a kubelet refusing them. It is never retried.
func TestRefusalFails(t *testing.T) {
	const msg = "sbatch: error: Invalid account or account/partition combination specified"
	e := newEnv(t)
	e.reconcile()
	e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
		e.condition(st, v1alpha1.OffloadRequestAccepted, metav1.ConditionFalse, "Refused", msg)
	})
	if res := e.reconcile(); res.RequeueAfter != 0 {
		t.Errorf("requeued after a refusal")
	}
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != msg || e.failed() != msg {
		t.Errorf("check = %s %q, failed %q", st.State, st.Message, e.failed())
	}
	if e.readyWrites() != 1 || len(e.job().Status.Conditions) != 0 {
		t.Errorf("%d Ready writes, owner status %+v", e.readyWrites(), e.job().Status)
	}
	if !hasEvent(e.drainEvents(), "Warning Rejected sbatch: error") {
		t.Errorf("no Rejected event")
	}
	writes := len(e.checks.writes)
	e.now = e.now.Add(time.Hour)
	e.reconcile()
	if len(e.requests()) != 1 || len(e.checks.writes) != writes || len(e.drainEvents()) != 0 {
		t.Errorf("acted again on a failed Workload: %d requests, %d writes", len(e.requests()), len(e.checks.writes)-writes)
	}
}

// A failure is a message as long as a check message may be.
func TestFailureMessageIsBounded(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
		e.condition(st, v1alpha1.OffloadRequestAccepted, metav1.ConditionFalse, "Refused", strings.Repeat("x", 40000))
	})
	e.reconcile()
	if n := len(e.failed()); n != 32768 {
		t.Errorf("%s of %d bytes, want 32768", v1alpha1.FailedAnnotation, n)
	}
}

// Without a heartbeat the check says so after two poll intervals and goes to
// Retry after executorLostTimeout, as MultiKueue's workerLostTimeout.
func TestExecutorLostThenRetry(t *testing.T) {
	e := newEnv(t)
	e.submitted()
	e.now = e.now.Add(29 * time.Second)
	e.reconcile()
	if st := e.check("hpc-a"); st.Message != "job 123 queued (Priority)" {
		t.Fatalf("check message %q within two poll intervals", st.Message)
	}
	e.drainEvents()
	e.now = e.now.Add(time.Second)
	if res := e.reconcile(); res.RequeueAfter != 15*time.Second {
		t.Errorf("requeue = %v", res.RequeueAfter)
	}
	want := "no executor answering in namespace alice since 2026-10-29T10:00:00Z"
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending || st.Message != want {
		t.Fatalf("check = %s %q, want Pending %q", st.State, st.Message, want)
	}
	if !hasEvent(e.drainEvents(), "Warning ExecutorUnreachable "+want) {
		t.Errorf("no ExecutorUnreachable event")
	}
	e.now = e.now.Add(9 * time.Minute)
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending {
		t.Fatalf("check %s before executorLostTimeout", st.State)
	}
	e.now = e.now.Add(30 * time.Second)
	e.reconcile()
	st := e.check("hpc-a")
	if st.State != kueue.CheckStateRetry || st.Message != want {
		t.Errorf("check = %s %q, want Retry", st.State, st.Message)
	}
	if ptr.Deref(st.RequeueAfterSeconds, -1) != 60 {
		t.Errorf("requeueAfterSeconds = %v, want 60", st.RequeueAfterSeconds)
	}
	if q := e.request(1); q.DeletionTimestamp.IsZero() {
		t.Errorf("request of a lost executor not deleted")
	}
}

// A request no executor ever took counts from its creation.
func TestNoExecutorEverAnswers(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	e.now = e.now.Add(10 * time.Minute)
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateRetry || st.Message != "no executor answering in namespace alice since 2026-10-29T10:00:00Z" {
		t.Errorf("check = %s %q, want Retry", st.State, st.Message)
	}
}

// An executor that keeps its heartbeat keeps the job, however long it queues.
func TestHeartbeatKeepsTheAttempt(t *testing.T) {
	e := newEnv(t)
	e.submitted()
	for i := 0; i < 30; i++ {
		e.now = e.now.Add(time.Minute)
		e.report(1, func(*v1alpha1.OffloadRequestStatus) {})
		e.reconcile()
	}
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending || st.Message != "job 123 queued (Priority)" {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
}

// A job queued longer than maxQueueTime goes through Retry, which cancels it.
func TestQueuedPastMaxQueueTimeRetries(t *testing.T) {
	e := newEnv(t, withConfig(func(sc *v1alpha1.OffloadConfig) {
		sc.Spec.MaxQueueTime = &metav1.Duration{Duration: time.Hour}
	}))
	e.submitted()
	e.now = e.now.Add(59 * time.Minute)
	e.report(1, func(*v1alpha1.OffloadRequestStatus) {})
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending {
		t.Fatalf("check %s before maxQueueTime", st.State)
	}
	e.now = e.now.Add(time.Minute)
	e.report(1, func(*v1alpha1.OffloadRequestStatus) {})
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateRetry || st.Message != "job 123 queued for longer than 1h0m0s" {
		t.Errorf("check = %s %q, want Retry", st.State, st.Message)
	}
	if q := e.request(1); q.DeletionTimestamp.IsZero() {
		t.Errorf("request of the queued job not deleted")
	}

	// A running job is past the queue.
	e = newEnv(t, withConfig(func(sc *v1alpha1.OffloadConfig) {
		sc.Spec.MaxQueueTime = &metav1.Duration{Duration: time.Hour}
	}))
	e.submitted()
	e.now = e.now.Add(2 * time.Hour)
	e.report(1, e.running("node07", nil))
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady {
		t.Errorf("running job retried: %s %q", st.State, st.Message)
	}
}

func TestLastTransitionTimeOnlyOnStateChange(t *testing.T) {
	e := newEnv(t)
	first := e.check("hpc-a").LastTransitionTime // set by Kueue with Pending
	e.reconcile()
	if st := e.check("hpc-a"); st.Message != "waiting for the executor in namespace alice" || !st.LastTransitionTime.Equal(&first) {
		t.Fatalf("after the request: %q at %v, want Kueue's transition time %v", st.Message, st.LastTransitionTime, first)
	}
	e.takes(1)
	e.report(1, e.queued("123", "Resources", ""))
	e.now = e.now.Add(time.Minute)
	e.report(1, func(*v1alpha1.OffloadRequestStatus) {})
	e.reconcile()
	st := e.check("hpc-a")
	if st.Message != "job 123 queued (Resources)" || !st.LastTransitionTime.Equal(&first) {
		t.Errorf("message %q at %v, want new message and unchanged time", st.Message, st.LastTransitionTime)
	}
	n := len(e.checks.writes)
	e.reconcile()
	if len(e.checks.writes) != n {
		t.Errorf("unchanged state written again")
	}
	e.now = e.now.Add(time.Minute)
	e.report(1, e.evicted(""))
	e.reconcile()
	if st := e.check("hpc-a"); !st.LastTransitionTime.Time.Equal(e.now) {
		t.Errorf("transition time %v after state change, want %v", st.LastTransitionTime, e.now)
	}
}

// Every PodSet runs in the one allocation, so they must share a flavor: a JobSet
// whose leader and workers Kueue put on different queues is rejected.
func TestPodSetsShareOneFlavor(t *testing.T) {
	twoPodSets := func(launcher, worker map[corev1.ResourceName]kueue.ResourceFlavorReference) envOption {
		return func(objs *[]client.Object, _ *interceptor.Funcs) {
			for _, o := range *objs {
				if wl, ok := o.(*kueue.Workload); ok {
					wl.Spec.PodSets = []kueue.PodSet{
						{Name: "launcher", Count: 1, Template: podTemplate()},
						{Name: "worker", Count: 2, Template: podTemplate()},
					}
					wl.Status.Admission.PodSetAssignments = []kueue.PodSetAssignment{
						{Name: "launcher", Count: ptr.To[int32](1), Flavors: launcher},
						{Name: "worker", Count: ptr.To[int32](2), Flavors: worker},
					}
				}
			}
		}
	}

	e := newEnv(t, twoPodSets(
		map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-gpu"},
		map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-gpu", "nvidia.com/gpu": "hpc-a-gpu"}))
	e.reconcile()
	var got []string
	for _, ps := range e.request(1).Spec.PodSets {
		got = append(got, fmt.Sprintf("%s/%d/%s", ps.Name, ps.Count, ps.Annotations[flagsKey]))
	}
	if want := []string{"launcher/1/--account=proj42 --partition=gpu", "worker/2/--account=proj42 --partition=gpu"}; !reflect.DeepEqual(got, want) {
		t.Errorf("podSets = %v, want %v", got, want)
	}
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending {
		t.Errorf("check = %s %q, want Pending", st.State, st.Message)
	}

	e = newEnv(t, twoPodSets(
		map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-cpu"},
		map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-gpu", "nvidia.com/gpu": "hpc-a-gpu"}))
	e.reconcile()
	if len(e.requests()) != 0 {
		t.Fatalf("requests %v, want none", e.requests())
	}
	want := "podSet launcher was admitted on hpc-a-cpu and podSet worker on hpc-a-gpu: the Workload runs in one allocation, so pin one queue on every PodSet"
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != want || e.failed() != want {
		t.Errorf("check = %s %q, failed %q, want Ready and failed with %q", st.State, st.Message, e.failed(), want)
	}
}

// offload.interlink.eu/independent-pods on the owner reaches the executor in
// ownerAnnotations.
func TestIndependentPodsIsForwarded(t *testing.T) {
	e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if job, ok := o.(*batchv1.Job); ok {
				job.Annotations[executorapi.IndependentPodsAnnotation] = "true"
			}
		}
	})
	e.reconcile()
	if got := e.request(1).Spec.OwnerAnnotations[executorapi.IndependentPodsAnnotation]; got != "true" {
		t.Errorf("%s = %q in ownerAnnotations, want the owner's \"true\"", executorapi.IndependentPodsAnnotation, got)
	}
}

// The wall time of a flavor comes from the Workload's maximumExecutionTimeSeconds,
// rounded up to minutes, which the request also carries.
func TestFlavorAnnotationsTakeTheWallTime(t *testing.T) {
	for _, tt := range []struct {
		seconds *int32
		want    string
	}{
		{nil, "--account=proj42 --partition=gpu --qos=debug --time=30"},
		// 3601 s is 61 minutes, and a minute more: Kueue deactivates at its own limit first
		{ptr.To[int32](3601), "--account=proj42 --partition=gpu --qos=debug --time=62"},
	} {
		e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
			for _, o := range *objs {
				if wl, ok := o.(*kueue.Workload); ok {
					wl.Spec.MaximumExecutionTimeSeconds = tt.seconds
					wl.Spec.Priority = ptr.To[int32](100)
					wl.Status.Admission.PodSetAssignments[0].Flavors = map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-debug", "nvidia.com/gpu": "hpc-a-debug"}
				}
			}
		})
		e.reconcile()
		sp := e.request(1).Spec
		if got := sp.PodSets[0].Annotations[flagsKey]; got != tt.want {
			t.Errorf("maximumExecutionTimeSeconds %v: flags %q, want %q", tt.seconds, got, tt.want)
		}
		if !reflect.DeepEqual(sp.MaximumExecutionTimeSeconds, tt.seconds) || ptr.Deref(sp.Priority, 0) != 100 {
			t.Errorf("maximumExecutionTimeSeconds %v priority %v", sp.MaximumExecutionTimeSeconds, sp.Priority)
		}
	}
}

// A PodSet on no listed flavor would reach the site without an account.
func TestUnlistedFlavorRejected(t *testing.T) {
	e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if wl, ok := o.(*kueue.Workload); ok {
				wl.Status.Admission.PodSetAssignments[0].Flavors = map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "default"}
			}
		}
	})
	e.reconcile()
	if len(e.requests()) != 0 {
		t.Fatalf("requests %v, want none", e.requests())
	}
	if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || !strings.Contains(e.failed(), "none of the admitted flavors default is listed") {
		t.Errorf("check = %s %q, failed %q", st.State, st.Message, e.failed())
	}
}

// The membership controller's ClusterQueue of queue debug of proj42: the
// account is its label, not a part of its name, and the flags are those of
// the flavor admitted, as on the ClusterQueue of every flavor.
func TestQueueClusterQueueResolvesAccountAndFlavor(t *testing.T) {
	e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			switch o := o.(type) {
			case *kueue.ClusterQueue:
				o.Name = "hpc-a-proj42-debug"
				o.Labels = map[string]string{v1alpha1.AccountLabel: "proj42", "hpc.interlink.eu/queue": "debug"}
			case *kueue.Workload:
				o.Status.Admission.ClusterQueue = "hpc-a-proj42-debug"
				o.Status.Admission.PodSetAssignments[0].Flavors = map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-debug", "nvidia.com/gpu": "hpc-a-debug"}
			}
		}
	})
	e.reconcile()
	if got, want := e.request(1).Spec.PodSets[0].Annotations[flagsKey], "--account=proj42 --partition=gpu --qos=debug --time=30"; got != want {
		t.Errorf("flags %q, want %q", got, want)
	}
	if e.failed() != "" {
		t.Errorf("failed %q", e.failed())
	}
}

func TestRequestNameIsBounded(t *testing.T) {
	if got := requestName("wl", "hpc-a", 3); got != "wl-hpc-a-3" {
		t.Errorf("short name %q", got)
	}
	long := strings.Repeat("w", 250)
	got := requestName(long, "hpc-a", 1)
	if len(got) != 253 || got == requestName(long, "hpc-a", 2) {
		t.Errorf("long name %q (%d), want 253 characters distinct per attempt", got, len(got))
	}
}
