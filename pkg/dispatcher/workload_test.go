package dispatcher

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/interlink-hq/interlink-dispatch/api/v1alpha1"
	"github.com/interlink-hq/interlink-dispatch/pkg/agentapi"
)

func TestSubmitOnceWithResolvedPodSets(t *testing.T) {
	e := newEnv(t)
	res := e.reconcile()
	if res.RequeueAfter != 15*time.Second {
		t.Errorf("requeue after submit = %v, want the poll interval", res.RequeueAfter)
	}
	e.reconcile()
	e.reconcile()

	submits, _, gets := e.agent.counts()
	if submits != 1 {
		t.Fatalf("submits = %d, want 1", submits)
	}
	if gets != 2 {
		t.Errorf("polls = %d, want 2", gets)
	}
	req := e.agent.submits[0]
	if req.APIVersion != agentapi.APIVersion || req.WorkloadUID != string(wlUID) || req.WorkloadName != wlName ||
		req.Namespace != testNS || req.Site != testSite || req.Account != "proj42" {
		t.Errorf("submit header fields wrong: %+v", req)
	}
	if req.Owner != (agentapi.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: "train", UID: string(jobUID)}) {
		t.Errorf("owner = %+v", req.Owner)
	}
	if len(req.PodSets) != 1 {
		t.Fatalf("podSets = %d, want 1", len(req.PodSets))
	}
	ps := req.PodSets[0]
	if ps.Name != "main" || ps.Count != 4 || ps.Partition != "gpu" {
		t.Errorf("podSet = %s count %d partition %q, want main 4 gpu", ps.Name, ps.Count, ps.Partition)
	}
	if want := []string{"hf-token", "s3-creds"}; !reflect.DeepEqual(ps.SecretRefs, want) {
		t.Errorf("secretRefs = %v, want %v", ps.SecretRefs, want)
	}
	if want := []string{"train-config", "train-env"}; !reflect.DeepEqual(ps.ConfigMapRefs, want) {
		t.Errorf("configMapRefs = %v, want %v", ps.ConfigMapRefs, want)
	}
	ann := ps.Template.Annotations
	if ann["slurm-job.vk.io/flags"] != "--time=04:00:00 --gres=gpu:1" {
		t.Errorf("owner flags not merged: %v", ann)
	}
	if ann["interlink.eu/mode"] != "template" {
		t.Errorf("template annotation must win over the owner's, got %q", ann["interlink.eu/mode"])
	}
	if _, ok := ann["example.org/unrelated"]; ok {
		t.Errorf("annotation outside the forwarded prefixes was merged")
	}
	hdr := e.agent.submitHdrs[0]
	if hdr.Get("Authorization") != "Bearer "+testToken {
		t.Errorf("Authorization = %q", hdr.Get("Authorization"))
	}
	if hdr.Get(agentapi.SessionHeader) != string(wlUID) {
		t.Errorf("%s = %q", agentapi.SessionHeader, hdr.Get(agentapi.SessionHeader))
	}

	rec := e.record()
	if rec.Status.JobID != "123" || rec.Spec.Account != "proj42" || rec.Spec.AgentEndpoint != e.agent.srv.URL {
		t.Errorf("record = %+v / %+v", rec.Spec, rec.Status)
	}
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending || st.Message != "Slurm job 123 queued (Priority)" {
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
	if !hasEvent(e.drainEvents(), "Normal Submitted submitted as Slurm job 123") {
		t.Errorf("no Submitted event")
	}
}

func TestAgentStates(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 10, 29, 10, 14, 2, 0, time.UTC))
	end := metav1.NewTime(time.Date(2026, 10, 29, 11, 0, 0, 0, time.UTC))
	cases := []struct {
		name      string
		job       agentapi.JobStatus
		message   string
		event     string
		jobStatus func(t *testing.T, j *batchv1.Job)
		requeue   bool
	}{
		{
			name:    "queued",
			job:     agentapi.JobStatus{State: agentapi.StateQueued, Reason: "Resources"},
			message: "Slurm job 123 queued (Resources)",
			requeue: true,
			jobStatus: func(t *testing.T, j *batchv1.Job) {
				if len(j.Status.Conditions) != 0 {
					t.Errorf("owner status changed while queued: %+v", j.Status)
				}
			},
		},
		{
			name:    "running",
			job:     agentapi.JobStatus{State: agentapi.StateRunning, NodeName: "node07", StartTime: &start},
			message: "Slurm job 123 running on node07 since 2026-10-29T10:14:02Z",
			event:   "Normal Running Slurm job 123 running on node07",
			requeue: true,
			jobStatus: func(t *testing.T, j *batchv1.Job) {
				if len(j.Status.Conditions) != 0 {
					t.Errorf("owner status changed while running: %+v", j.Status)
				}
			},
		},
		{
			name:    "completed",
			job:     agentapi.JobStatus{State: agentapi.StateCompleted, StartTime: &start, CompletionTime: &end},
			message: "Slurm job 123 completed",
			event:   "Normal Completed Slurm job 123 completed",
			jobStatus: func(t *testing.T, j *batchv1.Job) {
				c := jobCondition(j, batchv1.JobComplete)
				if c == nil || c.Status != corev1.ConditionTrue || c.Reason != ReasonDispatch || !strings.Contains(c.Message, "123") {
					t.Errorf("Complete condition = %+v", c)
				}
				if j.Status.Succeeded != 4 || j.Status.CompletedIndexes != "0-3" {
					t.Errorf("succeeded %d indexes %q, want 4 and 0-3", j.Status.Succeeded, j.Status.CompletedIndexes)
				}
				if !j.Status.StartTime.Equal(&start) || !j.Status.CompletionTime.Equal(&end) {
					t.Errorf("times %v %v", j.Status.StartTime, j.Status.CompletionTime)
				}
				if jobCondition(j, batchv1.JobFailed) != nil {
					t.Errorf("Failed set on a completed job")
				}
			},
		},
		{
			name:    "failed",
			job:     agentapi.JobStatus{State: agentapi.StateFailed, ExitCode: 3, Message: "rank 2 exited 3"},
			message: "Slurm job 123 failed: rank 2 exited 3",
			event:   "Warning Failed Slurm job 123 failed: rank 2 exited 3",
			jobStatus: func(t *testing.T, j *batchv1.Job) {
				c := jobCondition(j, batchv1.JobFailed)
				if c == nil || c.Status != corev1.ConditionTrue || c.Reason != ReasonDispatch || !strings.Contains(c.Message, "rank 2 exited 3") {
					t.Errorf("Failed condition = %+v", c)
				}
				if j.Status.Failed != 1 || jobCondition(j, batchv1.JobComplete) != nil {
					t.Errorf("failed %d, complete %v", j.Status.Failed, jobCondition(j, batchv1.JobComplete))
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.reconcile() // submit
			e.drainEvents()
			tc.job.APIVersion, tc.job.JobID = agentapi.APIVersion, "123"
			e.agent.set(func(a *fakeAgent) { a.job = tc.job })
			res := e.reconcile()

			if st := e.check("hpc-a"); st.State != kueue.CheckStatePending || st.Message != tc.message {
				t.Errorf("check = %s %q, want Pending %q", st.State, st.Message, tc.message)
			}
			if (res.RequeueAfter > 0) != tc.requeue {
				t.Errorf("requeue = %v, want %v", res.RequeueAfter, tc.requeue)
			}
			tc.jobStatus(t, e.job())
			if rec := e.record(); rec.Status.State != tc.job.State {
				t.Errorf("record state = %q", rec.Status.State)
			}
			evs := e.drainEvents()
			if tc.event != "" && !hasEvent(evs, tc.event) {
				t.Errorf("events %v, want %q", evs, tc.event)
			}
			for _, w := range e.checks.writes {
				if w.State == kueue.CheckStateReady {
					t.Errorf("check set Ready; the job must never be unsuspended (D1)")
				}
			}
			// Terminal states are not polled again.
			_, _, before := e.agent.counts()
			e.reconcile()
			if _, _, after := e.agent.counts(); !tc.requeue && after != before {
				t.Errorf("terminal job polled again")
			}
		})
	}
}

func TestRunningRecordsNodeAndEventsOnce(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	e.agent.set(func(a *fakeAgent) { a.job.State, a.job.NodeName = agentapi.StateRunning, "node07" })
	e.drainEvents()
	e.reconcile()
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
	if rec := e.record(); rec.Status.NodeName != "node07" || rec.Status.LastPoll == nil {
		t.Errorf("record = %+v", rec.Status)
	}
}

func TestCancelledRetriesThenRejects(t *testing.T) {
	e := newEnv(t) // maxRetries 2
	e.reconcile()
	e.agent.set(func(a *fakeAgent) { a.job.State, a.job.Message = agentapi.StateCancelled, "scancel by an operator" })

	for i := 1; i <= 2; i++ {
		e.reconcile()
		_, deletes, _ := e.agent.counts()
		if deletes != i {
			t.Fatalf("round %d: deletes = %d", i, deletes)
		}
		if e.agent.deletes[i-1] != string(wlUID) {
			t.Errorf("DELETE for %s", e.agent.deletes[i-1])
		}
		rec := e.record()
		if rec.Status.Retries != int32(i) || rec.Status.JobID != "" {
			t.Fatalf("round %d: record %+v", i, rec.Status)
		}
		st := e.check("hpc-a")
		if st.State != kueue.CheckStateRetry || !strings.Contains(st.Message, "cancelled: scancel by an operator") {
			t.Fatalf("round %d: check %s %q", i, st.State, st.Message)
		}
		if !hasEvent(e.drainEvents(), "Warning Retry") {
			t.Errorf("round %d: no Retry event", i)
		}
		// Retry while still reserved must not resubmit.
		e.reconcile()
		if s, _, _ := e.agent.counts(); s != i {
			t.Fatalf("round %d: resubmitted while the check is Retry", i)
		}
		// Kueue evicts, requeues and reserves again: the check is back to Pending.
		e.mutateWorkload(func(wl *kueue.Workload) { findCheck(wl, "hpc-a").State = kueue.CheckStatePending })
		e.reconcile()
		if s, _, _ := e.agent.counts(); s != i+1 {
			t.Fatalf("round %d: submits = %d, want %d", i, s, i+1)
		}
	}

	e.reconcile()
	st := e.check("hpc-a")
	if st.State != kueue.CheckStateRejected || !strings.Contains(st.Message, "giving up after 2 retries") {
		t.Fatalf("check = %s %q, want Rejected", st.State, st.Message)
	}
	if c := jobCondition(e.job(), batchv1.JobFailed); c == nil || c.Reason != ReasonRejected {
		t.Errorf("owner Failed = %+v", c)
	}
	if _, deletes, _ := e.agent.counts(); deletes != 3 {
		t.Errorf("deletes = %d, want 3", deletes)
	}
	e.reconcile()
	if s, _, _ := e.agent.counts(); s != 3 {
		t.Errorf("submitted after Rejected")
	}
}

func TestPermanentSubmitErrorRejects(t *testing.T) {
	e := newEnv(t)
	e.agent.set(func(a *fakeAgent) {
		a.submitCode = http.StatusBadRequest
		a.submitBody = agentapi.ErrorResponse{APIVersion: agentapi.APIVersion, Permanent: true,
			Message: "sbatch: error: Invalid account or account/partition combination specified"}
	})
	res := e.reconcile()
	if res.RequeueAfter != 0 {
		t.Errorf("requeued after a permanent error")
	}
	st := e.check("hpc-a")
	if st.State != kueue.CheckStateRejected || st.Message != "sbatch: error: Invalid account or account/partition combination specified" {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
	j := e.job()
	if c := jobCondition(j, batchv1.JobFailed); c == nil || c.Status != corev1.ConditionTrue || c.Reason != ReasonRejected {
		t.Errorf("owner Failed = %+v", c)
	}
	if rec := e.record(); rec.Status.State != agentapi.StateRejected || rec.Status.JobID != "" {
		t.Errorf("record = %+v", rec.Status)
	}
	if !hasEvent(e.drainEvents(), "Warning Rejected sbatch: error") {
		t.Errorf("no Rejected event")
	}
	e.reconcile()
	if s, _, _ := e.agent.counts(); s != 1 {
		t.Errorf("submits = %d after Rejected", s)
	}
}

func TestTransientSubmitErrorKeepsPending(t *testing.T) {
	e := newEnv(t)
	e.agent.set(func(a *fakeAgent) {
		a.submitCode = http.StatusServiceUnavailable
		a.submitBody = agentapi.ErrorResponse{Permanent: true, Message: "ssh: connect to host login.hpc-a.example.org: timed out"}
	})
	first := e.reconcile()
	second := e.reconcile()
	if first.RequeueAfter <= 0 || second.RequeueAfter <= first.RequeueAfter {
		t.Errorf("backoff %v then %v, want growing", first.RequeueAfter, second.RequeueAfter)
	}
	st := e.check("hpc-a")
	if st.State != kueue.CheckStatePending || st.Message != "" {
		t.Errorf("check = %s %q, want untouched Pending", st.State, st.Message)
	}
	if len(e.checks.writes) != 0 {
		t.Errorf("check written on a transient error: %+v", e.checks.writes)
	}
	if c := jobCondition(e.job(), batchv1.JobFailed); c != nil {
		t.Errorf("owner failed on a transient error")
	}
	if rec := e.record(); rec.Status.JobID != "" {
		t.Errorf("record has job %q", rec.Status.JobID)
	}
	e.agent.set(func(a *fakeAgent) {
		a.submitCode = http.StatusCreated
		a.submitBody = agentapi.SubmitResponse{APIVersion: agentapi.APIVersion, JobID: "124", Accepted: true}
	})
	e.reconcile()
	if rec := e.record(); rec.Status.JobID != "124" {
		t.Errorf("record job = %q after recovery", rec.Status.JobID)
	}
	if d := e.r.backoff(wlUID); d != time.Second {
		t.Errorf("backoff not reset after success: %v", d)
	}
}

func TestRestartBeforeSubmitReturned(t *testing.T) {
	e := newEnv(t)
	// A previous dispatcher wrote the record, then died before storing the job ID.
	rec := &v1alpha1.SubmitRecord{
		ObjectMeta: metav1.ObjectMeta{Name: string(wlUID), Namespace: testDNS},
		Spec: v1alpha1.SubmitRecordSpec{Workload: v1alpha1.WorkloadReference{Namespace: testNS, Name: wlName},
			Site: testSite, Account: "proj42", AgentEndpoint: e.agent.srv.URL},
	}
	if err := e.c.Create(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	e.agent.set(func(a *fakeAgent) {
		a.submitCode = http.StatusOK
		a.submitBody = agentapi.SubmitResponse{APIVersion: agentapi.APIVersion, JobID: "8812345", Accepted: true}
	})
	e.reconcile()
	e.reconcile()
	if s, _, _ := e.agent.counts(); s != 1 {
		t.Fatalf("submits = %d, want 1", s)
	}
	if got := e.record(); got.Status.JobID != "8812345" {
		t.Errorf("record job = %q, want the agent's existing job", got.Status.JobID)
	}
}

func TestEvictionDeletesOnce(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	e.mutateWorkload(func(wl *kueue.Workload) {
		for i := range wl.Status.Conditions {
			if wl.Status.Conditions[i].Type == kueue.WorkloadQuotaReserved {
				wl.Status.Conditions[i].Status = metav1.ConditionFalse
			}
		}
		wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
			Type: kueue.WorkloadEvicted, Status: metav1.ConditionTrue, Reason: "Preempted", LastTransitionTime: metav1.Now()})
		wl.Status.Admission = nil
	})
	e.reconcile()
	e.reconcile()
	_, deletes, _ := e.agent.counts()
	if deletes != 1 || e.agent.deletes[0] != string(wlUID) {
		t.Fatalf("deletes = %v, want one for %s", e.agent.deletes, wlUID)
	}
	if rec := e.record(); rec.Status.JobID != "" {
		t.Errorf("record still holds job %q", rec.Status.JobID)
	}
}

func TestDeactivatedDeletesJob(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	wl := e.workload()
	wl.Spec.Active = ptr.To(false)
	if err := e.c.Update(context.Background(), wl); err != nil {
		t.Fatal(err)
	}
	e.reconcile()
	if _, deletes, _ := e.agent.counts(); deletes != 1 {
		t.Errorf("deletes = %d, want 1", deletes)
	}
}

func TestFinishedWorkloadIsLeftAlone(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	e.mutateWorkload(func(wl *kueue.Workload) {
		wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
			Type: kueue.WorkloadFinished, Status: metav1.ConditionTrue, Reason: "Succeeded", LastTransitionTime: metav1.Now()})
	})
	_, _, gets := e.agent.counts()
	e.reconcile()
	if s, d, g := e.agent.counts(); s != 1 || d != 0 || g != gets {
		t.Errorf("calls on a finished workload: submits %d deletes %d gets %d", s, d, g-gets)
	}
}

func TestGarbageCollectRemovesOrphans(t *testing.T) {
	e := newEnv(t)
	e.reconcile() // live record for wlUID
	ctx := context.Background()
	mk := func(name, site, wl string) {
		rec := &v1alpha1.SubmitRecord{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testDNS},
			Spec: v1alpha1.SubmitRecordSpec{Workload: v1alpha1.WorkloadReference{Namespace: testNS, Name: wl},
				Site: site, AgentEndpoint: e.agent.srv.URL},
		}
		if err := e.c.Create(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	mk("gone-uid", testSite, "job-gone-1")
	mk("old-uid", testSite, wlName) // same name, older Workload UID
	mk("other-site-uid", "hpc-b", "job-gone-2")

	if err := e.r.GarbageCollect(ctx); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(e.agent.deletes, ","); got != "gone-uid,old-uid" && got != "old-uid,gone-uid" {
		t.Errorf("deletes = %v, want gone-uid and old-uid", e.agent.deletes)
	}
	for name, want := range map[string]bool{"gone-uid": false, "old-uid": false, "other-site-uid": true, string(wlUID): true} {
		err := e.c.Get(ctx, types.NamespacedName{Namespace: testDNS, Name: name}, &v1alpha1.SubmitRecord{})
		if exists := err == nil; exists != want {
			t.Errorf("record %s exists = %v, want %v (%v)", name, exists, want, err)
		}
	}
}

func TestDeletedWorkloadCleansUpOnWatch(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	if err := e.c.Delete(context.Background(), e.workload()); err != nil {
		t.Fatal(err)
	}
	e.reconcile()
	if _, deletes, _ := e.agent.counts(); deletes != 1 {
		t.Errorf("deletes = %d, want 1", deletes)
	}
	err := e.c.Get(context.Background(), types.NamespacedName{Namespace: testDNS, Name: string(wlUID)}, &v1alpha1.SubmitRecord{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("record not removed: %v", err)
	}
}

func TestAgentEndpointFromNamespaceOnly(t *testing.T) {
	evil := newFakeAgent(t)
	e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if wl, ok := o.(*kueue.Workload); ok {
				wl.Annotations = map[string]string{AgentEndpointAnnotation: evil.srv.URL}
			}
		}
	})
	e.reconcile()
	e.reconcile()
	if s, _, g := evil.counts(); s != 0 || g != 0 {
		t.Errorf("the Workload's annotation redirected the dispatch: %d submits", s)
	}
	if s, _, _ := e.agent.counts(); s != 1 {
		t.Errorf("namespace agent got %d submits, want 1", s)
	}

	s := resolveSite(siteConfig())
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "bob"}}
	if got := s.agentEndpoint(ns); got != "http://interlink-agent.bob.svc:8000" {
		t.Errorf("default endpoint = %q", got)
	}
	sc := siteConfig()
	sc.Spec.AgentService, sc.Spec.AgentPort = "agent", 9000
	if got := resolveSite(sc).agentEndpoint(ns); got != "http://agent.bob.svc:9000" {
		t.Errorf("configured endpoint = %q", got)
	}
}

func TestNonJobOwnerRejected(t *testing.T) {
	e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if wl, ok := o.(*kueue.Workload); ok {
				wl.OwnerReferences[0] = metav1.OwnerReference{APIVersion: "ray.io/v1", Kind: "RayJob",
					Name: "train", UID: "rj", Controller: ptr.To(true)}
			}
		}
	})
	e.reconcile()
	st := e.check("hpc-a")
	if st.State != kueue.CheckStateRejected || !strings.Contains(st.Message, "only batch/v1 Job") || !strings.Contains(st.Message, "ray.io/v1 RayJob") {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
	if s, _, _ := e.agent.counts(); s != 0 {
		t.Errorf("submitted a RayJob workload")
	}
	if len(e.job().Status.Conditions) != 0 {
		t.Errorf("an unrelated Job was patched")
	}
}

func TestAccountRuleMismatchRejects(t *testing.T) {
	e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if sc, ok := o.(*v1alpha1.SiteConfig); ok {
				sc.Spec.AccountRule.StripPrefix = ptr.To("site-b-")
			}
		}
	})
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateRejected || !strings.Contains(st.Message, "account rule") {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
	if s, _, _ := e.agent.counts(); s != 0 {
		t.Errorf("submitted under a wrong account")
	}
}

func TestWorkloadOfAnotherSiteIgnored(t *testing.T) {
	e := newEnv(t)
	e.r.Site = "hpc-b"
	if err := e.c.Create(context.Background(), &v1alpha1.SiteConfig{ObjectMeta: metav1.ObjectMeta{Name: "hpc-b"}}); err != nil {
		t.Fatal(err)
	}
	e.reconcile()
	if s, _, _ := e.agent.counts(); s != 0 || len(e.checks.writes) != 0 {
		t.Errorf("dispatcher of hpc-b acted on an hpc-a workload")
	}
}

func TestAgentLostThenRetry(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	e.agent.set(func(a *fakeAgent) { a.jobCode = http.StatusServiceUnavailable })
	res := e.reconcile()
	if res.RequeueAfter != 15*time.Second {
		t.Errorf("requeue = %v", res.RequeueAfter)
	}
	rec := e.record()
	if rec.Status.FirstUnreachable == nil || !rec.Status.FirstUnreachable.Time.Equal(e.now) {
		t.Fatalf("firstUnreachable = %v", rec.Status.FirstUnreachable)
	}
	e.now = e.now.Add(9 * time.Minute)
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending {
		t.Fatalf("check %s before agentLostTimeout", st.State)
	}
	e.now = e.now.Add(time.Minute)
	e.reconcile()
	st := e.check("hpc-a")
	if st.State != kueue.CheckStateRetry || !strings.Contains(st.Message, "unreachable since 2026-10-29T10:00:00Z") {
		t.Errorf("check = %s %q", st.State, st.Message)
	}
	if rec := e.record(); rec.Status.JobID != "" || rec.Status.State != recordLost {
		t.Errorf("record = %+v", rec.Status)
	}
	if _, deletes, _ := e.agent.counts(); deletes != 0 {
		t.Errorf("DELETE sent to a lost agent")
	}
}

func TestAgentForgotJobRetries(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	e.agent.set(func(a *fakeAgent) { a.jobCode = http.StatusNotFound })
	e.reconcile()
	if st := e.check("hpc-a"); st.State != kueue.CheckStateRetry {
		t.Errorf("check = %s, want Retry", st.State)
	}
}

func TestLastTransitionTimeOnlyOnStateChange(t *testing.T) {
	e := newEnv(t)
	first := e.check("hpc-a").LastTransitionTime // set by Kueue with Pending
	e.reconcile()
	if st := e.check("hpc-a"); st.Message != "submitted as Slurm job 123" || !st.LastTransitionTime.Equal(&first) {
		t.Fatalf("after submit: %q at %v, want Kueue's transition time %v", st.Message, st.LastTransitionTime, first)
	}
	e.now = e.now.Add(time.Minute)
	e.agent.set(func(a *fakeAgent) { a.job.Reason = "Resources" })
	e.reconcile()
	st := e.check("hpc-a")
	if st.Message != "Slurm job 123 queued (Resources)" || !st.LastTransitionTime.Equal(&first) {
		t.Errorf("message %q at %v, want new message and unchanged time", st.Message, st.LastTransitionTime)
	}
	n := len(e.checks.writes)
	e.reconcile()
	if len(e.checks.writes) != n {
		t.Errorf("unchanged state written again")
	}
	e.now = e.now.Add(time.Minute)
	e.agent.set(func(a *fakeAgent) { a.job.State = agentapi.StateCancelled })
	e.reconcile()
	if st := e.check("hpc-a"); !st.LastTransitionTime.Time.Equal(e.now) {
		t.Errorf("transition time %v after state change, want %v", st.LastTransitionTime, e.now)
	}
}

func TestOwnerPatchFallsBackWithoutInterimConditions(t *testing.T) {
	var refused int
	e := newEnv(t, func(_ *[]client.Object, f *interceptor.Funcs) {
		f.SubResourcePatch = func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				data, _ := patch.Data(obj)
				if strings.Contains(string(data), string(batchv1.JobSuccessCriteriaMet)) {
					refused++
					return apierrors.NewInvalid(schema.GroupKind{Group: "batch", Kind: "Job"}, "train",
						field.ErrorList{field.Invalid(field.NewPath("status", "conditions"), "SuccessCriteriaMet", "not allowed")})
				}
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		}
	})
	e.reconcile()
	e.agent.set(func(a *fakeAgent) { a.job.State = agentapi.StateCompleted })
	e.reconcile()
	j := e.job()
	if refused != 1 {
		t.Errorf("first patch with SuccessCriteriaMet tried %d times, want 1", refused)
	}
	if c := jobCondition(j, batchv1.JobComplete); c == nil || c.Status != corev1.ConditionTrue {
		t.Errorf("Complete not set after fallback")
	}
	if jobCondition(j, batchv1.JobSuccessCriteriaMet) != nil {
		t.Errorf("SuccessCriteriaMet present after fallback")
	}
}

func TestOwnerPatchCarriesInterimConditions(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	e.agent.set(func(a *fakeAgent) { a.job.State = agentapi.StateFailed })
	e.reconcile()
	j := e.job()
	ft := jobCondition(j, batchv1.JobFailureTarget)
	if ft == nil || ft.Status != corev1.ConditionTrue {
		t.Errorf("FailureTarget = %+v", ft)
	}
}

func TestTokenReReadOnChange(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	path := e.r.Agent.Token.Path
	if err := writeFileWithMtime(path, "token-2", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	e.agent.set(func(a *fakeAgent) { a.job.State = agentapi.StateCancelled })
	e.reconcile() // DELETE + Retry
	e.mutateWorkload(func(wl *kueue.Workload) { findCheck(wl, "hpc-a").State = kueue.CheckStatePending })
	e.reconcile() // second submit
	if got := e.agent.submitHdrs[1].Get("Authorization"); got != "Bearer token-2" {
		t.Errorf("Authorization after rotation = %q", got)
	}
}

func TestRecordOfAnotherSiteIgnored(t *testing.T) {
	e := newEnv(t)
	rec := &v1alpha1.SubmitRecord{
		ObjectMeta: metav1.ObjectMeta{Name: string(wlUID), Namespace: testDNS},
		Spec: v1alpha1.SubmitRecordSpec{Workload: v1alpha1.WorkloadReference{Namespace: testNS, Name: wlName},
			Site: "hpc-b", AgentEndpoint: e.agent.srv.URL},
	}
	if err := e.c.Create(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	e.mutateRecord(func(r *v1alpha1.SubmitRecord) { r.Status.JobID = "9" })
	e.mutateWorkload(func(wl *kueue.Workload) {
		wl.Status.Conditions[0].Status = metav1.ConditionFalse
		wl.Status.Admission = nil
	})
	e.reconcile()
	if s, d, g := e.agent.counts(); s+d+g != 0 {
		t.Errorf("acted on another site's record: submits %d deletes %d gets %d", s, d, g)
	}
	if got := e.record(); got.Status.JobID != "9" {
		t.Errorf("another site's record changed: %+v", got.Status)
	}
}

func TestPodSetsGetTheirOwnPartitions(t *testing.T) {
	e := newEnv(t, func(objs *[]client.Object, _ *interceptor.Funcs) {
		for _, o := range *objs {
			if wl, ok := o.(*kueue.Workload); ok {
				wl.Spec.PodSets = []kueue.PodSet{
					{Name: "launcher", Count: 1, Template: podTemplate()},
					{Name: "worker", Count: 2, Template: podTemplate()},
				}
				wl.Status.Admission.PodSetAssignments = []kueue.PodSetAssignment{
					{Name: "launcher", Count: ptr.To[int32](1), Flavors: map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-cpu"}},
					{Name: "worker", Count: ptr.To[int32](2), Flavors: map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-gpu", "nvidia.com/gpu": "hpc-a-gpu"}},
				}
			}
		}
	})
	e.reconcile()
	if s, _, _ := e.agent.counts(); s != 1 {
		t.Fatalf("submits = %d, want 1", s)
	}
	var got []string
	for _, ps := range e.agent.submits[0].PodSets {
		got = append(got, fmt.Sprintf("%s/%d/%s", ps.Name, ps.Count, ps.Partition))
	}
	if want := []string{"launcher/1/cpu", "worker/2/gpu"}; !reflect.DeepEqual(got, want) {
		t.Errorf("podSets = %v, want %v", got, want)
	}
	if st := e.check("hpc-a"); st.State != kueue.CheckStatePending {
		t.Errorf("check = %s %q, want Pending", st.State, st.Message)
	}
}
