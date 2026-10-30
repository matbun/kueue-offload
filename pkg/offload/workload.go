package offload

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/admissioncheck"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// Event reasons on the Workload.
const (
	EventSubmitted           = "Submitted"
	EventRunning             = "Running"
	EventCompleted           = "Completed"
	EventFailed              = "Failed"
	EventRetry               = "Retry"
	EventDeferred            = "Deferred"
	EventRejected            = "Rejected"
	EventExecutorUnreachable = "ExecutorUnreachable"
)

// WorkloadReconciler drives the offload checks on Workloads: for each attempt it
// creates an OffloadRequest, maps the executor's status of it to the check and
// the owner, and deletes it when the attempt is given up.
type WorkloadReconciler struct {
	// Client reads from the cache and writes to the API server.
	Client client.Client
	// APIReader reads the owners, which the offload controller may only get:
	// Jobs, JobSets, TrainJobs and pods.
	APIReader client.Reader
	Checks    CheckWriter
	// Configs resolves each check's OffloadConfig.
	Configs  *configHelper
	Recorder events.EventRecorder
	Now      func() time.Time
}

func (r *WorkloadReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *WorkloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	wl := &kueue.Workload{}
	if err := r.Client.Get(ctx, req.NamespacedName, wl); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		wl = nil
	}
	reqs, err := r.requests(ctx, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.releaseAttempts(ctx, wl, reqs); err != nil {
		return ctrl.Result{}, err
	}
	// The request of a finished job stays until the Workload goes, and with it
	// the job directory and the shadow, so that its logs stay readable.
	if wl == nil || kueueworkload.IsFinished(wl) {
		return ctrl.Result{}, nil
	}
	own := ownedBy(wl, reqs)
	if !kueueworkload.HasQuotaReservation(wl) || kueueworkload.IsEvicted(wl) || !kueueworkload.IsActive(wl) {
		if newestLive(own) != nil {
			// The job goes with its request: a pod shown running goes back to Pending.
			owner, _ := ownerOf(wl)
			if err := syncSinglePod(ctx, r.Client, r.APIReader, wl.Namespace, owner, nil, r.now()); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, r.deleteLive(ctx, own)
	}

	checks, err := admissioncheck.FilterForController(ctx, r.Client, wl.Status.AdmissionChecks, ControllerName)
	if err != nil {
		return ctrl.Result{}, err
	}
	var (
		res  ctrl.Result
		errs []error
	)
	for _, name := range checks {
		cr, err := r.reconcileCheck(ctx, wl, name, ofCheck(own, name))
		errs = append(errs, err)
		if cr.RequeueAfter > 0 && (res.RequeueAfter == 0 || cr.RequeueAfter < res.RequeueAfter) {
			res.RequeueAfter = cr.RequeueAfter
		}
	}
	return res, errors.Join(errs...)
}

// reconcileCheck runs one check of the Workload with its own OffloadConfig.
// reqs are the check's requests of this Workload.
func (r *WorkloadReconciler) reconcileCheck(ctx context.Context, wl *kueue.Workload, name kueue.AdmissionCheckReference, reqs []v1alpha1.OffloadRequest) (ctrl.Result, error) {
	st := admissioncheck.FindAdmissionCheck(wl.Status.AdmissionChecks, name)
	if st == nil || st.State != kueue.CheckStatePending {
		return ctrl.Result{}, nil
	}
	sc, err := r.Configs.ConfigForAdmissionCheck(ctx, name)
	if err != nil {
		// The check is inactive without its OffloadConfig, so Kueue reserves no
		// new quota through it; a Workload that holds some waits for it.
		if apierrors.IsNotFound(err) || errors.Is(err, admissioncheck.ErrBadParametersRef) || errors.Is(err, admissioncheck.ErrNilParametersRef) {
			log.FromContext(ctx).Info("no OffloadConfig for the check", "check", name, "reason", err.Error())
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
		return ctrl.Result{}, err
	}
	s := resolveSite(sc)
	delay := maxStartDelay(sc)

	owner, unsupported := ownerOf(wl)
	if owner == nil {
		return ctrl.Result{}, r.reject(ctx, wl, name, nil, unsupported)
	}
	if q := newestLive(reqs); q != nil {
		return r.sync(ctx, s, delay, wl, name, owner, q)
	}
	return r.submit(ctx, s, delay, wl, st, owner, reqs)
}

// maxStartDelay is the OffloadConfig's maxStartDelay, 0 without a limit.
func maxStartDelay(sc *v1alpha1.OffloadConfig) time.Duration {
	if d := sc.Spec.MaxStartDelay; d != nil && d.Duration > 0 {
		return d.Duration
	}
	return 0
}

// submit creates the OffloadRequest of the check's next attempt. delay is the
// OffloadConfig's maxStartDelay, which the attempt's deferrals count by.
func (r *WorkloadReconciler) submit(ctx context.Context, s site, delay time.Duration, wl *kueue.Workload, st *kueue.AdmissionCheckState, owner *workloadOwner, reqs []v1alpha1.OffloadRequest) (ctrl.Result, error) {
	ownerRef, ownerObj, err := readOwner(ctx, r.APIReader, wl.Namespace, owner)
	if err != nil || ownerRef == nil {
		return ctrl.Result{}, err
	}
	account, refusal, err := r.account(ctx, wl)
	if err != nil {
		return ctrl.Result{}, err
	}
	if refusal != "" {
		return ctrl.Result{}, r.reject(ctx, wl, st.Name, owner, refusal)
	}
	ownerAnnotations := forwardedAnnotations(ownerObj.GetAnnotations())
	wt, err := jobWallTime(wl, ownerAnnotations)
	if err != nil {
		return ctrl.Result{}, r.reject(ctx, wl, st.Name, owner, err.Error())
	}
	podSets, err := buildPodSets(wl, s, account, wt)
	if err == nil {
		err = ownerPodSets(ownerObj, podSets)
	}
	if err != nil {
		return ctrl.Result{}, r.reject(ctx, wl, st.Name, owner, err.Error())
	}

	q := newRequest(wl, st.Name, nextAttempt(reqs, st), s.name, account, *ownerRef, ownerAnnotations, podSets)
	q.Spec.Deferrals = deferrals(reqs, delay)
	if wt != nil {
		q.Spec.MaximumExecutionTimeSeconds = ptr.To(wt.seconds)
	}
	if owner.singlePod() != nil {
		q.Spec.OwnerLabels = ownerObj.GetLabels()
	}
	if err := setOwner(wl, q, r.Client.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	// The name is the attempt's, so a create that a stale cache repeats finds
	// the request it made.
	if err := r.Client.Create(ctx, q); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, err
	}
	if err := r.setCheck(ctx, wl, st.Name, kueue.CheckStatePending, waitingMessage(wl.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeNormal, EventSubmitted, "Submit", "attempt %d as OffloadRequest %s to site %s, account %s",
		q.Spec.Attempt, q.Name, s.name, account)
	return ctrl.Result{RequeueAfter: s.pollInterval}, nil
}

// sync maps the executor's status of the live request to the check and the
// owner, as MultiKueue maps a remote Workload's conditions. An attempt the
// site's start estimate defers by more than delay is given up before its job is
// submitted.
func (r *WorkloadReconciler) sync(ctx context.Context, s site, delay time.Duration, wl *kueue.Workload, name kueue.AdmissionCheckReference, owner *workloadOwner, q *v1alpha1.OffloadRequest) (ctrl.Result, error) {
	now := r.now()
	conds := q.Status.Conditions
	job := jobOf(q)

	if c := meta.FindStatusCondition(conds, v1alpha1.OffloadRequestAccepted); c != nil && c.Status == metav1.ConditionFalse {
		msg := c.Message
		if msg == "" {
			msg = fmt.Sprintf("the executor in namespace %s refused OffloadRequest %s", wl.Namespace, q.Name)
		}
		return ctrl.Result{}, r.reject(ctx, wl, name, owner, msg)
	}
	if deferred(q, delay) {
		return ctrl.Result{}, r.deferAttempt(ctx, s, wl, name, owner, q)
	}
	if c := meta.FindStatusCondition(conds, v1alpha1.OffloadRequestFinished); c != nil && c.Status == metav1.ConditionTrue {
		return ctrl.Result{}, r.finish(ctx, wl, name, owner, q, c, now)
	}
	if c := meta.FindStatusCondition(conds, v1alpha1.OffloadRequestEvicted); c != nil && c.Status == metav1.ConditionTrue {
		return ctrl.Result{}, r.retry(ctx, s, wl, name, owner, q, withDetail(job+" cancelled", "", c.Message))
	}

	beat := q.CreationTimestamp.Time
	if q.Status.LastSyncTime != nil {
		beat = q.Status.LastSyncTime.Time
	}
	silent := now.Sub(beat)
	unanswered := fmt.Sprintf("no executor answering in namespace %s since %s", wl.Namespace, beat.UTC().Format(time.RFC3339))
	if silent >= s.executorLostTimeout {
		return ctrl.Result{}, r.retry(ctx, s, wl, name, owner, q, unanswered)
	}
	if c := meta.FindStatusCondition(conds, v1alpha1.OffloadRequestQuotaReserved); s.maxQueueTime > 0 && c != nil && c.Status == metav1.ConditionTrue &&
		!meta.IsStatusConditionTrue(conds, v1alpha1.OffloadRequestAdmitted) && now.Sub(c.LastTransitionTime.Time) >= s.maxQueueTime {
		return ctrl.Result{}, r.retry(ctx, s, wl, name, owner, q, fmt.Sprintf("%s queued for longer than %s", job, s.maxQueueTime))
	}
	if err := syncSinglePod(ctx, r.Client, r.APIReader, wl.Namespace, owner, q, now); err != nil {
		return ctrl.Result{}, err
	}

	msg, event, eventType := progress(q, wl.Namespace), "", corev1.EventTypeNormal
	if meta.IsStatusConditionTrue(conds, v1alpha1.OffloadRequestAdmitted) && !meta.IsStatusConditionTrue(conds, v1alpha1.OffloadRequestStagingOut) {
		event = EventRunning
	}
	if silent >= 2*s.pollInterval {
		msg, event, eventType = unanswered, EventExecutorUnreachable, corev1.EventTypeWarning
	}
	wrote, err := r.writeCheck(ctx, wl, kueue.AdmissionCheckState{Name: name, State: kueue.CheckStatePending, Message: msg})
	if err != nil {
		return ctrl.Result{}, err
	}
	if wrote && event != "" {
		r.Recorder.Eventf(wl, nil, eventType, event, "Sync", "%s", msg)
	}
	return ctrl.Result{RequeueAfter: s.pollInterval}, nil
}

// finish writes the owner's terminal state. The check stays Pending: Kueue
// finishes the Workload from the owner.
func (r *WorkloadReconciler) finish(ctx context.Context, wl *kueue.Workload, name kueue.AdmissionCheckReference, owner *workloadOwner, q *v1alpha1.OffloadRequest, c *metav1.Condition, now time.Time) error {
	succeeded := c.Reason == v1alpha1.OffloadRequestSucceeded
	msg := jobOf(q) + " completed"
	if !succeeded {
		detail := c.Message
		if detail == "" {
			detail = q.Status.Message
		}
		msg = withDetail(jobOf(q)+" failed", "", detail)
	}
	o := jobOutcome{succeeded: succeeded, reason: ReasonOffload, message: msg,
		startTime: q.Status.StartTime, completionTime: q.Status.CompletionTime, exitCodes: exitCodes(&q.Status)}
	if err := finishOwner(ctx, r.Client, r.APIReader, wl.Namespace, owner, o, now); err != nil {
		return err
	}
	wrote, err := r.writeCheck(ctx, wl, kueue.AdmissionCheckState{Name: name, State: kueue.CheckStatePending, Message: msg})
	if err != nil || !wrote {
		return err
	}
	reason, ty := EventCompleted, corev1.EventTypeNormal
	if !succeeded {
		reason, ty = EventFailed, corev1.EventTypeWarning
	}
	r.Recorder.Eventf(wl, nil, ty, reason, "Sync", "%s", msg)
	return nil
}

// retry gives up on the attempt and sets the check to Retry (requeue). Once
// backoffLimitCount retries were spent, the request's deferrals not counted,
// the check is Rejected instead, as Kueue's provisioning check does. Either way
// the request is deleted after the check is written, and the executor's
// finalizer cancels the job; a request still there when the check is Retry is
// deleted at the eviction.
func (r *WorkloadReconciler) retry(ctx context.Context, s site, wl *kueue.Workload, name kueue.AdmissionCheckReference, owner *workloadOwner, q *v1alpha1.OffloadRequest, msg string) error {
	lost := q.Spec.Attempt - 1 - q.Spec.Deferrals
	if lost >= s.backoffLimitCount {
		if err := r.reject(ctx, wl, name, owner, fmt.Sprintf("%s; giving up after %d retries", msg, lost)); err != nil {
			return err
		}
		return r.deleteRequest(ctx, q)
	}
	after, err := r.requeue(ctx, s, wl, name, owner, q, msg)
	if err != nil {
		return err
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeWarning, EventRetry, "Retry", "%s; retry %d of %d in %ds",
		msg, lost+1, s.backoffLimitCount, after)
	return r.deleteRequest(ctx, q)
}

// deferAttempt gives up on an attempt whose job the site would not start
// within maxStartDelay, before it is submitted: the check goes to Retry
// (requeue), so that a job the site would keep waiting does not hold the quota
// meanwhile, as the virtual kubelet's Slurm admission check does. It never
// rejects: the next attempt counts this one in its deferrals, not as a retry.
func (r *WorkloadReconciler) deferAttempt(ctx context.Context, s site, wl *kueue.Workload, name kueue.AdmissionCheckReference, owner *workloadOwner, q *v1alpha1.OffloadRequest) error {
	msg := deferMessage(q.Status.Estimate)
	after, err := r.requeue(ctx, s, wl, name, owner, q, msg)
	if err != nil {
		return err
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeNormal, EventDeferred, "Retry", "%s; requeued in %ds", msg, after)
	return r.deleteRequest(ctx, q)
}

// requeue sets the check to Retry with requeueAfterSeconds from Kueue's backoff,
// counted by the attempt, and returns that delay: Kueue evicts the Workload,
// which releases its quota, and requeues it after the delay. A single owner pod
// shown running goes back to Pending.
func (r *WorkloadReconciler) requeue(ctx context.Context, s site, wl *kueue.Workload, name kueue.AdmissionCheckReference, owner *workloadOwner, q *v1alpha1.OffloadRequest, msg string) (int32, error) {
	if err := syncSinglePod(ctx, r.Client, r.APIReader, wl.Namespace, owner, nil, r.now()); err != nil {
		return 0, err
	}
	st := kueue.AdmissionCheckState{Name: name, State: kueue.CheckStateRetry, Message: msg, RetryCount: ptr.To(q.Spec.Attempt - 1)}
	kueueworkload.UpdateAdmissionCheckRequeueState(&st, s.backoffBaseSeconds, s.backoffMaxSeconds, clock.RealClock{})
	if _, err := r.writeCheck(ctx, wl, st); err != nil {
		return 0, err
	}
	return *st.RequeueAfterSeconds, nil
}

// deferred reports whether the site's start estimate gives q's attempt up
// under delay: nothing of it created yet, neither its stage-in nor its job, and
// a start further away than delay from when the executor asked, or none at all
// while a submit limit of the site holds. A delay of 0 defers nothing.
func deferred(q *v1alpha1.OffloadRequest, delay time.Duration) bool {
	e := q.Status.Estimate
	staging := meta.FindStatusCondition(q.Status.Conditions, v1alpha1.OffloadRequestStagingIn) != nil
	if delay <= 0 || e == nil || q.Status.JobID != "" || staging {
		return false
	}
	return e.StartTime == nil || e.StartTime.Sub(e.ProbeTime.Time) > delay
}

// deferrals is the deferrals of the check's next attempt: those of its newest
// request, plus that one when the start estimate deferred it.
func deferrals(reqs []v1alpha1.OffloadRequest, delay time.Duration) int32 {
	var last *v1alpha1.OffloadRequest
	for i := range reqs {
		if last == nil || reqs[i].Spec.Attempt > last.Spec.Attempt {
			last = &reqs[i]
		}
	}
	if last == nil {
		return 0
	}
	if deferred(last, delay) {
		return last.Spec.Deferrals + 1
	}
	return last.Spec.Deferrals
}

// reject writes Rejected and, when the offload controller handles the owner,
// fails it. Kueue then deactivates the Workload, which deletes its requests.
func (r *WorkloadReconciler) reject(ctx context.Context, wl *kueue.Workload, name kueue.AdmissionCheckReference, owner *workloadOwner, msg string) error {
	if owner != nil {
		o := jobOutcome{reason: ReasonRejected, message: msg}
		if err := finishOwner(ctx, r.Client, r.APIReader, wl.Namespace, owner, o, r.now()); err != nil {
			return err
		}
	}
	if err := r.setCheck(ctx, wl, name, kueue.CheckStateRejected, msg); err != nil {
		return err
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeWarning, EventRejected, "Reject", "%s", msg)
	return nil
}

// account reads the account of the Workload's ClusterQueue from its label. A
// refusal means the Workload can never run.
func (r *WorkloadReconciler) account(ctx context.Context, wl *kueue.Workload) (account, refusal string, err error) {
	if wl.Status.Admission == nil {
		return "", "the Workload has no admission", nil
	}
	cq := &kueue.ClusterQueue{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: string(wl.Status.Admission.ClusterQueue)}, cq); err != nil {
		return "", "", err
	}
	if a := cq.Labels[v1alpha1.AccountLabel]; a != "" {
		return a, "", nil
	}
	return "", fmt.Sprintf("ClusterQueue %s has no label %s naming the account to charge", cq.Name, v1alpha1.AccountLabel), nil
}

// setCheck writes the check state when the state or the message changed.
func (r *WorkloadReconciler) setCheck(ctx context.Context, wl *kueue.Workload, name kueue.AdmissionCheckReference, state kueue.CheckState, msg string) error {
	_, err := r.writeCheck(ctx, wl, kueue.AdmissionCheckState{Name: name, State: state, Message: msg})
	return err
}

// writeCheck writes the state, message and requeueAfterSeconds of st when the
// state or the message changed, and reports whether it wrote.
// lastTransitionTime moves only when the state changes. retryCount is Kueue's
// and is never written.
func (r *WorkloadReconciler) writeCheck(ctx context.Context, wl *kueue.Workload, st kueue.AdmissionCheckState) (bool, error) {
	if len(st.Message) > 32768 {
		st.Message = st.Message[:32768]
	}
	cur := admissioncheck.FindAdmissionCheck(wl.Status.AdmissionChecks, st.Name)
	st.LastTransitionTime = metav1.NewTime(r.now().Truncate(time.Second))
	if cur != nil {
		if cur.State == st.State && cur.Message == st.Message {
			return false, nil
		}
		if cur.State == st.State && !cur.LastTransitionTime.IsZero() {
			st.LastTransitionTime = cur.LastTransitionTime
		}
	}
	if err := r.Checks.WriteCheck(ctx, wl, st); err != nil {
		return false, err
	}
	checkWrites.WithLabelValues(string(st.Name), string(st.State)).Inc()
	if cur != nil {
		cur.State, cur.Message, cur.LastTransitionTime = st.State, st.Message, st.LastTransitionTime
		cur.RequeueAfterSeconds = st.RequeueAfterSeconds
	}
	return true, nil
}

// progress is the check message for a request whose job neither ended nor was
// lost.
func progress(q *v1alpha1.OffloadRequest, namespace string) string {
	st := &q.Status
	job := jobOf(q)
	switch {
	case meta.IsStatusConditionTrue(st.Conditions, v1alpha1.OffloadRequestStagingOut):
		return withDetail(job+" ended, staging out", "", st.Message)
	case meta.IsStatusConditionTrue(st.Conditions, v1alpha1.OffloadRequestAdmitted):
		msg := job + " running"
		if st.NodeName != "" {
			msg += " on " + st.NodeName
		}
		if st.StartTime != nil {
			msg += " since " + st.StartTime.UTC().Format(time.RFC3339)
		}
		return msg
	case meta.IsStatusConditionTrue(st.Conditions, v1alpha1.OffloadRequestQuotaReserved):
		return withDetail(job+" queued", st.Reason, st.Message)
	case meta.IsStatusConditionTrue(st.Conditions, v1alpha1.OffloadRequestStagingIn):
		return withDetail("staging in", "", st.Message)
	case st.Reason != "" || st.Message != "":
		return withDetail("waiting to submit", st.Reason, st.Message)
	case st.Estimate != nil && st.JobID == "":
		return estimateMessage(st.Estimate)
	case meta.IsStatusConditionTrue(st.Conditions, v1alpha1.OffloadRequestAccepted):
		return "accepted by the executor in namespace " + namespace
	}
	return waitingMessage(namespace)
}

// estimateMessage is the site's start estimate in a check message, in the
// virtual kubelet's words.
func estimateMessage(e *v1alpha1.StartEstimate) string {
	if e.StartTime == nil {
		return withDetail("no start time from Slurm", e.Reason, e.Message)
	}
	return fmt.Sprintf("Slurm expects to start %d node(s) at %s", e.Nodes, e.StartTime.UTC().Format(time.RFC3339))
}

// deferMessage is the check message of an attempt the start estimate deferred.
func deferMessage(e *v1alpha1.StartEstimate) string {
	if e.StartTime == nil {
		return estimateMessage(e) + "; quota released until then"
	}
	return fmt.Sprintf("%s, in %d s: quota released until then", estimateMessage(e), int64(e.StartTime.Sub(e.ProbeTime.Time).Seconds()))
}

func waitingMessage(namespace string) string {
	return "waiting for the executor in namespace " + namespace
}

// jobOf names the request's job in check messages.
func jobOf(q *v1alpha1.OffloadRequest) string {
	if q.Status.JobID == "" {
		return "the job"
	}
	return "job " + q.Status.JobID
}

func withDetail(msg, reason, detail string) string {
	if reason != "" {
		msg += " (" + reason + ")"
	}
	if detail != "" {
		msg += ": " + detail
	}
	return msg
}
