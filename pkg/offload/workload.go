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
	EventReactivated         = "Reactivated"
	EventRejected            = "Rejected"
	EventExecutorUnreachable = "ExecutorUnreachable"
)

// DeferredUntilAnnotation on a Workload the start estimate deferred holds when
// the offload controller sets spec.active again, in RFC 3339.
const DeferredUntilAnnotation = "offload.interlink.eu/deferred-until"

// NextAttemptAnnotation on a Workload of Kueue's pod integration whose attempt
// was given up after its admission holds when the offload controller makes the
// next one, in RFC 3339.
const NextAttemptAnnotation = "offload.interlink.eu/next-attempt-at"

// WorkloadReconciler drives the offload checks on Workloads: for each attempt it
// creates an OffloadRequest, maps the executor's status of it to the check, and
// deletes it when the attempt is given up.
type WorkloadReconciler struct {
	// Client reads from the cache and writes to the API server.
	Client client.Client
	// APIReader reads the owners, which the offload controller may only get:
	// Jobs, JobSets, TrainJobs, Ray owners and pods, and a Workload when the
	// cache may be behind.
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
	if wl == nil {
		return ctrl.Result{}, nil
	}
	own := ownedBy(wl, reqs)
	// The request of a finished job stays until the Workload goes, and with it
	// the job directory, so that its logs stay readable. A job that has not
	// ended is cancelled: KubeRay completed the RayJob, or a user deleted the
	// pods of a group.
	if kueueworkload.IsFinished(wl) {
		return ctrl.Result{}, r.cancelUnended(ctx, own)
	}
	wait, err := r.reactivate(ctx, wl)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !kueueworkload.HasQuotaReservation(wl) || kueueworkload.IsEvicted(wl) || !kueueworkload.IsActive(wl) {
		return ctrl.Result{RequeueAfter: wait}, r.deleteLive(ctx, own)
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
	if st == nil || (st.State != kueue.CheckStatePending && st.State != kueue.CheckStateReady) {
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
		return ctrl.Result{}, r.fail(ctx, s, wl, name, nil, unsupported)
	}
	// A Workload given up stays so: its pods fail on the node.
	if msg, ok := wl.Annotations[v1alpha1.FailedAnnotation]; ok {
		return ctrl.Result{}, r.fail(ctx, s, wl, name, owner, msg)
	}
	if q := newestLive(reqs); q != nil {
		return r.sync(ctx, s, delay, wl, st, owner, q)
	}
	if st.State == kueue.CheckStateReady {
		return r.nextInPlace(ctx, s, delay, wl, st, owner, reqs)
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
// OffloadConfig's maxStartDelay, which the attempt's deferrals count by. The
// check keeps its state: Pending before the admission, Ready for an attempt
// made in place.
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
		return ctrl.Result{}, r.fail(ctx, s, wl, st.Name, owner, refusal)
	}
	ownerAnnotations := forwardedAnnotations(ownerObj.GetAnnotations())
	wt, err := jobWallTime(wl, ownerAnnotations)
	if err != nil {
		return ctrl.Result{}, r.fail(ctx, s, wl, st.Name, owner, err.Error())
	}
	podSets, err := buildPodSets(wl, s, account, wt)
	if err != nil {
		return ctrl.Result{}, r.fail(ctx, s, wl, st.Name, owner, err.Error())
	}

	q := newRequest(wl, st.Name, nextAttempt(reqs, st), s.name, account, *ownerRef, ownerAnnotations, podSets)
	q.Spec.Deferrals = deferrals(reqs, delay)
	if wt != nil {
		q.Spec.MaximumExecutionTimeSeconds = ptr.To(wt.seconds)
	}
	if err := setOwner(wl, q, r.Client.Scheme()); err != nil {
		return ctrl.Result{}, err
	}
	// The name is the attempt's, so a create that a stale cache repeats finds
	// the request it made.
	if err := r.Client.Create(ctx, q); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, err
	}
	next := kueue.AdmissionCheckState{Name: st.Name, State: kueue.CheckStatePending, Message: waitingMessage(wl.Namespace)}
	if st.State == kueue.CheckStateReady {
		next = readyState(s, wl, st.Name, next.Message)
	}
	if _, err := r.writeCheck(ctx, wl, next); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeNormal, EventSubmitted, "Submit", "attempt %d as OffloadRequest %s to site %s, account %s",
		q.Spec.Attempt, q.Name, s.name, account)
	return ctrl.Result{RequeueAfter: s.pollInterval}, nil
}

// sync maps the executor's status of the live request to the check, as
// MultiKueue maps a remote Workload's conditions: Pending until the job has
// started, Ready from then on. An attempt the site's start estimate defers by
// more than delay is given up before its job is submitted.
func (r *WorkloadReconciler) sync(ctx context.Context, s site, delay time.Duration, wl *kueue.Workload, st *kueue.AdmissionCheckState, owner *workloadOwner, q *v1alpha1.OffloadRequest) (ctrl.Result, error) {
	now := r.now()
	name := st.Name
	conds := q.Status.Conditions
	job := jobOf(q)

	if c := meta.FindStatusCondition(conds, v1alpha1.OffloadRequestAccepted); c != nil && c.Status == metav1.ConditionFalse {
		msg := c.Message
		if msg == "" {
			msg = fmt.Sprintf("the executor in namespace %s refused OffloadRequest %s", wl.Namespace, q.Name)
		}
		return ctrl.Result{}, r.fail(ctx, s, wl, name, owner, msg)
	}
	if deferred(q, delay) {
		if st.State == kueue.CheckStateReady {
			return ctrl.Result{RequeueAfter: s.deferInterval}, r.deferInPlace(ctx, s, wl, name, q)
		}
		return ctrl.Result{}, r.deferAttempt(ctx, s, wl, name, q)
	}
	if c := meta.FindStatusCondition(conds, v1alpha1.OffloadRequestFinished); c != nil && c.Status == metav1.ConditionTrue {
		return ctrl.Result{}, r.finish(ctx, s, wl, st, owner, q, c)
	}
	if c := meta.FindStatusCondition(conds, v1alpha1.OffloadRequestEvicted); c != nil && c.Status == metav1.ConditionTrue {
		return r.retry(ctx, s, wl, st, owner, q, withDetail(job+" cancelled", "", c.Message))
	}

	beat := q.CreationTimestamp.Time
	if q.Status.LastSyncTime != nil {
		beat = q.Status.LastSyncTime.Time
	}
	silent := now.Sub(beat)
	unanswered := fmt.Sprintf("no executor answering in namespace %s since %s", wl.Namespace, beat.UTC().Format(time.RFC3339))
	if silent >= s.executorLostTimeout {
		return r.retry(ctx, s, wl, st, owner, q, unanswered)
	}
	if c := meta.FindStatusCondition(conds, v1alpha1.OffloadRequestQuotaReserved); s.maxQueueTime > 0 && c != nil && c.Status == metav1.ConditionTrue &&
		!meta.IsStatusConditionTrue(conds, v1alpha1.OffloadRequestAdmitted) && now.Sub(c.LastTransitionTime.Time) >= s.maxQueueTime {
		return r.retry(ctx, s, wl, st, owner, q, fmt.Sprintf("%s queued for longer than %s", job, s.maxQueueTime))
	}

	msg, event, eventType := progress(q, wl.Namespace), "", corev1.EventTypeNormal
	if meta.IsStatusConditionTrue(conds, v1alpha1.OffloadRequestAdmitted) && !meta.IsStatusConditionTrue(conds, v1alpha1.OffloadRequestStagingOut) {
		event = EventRunning
	}
	if silent >= 2*s.pollInterval {
		msg, event, eventType = unanswered, EventExecutorUnreachable, corev1.EventTypeWarning
	}
	next := kueue.AdmissionCheckState{Name: name, State: kueue.CheckStatePending, Message: msg}
	if st.State == kueue.CheckStateReady || started(q) {
		next = readyState(s, wl, name, msg)
	}
	wrote, err := r.writeCheck(ctx, wl, next)
	if err != nil {
		return ctrl.Result{}, err
	}
	if wrote && event != "" {
		r.Recorder.Eventf(wl, nil, eventType, event, "Sync", "%s", msg)
	}
	return ctrl.Result{RequeueAfter: s.pollInterval}, nil
}

// finish reports how the attempt's job ended on the check, Ready: Kueue admits
// the Workload if it has not yet, and the virtual node ends its pods as their
// members ended. A Ray owner's Workload is deactivated, as a user stops one:
// KubeRay runs its cluster on the job's allocation, which is gone, and would
// make a failed head again. One never admitted keeps its check Pending.
func (r *WorkloadReconciler) finish(ctx context.Context, s site, wl *kueue.Workload, st *kueue.AdmissionCheckState, owner *workloadOwner, q *v1alpha1.OffloadRequest, c *metav1.Condition) error {
	succeeded := c.Reason == v1alpha1.OffloadRequestSucceeded
	msg := jobOf(q) + " completed"
	if !succeeded {
		detail := c.Message
		if detail == "" {
			detail = q.Status.Message
		}
		msg = withDetail(jobOf(q)+" failed", "", detail)
	}
	next := readyState(s, wl, st.Name, msg)
	if owner.ray() && st.State == kueue.CheckStatePending {
		next = kueue.AdmissionCheckState{Name: st.Name, State: kueue.CheckStatePending, Message: msg}
	}
	wrote, err := r.writeCheck(ctx, wl, next)
	if err != nil {
		return err
	}
	if owner.ray() {
		if err := deactivateWorkload(ctx, r.Client, r.APIReader, owner.workload, owner.uid); err != nil {
			return err
		}
	}
	if !wrote {
		return nil
	}
	reason, ty := EventCompleted, corev1.EventTypeNormal
	if !succeeded {
		reason, ty = EventFailed, corev1.EventTypeWarning
	}
	r.Recorder.Eventf(wl, nil, ty, reason, "Sync", "%s", msg)
	return nil
}

// retry gives up on the attempt. Before the admission, and for an owner with a
// controller after it, the check goes to Retry (requeue): Kueue evicts the
// Workload, which suspends the owner and deletes its pods, and queues it
// again. A Workload of Kueue's pod integration that Kueue admitted retries in
// place instead (retryInPlace): its eviction would delete its pods for good.
// Once backoffLimitCount retries were spent, the request's deferrals not
// counted, the Workload fails (fail), as Kueue's provisioning check rejects
// it. Either way the request is deleted after the check is written, and the
// executor's finalizer cancels the job; a request still there when the check
// is Retry is deleted at the eviction.
func (r *WorkloadReconciler) retry(ctx context.Context, s site, wl *kueue.Workload, st *kueue.AdmissionCheckState, owner *workloadOwner, q *v1alpha1.OffloadRequest, msg string) (ctrl.Result, error) {
	lost := q.Spec.Attempt - 1 - q.Spec.Deferrals
	if lost >= s.backoffLimitCount {
		if err := r.fail(ctx, s, wl, st.Name, owner, fmt.Sprintf("%s; giving up after %d retries", msg, lost)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.deleteRequest(ctx, q)
	}
	if st.State == kueue.CheckStateReady && owner.ref == nil {
		return r.retryInPlace(ctx, s, wl, st.Name, q, msg, lost)
	}
	after, err := r.requeue(ctx, s, wl, st.Name, q, msg)
	if err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeWarning, EventRetry, "Retry", "%s; retry %d of %d in %ds",
		msg, lost+1, s.backoffLimitCount, after)
	return ctrl.Result{}, r.deleteRequest(ctx, q)
}

// retryInPlace gives up an attempt of an admitted Workload of Kueue's pod
// integration, whose pods Kueue would delete when evicting it: a plain pod, a
// pod group, a notebook's or a workflow step's pod. The check stays Ready and
// the quota held, the request is deleted, which cancels the job, and
// NextAttemptAnnotation holds when nextInPlace makes the next attempt: after
// the backoff Kueue would wait for the attempt. The pods show Pending
// meanwhile.
func (r *WorkloadReconciler) retryInPlace(ctx context.Context, s site, wl *kueue.Workload, name kueue.AdmissionCheckReference, q *v1alpha1.OffloadRequest, msg string, lost int32) (ctrl.Result, error) {
	backoff := kueue.AdmissionCheckState{Name: name, State: kueue.CheckStateRetry, RetryCount: ptr.To(q.Spec.Attempt - 1)}
	kueueworkload.UpdateAdmissionCheckRequeueState(&backoff, s.backoffBaseSeconds, s.backoffMaxSeconds, clock.RealClock{})
	after := time.Duration(*backoff.RequeueAfterSeconds) * time.Second
	at := r.now().Add(after).UTC().Truncate(time.Second)
	if err := r.waitInPlace(ctx, s, wl, name, at, msg+"; next attempt at "+at.Format(time.RFC3339)); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeWarning, EventRetry, "Retry", "%s; retry %d of %d in %ds",
		msg, lost+1, s.backoffLimitCount, *backoff.RequeueAfterSeconds)
	return ctrl.Result{RequeueAfter: after}, r.deleteRequest(ctx, q)
}

// deferInPlace gives up an attempt made in place whose job the site would not
// start within maxStartDelay, before it is submitted, as deferAttempt does
// before the admission. Deactivating the Workload would delete its pods, so
// the quota stays held and nextInPlace makes the next attempt after
// deferInterval, which counts this one in its deferrals.
func (r *WorkloadReconciler) deferInPlace(ctx context.Context, s site, wl *kueue.Workload, name kueue.AdmissionCheckReference, q *v1alpha1.OffloadRequest) error {
	at := r.now().Add(s.deferInterval).UTC().Truncate(time.Second)
	msg := deferMessage(q.Status.Estimate, "next attempt at "+at.Format(time.RFC3339)+", quota kept")
	if err := r.waitInPlace(ctx, s, wl, name, at, msg); err != nil {
		return err
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeNormal, EventDeferred, "Defer", "%s; next attempt in %ds", msg, int64(s.deferInterval/time.Second))
	return r.deleteRequest(ctx, q)
}

// waitInPlace sets NextAttemptAnnotation to at, then the check message, Ready.
func (r *WorkloadReconciler) waitInPlace(ctx context.Context, s site, wl *kueue.Workload, name kueue.AdmissionCheckReference, at time.Time, msg string) error {
	annotated := wl.DeepCopy()
	metav1.SetMetaDataAnnotation(&annotated.ObjectMeta, NextAttemptAnnotation, at.Format(time.RFC3339))
	if err := r.Client.Patch(ctx, annotated, client.MergeFrom(wl)); err != nil {
		return client.IgnoreNotFound(err)
	}
	_, err := r.writeCheck(ctx, wl, readyState(s, wl, name, msg))
	return err
}

// nextInPlace makes the next attempt of a Workload of Kueue's pod integration
// whose check is Ready and has no live request, once the time in its
// NextAttemptAnnotation has come, and drops the annotation. The Workload is
// read from the API server: the cache can show the request deleted before the
// annotation written ahead of it. An owner with a controller retries through
// Kueue, which evicts the Workload.
func (r *WorkloadReconciler) nextInPlace(ctx context.Context, s site, delay time.Duration, wl *kueue.Workload, st *kueue.AdmissionCheckState, owner *workloadOwner, reqs []v1alpha1.OffloadRequest) (ctrl.Result, error) {
	if owner.ref != nil {
		return ctrl.Result{}, nil
	}
	fresh := &kueue.Workload{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(wl), fresh); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	v, waited := fresh.Annotations[NextAttemptAnnotation]
	// A value that does not parse lets the next attempt go at once.
	at, _ := time.Parse(time.RFC3339, v)
	if wait := at.Sub(r.now()); waited && wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	res, err := r.submit(ctx, s, delay, wl, st, owner, reqs)
	if err != nil || !waited {
		return res, err
	}
	done := fresh.DeepCopy()
	delete(done.Annotations, NextAttemptAnnotation)
	return res, client.IgnoreNotFound(r.Client.Patch(ctx, done, client.MergeFrom(fresh)))
}

// deferAttempt gives up on an attempt whose job the site would not start
// within maxStartDelay, before it is submitted, so that a job the site would
// keep waiting does not hold the quota meanwhile. It deactivates the Workload
// for the site's deferInterval, as a user stops one: Kueue evicts
// it at once, which releases the quota, and queues it again once reactivate
// sets spec.active back. A Retry with requeueAfterSeconds would keep the
// quota: Kueue evicts the Workload only when requeueAt has passed. The check
// stays Pending with the reason, which the eviction keeps. It never rejects:
// the next attempt counts this one in its deferrals, not as a retry.
func (r *WorkloadReconciler) deferAttempt(ctx context.Context, s site, wl *kueue.Workload, name kueue.AdmissionCheckReference, q *v1alpha1.OffloadRequest) error {
	after := int64(s.deferInterval / time.Second)
	until := r.now().Add(s.deferInterval).UTC().Truncate(time.Second)
	msg := deferMessage(q.Status.Estimate, "Workload deactivated until "+until.Format(time.RFC3339)+", quota released")
	if err := r.setCheck(ctx, wl, name, kueue.CheckStatePending, msg); err != nil {
		return err
	}
	inactive := wl.DeepCopy()
	inactive.Spec.Active = ptr.To(false)
	metav1.SetMetaDataAnnotation(&inactive.ObjectMeta, DeferredUntilAnnotation, until.Format(time.RFC3339))
	if err := r.Client.Patch(ctx, inactive, client.MergeFrom(wl)); err != nil {
		return client.IgnoreNotFound(err)
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeNormal, EventDeferred, "Deactivate", "%s; requeued in %ds", msg, after)
	return r.deleteRequest(ctx, q)
}

// reactivate sets spec.active again on a Workload deferAttempt deactivated,
// once the time in its DeferredUntilAnnotation has come, and drops the
// annotation; Kueue then queues the Workload again. It returns how long the
// Workload still waits. A Workload reactivated before then, by a user, only
// loses the annotation.
func (r *WorkloadReconciler) reactivate(ctx context.Context, wl *kueue.Workload) (time.Duration, error) {
	v, ok := wl.Annotations[DeferredUntilAnnotation]
	if !ok {
		return 0, nil
	}
	// A value that does not parse reactivates at once.
	until, _ := time.Parse(time.RFC3339, v)
	inactive := !kueueworkload.IsActive(wl)
	if wait := until.Sub(r.now()); inactive && wait > 0 {
		return wait, nil
	}
	active := wl.DeepCopy()
	active.Spec.Active = ptr.To(true)
	delete(active.Annotations, DeferredUntilAnnotation)
	if err := r.Client.Patch(ctx, active, client.MergeFrom(wl)); err != nil {
		return 0, client.IgnoreNotFound(err)
	}
	if inactive {
		r.Recorder.Eventf(wl, nil, corev1.EventTypeNormal, EventReactivated, "Reactivate", "deferral over: Kueue queues the Workload again")
	}
	return 0, nil
}

// requeue sets the check to Retry with requeueAfterSeconds from Kueue's backoff,
// counted by the attempt, and returns that delay: Kueue keeps the reservation
// until the delay has passed, then evicts the Workload, which releases its
// quota, and queues it again.
func (r *WorkloadReconciler) requeue(ctx context.Context, s site, wl *kueue.Workload, name kueue.AdmissionCheckReference, q *v1alpha1.OffloadRequest, msg string) (int32, error) {
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

// fail gives the Workload up for good with msg, as a kubelet that refuses its
// pods: the Workload is annotated with v1alpha1.FailedAnnotation and the check
// set Ready, so that Kueue admits it, the owner makes or ungates its pods and
// the virtual node fails them with msg. The owner then fails by its own rules:
// a Job by its backoffLimit, a pod group, a TrainJob through its JobSet. An
// unsupported owner, which no node pod would show, and a Ray owner, whose head
// KubeRay would make again after every failure, are Rejected instead, and
// Kueue deactivates the Workload.
func (r *WorkloadReconciler) fail(ctx context.Context, s site, wl *kueue.Workload, name kueue.AdmissionCheckReference, owner *workloadOwner, msg string) error {
	if len(msg) > maxMessage {
		msg = msg[:maxMessage]
	}
	if owner == nil || owner.ray() {
		wrote, err := r.writeCheck(ctx, wl, kueue.AdmissionCheckState{Name: name, State: kueue.CheckStateRejected, Message: msg})
		if err == nil && wrote {
			r.Recorder.Eventf(wl, nil, corev1.EventTypeWarning, EventRejected, "Reject", "%s", msg)
		}
		return err
	}
	annotated := wl.Annotations[v1alpha1.FailedAnnotation] != msg
	if annotated {
		failed := wl.DeepCopy()
		metav1.SetMetaDataAnnotation(&failed.ObjectMeta, v1alpha1.FailedAnnotation, msg)
		if err := r.Client.Patch(ctx, failed, client.MergeFrom(wl)); err != nil {
			return client.IgnoreNotFound(err)
		}
	}
	wrote, err := r.writeCheck(ctx, wl, readyState(s, wl, name, msg))
	if err == nil && (annotated || wrote) {
		r.Recorder.Eventf(wl, nil, corev1.EventTypeWarning, EventRejected, "Reject", "%s", msg)
	}
	return err
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

// maxMessage is the longest check message and annotation value written.
const maxMessage = 32768

// writeCheck writes the state, message, requeueAfterSeconds and, when Ready,
// podSetUpdates of st when the state or the message changed, and reports
// whether it wrote. lastTransitionTime moves only when the state changes.
// retryCount is Kueue's and is never written.
func (r *WorkloadReconciler) writeCheck(ctx context.Context, wl *kueue.Workload, st kueue.AdmissionCheckState) (bool, error) {
	if len(st.Message) > maxMessage {
		st.Message = st.Message[:maxMessage]
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
		cur.RequeueAfterSeconds, cur.PodSetUpdates = st.RequeueAfterSeconds, st.PodSetUpdates
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

// deferMessage is the check message of an attempt the start estimate
// deferred, with held saying until when and what happens to the quota.
func deferMessage(e *v1alpha1.StartEstimate, held string) string {
	if e.StartTime == nil {
		return estimateMessage(e) + "; " + held
	}
	return fmt.Sprintf("%s, in %d s: %s", estimateMessage(e), int64(e.StartTime.Sub(e.ProbeTime.Time).Seconds()), held)
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
