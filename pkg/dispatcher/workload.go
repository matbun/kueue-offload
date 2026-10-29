package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/interlink-hq/interlink-dispatch/api/v1alpha1"
	"github.com/interlink-hq/interlink-dispatch/pkg/agentapi"
)

// Record states the dispatcher writes itself, next to the agent's states.
const (
	recordSubmitted = "submitted"
	recordLost      = "lost"
)

// Event reasons on the Workload.
const (
	EventSubmitted        = "Submitted"
	EventSubmitFailed     = "SubmitFailed"
	EventRunning          = "Running"
	EventCompleted        = "Completed"
	EventFailed           = "Failed"
	EventRetry            = "Retry"
	EventRejected         = "Rejected"
	EventAgentUnreachable = "AgentUnreachable"
)

const maxSubmitBackoff = 5 * time.Minute

// WorkloadReconciler drives the dispatcher's admission check on Workloads.
type WorkloadReconciler struct {
	// Client reads from the cache and writes to the API server.
	Client client.Client
	// APIReader reads objects the dispatcher may only get: Jobs, pods and
	// Namespaces.
	APIReader client.Reader
	Checks    CheckWriter
	Agent     *AgentClient
	Recorder  events.EventRecorder
	// Site is the SiteConfig name.
	Site string
	// Namespace holds the SubmitRecords.
	Namespace string
	Now       func() time.Time

	mu       sync.Mutex
	failures map[types.UID]int
}

func (r *WorkloadReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *WorkloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	sc := &v1alpha1.SiteConfig{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: r.Site}, sc); err != nil {
		if apierrors.IsNotFound(err) {
			log.FromContext(ctx).Info("SiteConfig not found", "site", r.Site)
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
		return ctrl.Result{}, err
	}
	s := resolveSite(sc)

	wl := &kueue.Workload{}
	if err := r.Client.Get(ctx, req.NamespacedName, wl); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.cleanupByName(ctx, req.NamespacedName)
		}
		return ctrl.Result{}, err
	}
	rec, err := r.record(ctx, wl.UID)
	if err != nil {
		return ctrl.Result{}, err
	}

	if isTrue(wl, kueue.WorkloadFinished) {
		return ctrl.Result{}, nil
	}
	if !isTrue(wl, kueue.WorkloadQuotaReserved) || isTrue(wl, kueue.WorkloadEvicted) ||
		(wl.Spec.Active != nil && !*wl.Spec.Active) {
		if rec != nil && rec.Status.JobID != "" {
			return ctrl.Result{}, r.releaseJob(ctx, wl, rec)
		}
		return ctrl.Result{}, nil
	}

	checkName, err := r.ourCheck(ctx, wl)
	if err != nil || checkName == "" {
		return ctrl.Result{}, err
	}
	check := findCheck(wl, checkName)
	if check == nil || check.State == kueue.CheckStateRejected {
		return ctrl.Result{}, nil
	}

	owner, unsupported := ownerOf(wl)
	if owner == nil {
		return ctrl.Result{}, r.reject(ctx, wl, checkName, rec, nil, unsupported)
	}

	if rec == nil || rec.Status.JobID == "" {
		if check.State != kueue.CheckStatePending {
			return ctrl.Result{}, nil
		}
		return r.submit(ctx, s, wl, checkName, owner, rec)
	}
	return r.poll(ctx, s, wl, checkName, owner, rec)
}

// ourCheck returns the name of this site's check among the checks of the
// Workload's ClusterQueue, or "" when the Workload is not ours.
func (r *WorkloadReconciler) ourCheck(ctx context.Context, wl *kueue.Workload) (kueue.AdmissionCheckReference, error) {
	if wl.Status.Admission == nil {
		return "", nil
	}
	cq := &kueue.ClusterQueue{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: string(wl.Status.Admission.ClusterQueue)}, cq); err != nil {
		return "", client.IgnoreNotFound(err)
	}
	if cq.Spec.AdmissionChecksStrategy == nil {
		return "", nil
	}
	for _, rule := range cq.Spec.AdmissionChecksStrategy.AdmissionChecks {
		if findCheck(wl, rule.Name) == nil {
			continue
		}
		ac := &kueue.AdmissionCheck{}
		if err := r.Client.Get(ctx, types.NamespacedName{Name: string(rule.Name)}, ac); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", err
		}
		if servesCheck(ac, r.Site) {
			return rule.Name, nil
		}
	}
	return "", nil
}

// servesCheck reports whether ac is an interlink-dispatch check naming site.
func servesCheck(ac *kueue.AdmissionCheck, site string) bool {
	p := ac.Spec.Parameters
	return ac.Spec.ControllerName == ControllerName && p != nil &&
		p.APIGroup == v1alpha1.GroupVersion.Group && p.Kind == "SiteConfig" && p.Name == site
}

func (r *WorkloadReconciler) submit(ctx context.Context, s site, wl *kueue.Workload, checkName kueue.AdmissionCheckReference, owner *workloadOwner, rec *v1alpha1.SubmitRecord) (ctrl.Result, error) {
	ownerRef, ownerAnnotations, err := readOwner(ctx, r.APIReader, wl.Namespace, owner)
	if err != nil || ownerRef == nil {
		return ctrl.Result{}, err
	}
	ns := &corev1.Namespace{}
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: wl.Namespace}, ns); err != nil {
		return ctrl.Result{}, err
	}
	account, err := s.account(string(wl.Status.Admission.ClusterQueue))
	if err != nil {
		return ctrl.Result{}, r.reject(ctx, wl, checkName, rec, owner, err.Error())
	}
	podSets, err := buildPodSets(wl, ownerAnnotations, s)
	if err != nil {
		return ctrl.Result{}, r.reject(ctx, wl, checkName, rec, owner, err.Error())
	}
	endpoint := s.agentEndpoint(ns)

	// The record exists before the call: a dispatcher that dies before storing
	// the job ID submits again, and the agent answers with the existing job.
	if rec == nil {
		rec = &v1alpha1.SubmitRecord{
			ObjectMeta: metav1.ObjectMeta{Name: string(wl.UID), Namespace: r.Namespace},
		}
	}
	spec := v1alpha1.SubmitRecordSpec{
		Workload:      v1alpha1.WorkloadReference{Namespace: wl.Namespace, Name: wl.Name},
		Site:          r.Site,
		Account:       account,
		AgentEndpoint: endpoint,
	}
	if rec.CreationTimestamp.IsZero() && rec.ResourceVersion == "" {
		rec.Spec = spec
		if err := r.Client.Create(ctx, rec); err != nil {
			return ctrl.Result{}, err
		}
	} else if rec.Spec != spec {
		rec.Spec = spec
		if err := r.Client.Update(ctx, rec); err != nil {
			return ctrl.Result{}, err
		}
	}

	req := &agentapi.SubmitRequest{
		APIVersion:   agentapi.APIVersion,
		WorkloadUID:  string(wl.UID),
		WorkloadName: wl.Name,
		Namespace:    wl.Namespace,
		Owner:        *ownerRef,
		Site:         r.Site,
		Account:      account,
		PodSets:      podSets,
	}
	start := time.Now()
	resp, created, err := r.Agent.Submit(ctx, endpoint, req)
	submitLatency.WithLabelValues(r.Site, submitOutcome(created, err)).Observe(time.Since(start).Seconds())
	if err != nil {
		if IsPermanent(err) {
			return ctrl.Result{}, r.reject(ctx, wl, checkName, rec, owner, agentMessage(err))
		}
		r.Recorder.Eventf(wl, nil, corev1.EventTypeWarning, EventSubmitFailed, "Submit", "submit to %s failed: %v", endpoint, err)
		return ctrl.Result{RequeueAfter: r.backoff(wl.UID)}, nil
	}
	r.resetBackoff(wl.UID)

	msg := fmt.Sprintf("submitted as Slurm job %s", resp.JobID)
	rec.Status.JobID = resp.JobID
	rec.Status.State = recordSubmitted
	rec.Status.Message = msg
	rec.Status.NodeName = ""
	rec.Status.FirstUnreachable = nil
	if err := r.Client.Status().Update(ctx, rec); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.setCheck(ctx, wl, checkName, kueue.CheckStatePending, msg); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeNormal, EventSubmitted, "Submit", "%s to %s, account %s", msg, r.Site, account)
	return ctrl.Result{RequeueAfter: s.pollInterval}, nil
}

func (r *WorkloadReconciler) poll(ctx context.Context, s site, wl *kueue.Workload, checkName kueue.AdmissionCheckReference, owner *workloadOwner, rec *v1alpha1.SubmitRecord) (ctrl.Result, error) {
	switch rec.Status.State {
	case agentapi.StateCompleted, agentapi.StateFailed:
		// The owner was patched; Kueue has not marked the Workload Finished yet.
		return ctrl.Result{}, nil
	case agentapi.StateRejected:
		return ctrl.Result{}, nil
	}

	st, err := r.Agent.GetJob(ctx, rec.Spec.AgentEndpoint, string(wl.UID))
	now := r.now()
	if err != nil && !IsNotFound(err) {
		if rec.Status.FirstUnreachable == nil {
			rec.Status.FirstUnreachable = &metav1.Time{Time: now}
			if err := r.Client.Status().Update(ctx, rec); err != nil {
				return ctrl.Result{}, err
			}
			r.Recorder.Eventf(wl, nil, corev1.EventTypeWarning, EventAgentUnreachable, "Poll", "agent %s: %v", rec.Spec.AgentEndpoint, err)
		} else if now.Sub(rec.Status.FirstUnreachable.Time) >= s.agentLostTimeout {
			msg := fmt.Sprintf("agent %s unreachable since %s: %v", rec.Spec.AgentEndpoint,
				rec.Status.FirstUnreachable.UTC().Format(time.RFC3339), err)
			return ctrl.Result{}, r.retry(ctx, s, wl, checkName, owner, rec, recordLost, msg, false)
		}
		return ctrl.Result{RequeueAfter: s.pollInterval}, nil
	}
	if IsNotFound(err) {
		st = &agentapi.JobStatus{State: agentapi.StateCancelled, Message: "the agent has no record of the job"}
	}

	prevState, prevNode := rec.Status.State, rec.Status.NodeName
	rec.Status.FirstUnreachable = nil
	rec.Status.LastPoll = &metav1.Time{Time: now}

	jobID := rec.Status.JobID
	switch st.State {
	case agentapi.StateQueued, agentapi.StateRunning:
		msg := fmt.Sprintf("Slurm job %s queued", jobID)
		if st.Reason != "" {
			msg += " (" + st.Reason + ")"
		}
		if st.State == agentapi.StateRunning {
			msg = fmt.Sprintf("Slurm job %s running on %s", jobID, st.NodeName)
			if st.StartTime != nil {
				msg += " since " + st.StartTime.UTC().Format(time.RFC3339)
			}
		}
		rec.Status.State, rec.Status.NodeName, rec.Status.Message = st.State, st.NodeName, msg
		if err := r.Client.Status().Update(ctx, rec); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setCheck(ctx, wl, checkName, kueue.CheckStatePending, msg); err != nil {
			return ctrl.Result{}, err
		}
		if st.State == agentapi.StateRunning && (prevState != agentapi.StateRunning || prevNode != st.NodeName) {
			r.Recorder.Eventf(wl, nil, corev1.EventTypeNormal, EventRunning, "Poll", "%s", msg)
		}
		return ctrl.Result{RequeueAfter: s.pollInterval}, nil

	case agentapi.StateCompleted, agentapi.StateFailed:
		succeeded := st.State == agentapi.StateCompleted
		msg := fmt.Sprintf("Slurm job %s completed", jobID)
		if !succeeded {
			msg = fmt.Sprintf("Slurm job %s failed", jobID)
			if st.Message != "" {
				msg += ": " + st.Message
			}
		}
		o := jobOutcome{succeeded: succeeded, reason: ReasonDispatch, message: msg,
			startTime: st.StartTime, completionTime: st.CompletionTime, exitCodes: exitCodes(st)}
		if err := finishOwner(ctx, r.Client, r.APIReader, wl.Namespace, owner, o, now); err != nil {
			return ctrl.Result{}, err
		}
		rec.Status.State, rec.Status.Message = st.State, msg
		if st.NodeName != "" {
			rec.Status.NodeName = st.NodeName
		}
		if err := r.Client.Status().Update(ctx, rec); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setCheck(ctx, wl, checkName, kueue.CheckStatePending, msg); err != nil {
			return ctrl.Result{}, err
		}
		reason, ty := EventCompleted, corev1.EventTypeNormal
		if !succeeded {
			reason, ty = EventFailed, corev1.EventTypeWarning
		}
		r.Recorder.Eventf(wl, nil, ty, reason, "Poll", "%s", msg)
		return ctrl.Result{}, nil

	case agentapi.StateCancelled:
		msg := fmt.Sprintf("Slurm job %s cancelled", jobID)
		if st.Message != "" {
			msg += ": " + st.Message
		}
		return ctrl.Result{}, r.retry(ctx, s, wl, checkName, owner, rec, agentapi.StateCancelled, msg, true)

	case agentapi.StateRejected:
		msg := st.Message
		if msg == "" {
			msg = fmt.Sprintf("Slurm job %s rejected", jobID)
		}
		return ctrl.Result{}, r.reject(ctx, wl, checkName, rec, owner, msg)
	}

	log.FromContext(ctx).Info("unknown job state from agent", "state", st.State)
	if err := r.Client.Status().Update(ctx, rec); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: s.pollInterval}, nil
}

// retry gives up on the current job and asks Kueue to requeue the Workload,
// or rejects it once maxRetries resubmissions were spent.
func (r *WorkloadReconciler) retry(ctx context.Context, s site, wl *kueue.Workload, checkName kueue.AdmissionCheckReference, owner *workloadOwner, rec *v1alpha1.SubmitRecord, state, msg string, deleteJob bool) error {
	if deleteJob {
		if err := r.Agent.DeleteJob(ctx, rec.Spec.AgentEndpoint, string(wl.UID)); err != nil {
			return fmt.Errorf("deleting cancelled job: %w", err)
		}
	}
	if rec.Status.Retries >= s.maxRetries {
		rec.Status.JobID = ""
		return r.reject(ctx, wl, checkName, rec, owner, fmt.Sprintf("%s; giving up after %d retries", msg, rec.Status.Retries))
	}
	rec.Status.Retries++
	rec.Status.JobID, rec.Status.NodeName = "", ""
	rec.Status.State, rec.Status.Message = state, msg
	rec.Status.FirstUnreachable = nil
	if err := r.Client.Status().Update(ctx, rec); err != nil {
		return err
	}
	if err := r.setCheck(ctx, wl, checkName, kueue.CheckStateRetry, msg); err != nil {
		return err
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeWarning, EventRetry, "Retry", "%s; retry %d of %d", msg, rec.Status.Retries, s.maxRetries)
	return nil
}

// reject writes Rejected and, when the dispatcher handles the owner, fails it.
func (r *WorkloadReconciler) reject(ctx context.Context, wl *kueue.Workload, checkName kueue.AdmissionCheckReference, rec *v1alpha1.SubmitRecord, owner *workloadOwner, msg string) error {
	if owner != nil {
		o := jobOutcome{reason: ReasonRejected, message: msg}
		if err := finishOwner(ctx, r.Client, r.APIReader, wl.Namespace, owner, o, r.now()); err != nil {
			return err
		}
	}
	if rec != nil && rec.ResourceVersion != "" {
		rec.Status.State, rec.Status.Message = agentapi.StateRejected, msg
		if err := r.Client.Status().Update(ctx, rec); err != nil {
			return err
		}
	}
	if err := r.setCheck(ctx, wl, checkName, kueue.CheckStateRejected, msg); err != nil {
		return err
	}
	r.Recorder.Eventf(wl, nil, corev1.EventTypeWarning, EventRejected, "Reject", "%s", msg)
	return nil
}

// releaseJob deletes the agent's job of a Workload that lost its reservation.
func (r *WorkloadReconciler) releaseJob(ctx context.Context, wl *kueue.Workload, rec *v1alpha1.SubmitRecord) error {
	if err := r.Agent.DeleteJob(ctx, rec.Spec.AgentEndpoint, string(wl.UID)); err != nil {
		return fmt.Errorf("deleting job of evicted workload: %w", err)
	}
	rec.Status.JobID, rec.Status.NodeName = "", ""
	rec.Status.State, rec.Status.Message = "", "job deleted: workload lost its reservation"
	rec.Status.FirstUnreachable = nil
	return r.Client.Status().Update(ctx, rec)
}

// setCheck writes the check state when the state or the message changed.
// lastTransitionTime moves only when the state changes.
func (r *WorkloadReconciler) setCheck(ctx context.Context, wl *kueue.Workload, name kueue.AdmissionCheckReference, state kueue.CheckState, msg string) error {
	if len(msg) > 32768 {
		msg = msg[:32768]
	}
	cur := findCheck(wl, name)
	st := kueue.AdmissionCheckState{Name: name, State: state, Message: msg,
		LastTransitionTime: metav1.NewTime(r.now().Truncate(time.Second))}
	if cur != nil {
		if cur.State == state && cur.Message == msg {
			return nil
		}
		if cur.State == state && !cur.LastTransitionTime.IsZero() {
			st.LastTransitionTime = cur.LastTransitionTime
		}
	}
	if err := r.Checks.WriteCheck(ctx, wl, st); err != nil {
		return err
	}
	checkWrites.WithLabelValues(r.Site, string(state)).Inc()
	if cur != nil {
		cur.State, cur.Message, cur.LastTransitionTime = st.State, st.Message, st.LastTransitionTime
	}
	return nil
}

// record returns this site's SubmitRecord for a Workload UID, or nil.
func (r *WorkloadReconciler) record(ctx context.Context, uid types.UID) (*v1alpha1.SubmitRecord, error) {
	rec := &v1alpha1.SubmitRecord{}
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: string(uid)}, rec); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if rec.Spec.Site != r.Site {
		return nil, nil
	}
	return rec, nil
}

// cleanupByName handles a deleted Workload seen by the watch.
func (r *WorkloadReconciler) cleanupByName(ctx context.Context, key types.NamespacedName) error {
	list := &v1alpha1.SubmitRecordList{}
	if err := r.Client.List(ctx, list, client.InNamespace(r.Namespace)); err != nil {
		return err
	}
	var errs []error
	for i := range list.Items {
		rec := &list.Items[i]
		if rec.Spec.Site == r.Site && rec.Spec.Workload.Namespace == key.Namespace && rec.Spec.Workload.Name == key.Name {
			errs = append(errs, r.cleanupRecord(ctx, rec))
		}
	}
	return errors.Join(errs...)
}

// cleanupRecord deletes the agent's job and the record of a Workload that is
// gone (design D7).
func (r *WorkloadReconciler) cleanupRecord(ctx context.Context, rec *v1alpha1.SubmitRecord) error {
	if err := r.Agent.DeleteJob(ctx, rec.Spec.AgentEndpoint, rec.Name); err != nil {
		return fmt.Errorf("deleting job of removed workload %s/%s: %w", rec.Spec.Workload.Namespace, rec.Spec.Workload.Name, err)
	}
	return client.IgnoreNotFound(r.Client.Delete(ctx, rec))
}

// GarbageCollect removes the records whose Workload no longer exists.
func (r *WorkloadReconciler) GarbageCollect(ctx context.Context) error {
	list := &v1alpha1.SubmitRecordList{}
	if err := r.Client.List(ctx, list, client.InNamespace(r.Namespace)); err != nil {
		return err
	}
	var errs []error
	for i := range list.Items {
		rec := &list.Items[i]
		if rec.Spec.Site != r.Site {
			continue
		}
		wl := &kueue.Workload{}
		err := r.Client.Get(ctx, types.NamespacedName{Namespace: rec.Spec.Workload.Namespace, Name: rec.Spec.Workload.Name}, wl)
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, err)
			continue
		}
		if err == nil && string(wl.UID) == rec.Name {
			continue
		}
		errs = append(errs, r.cleanupRecord(ctx, rec))
	}
	return errors.Join(errs...)
}

func (r *WorkloadReconciler) backoff(uid types.UID) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failures == nil {
		r.failures = map[types.UID]int{}
	}
	n := r.failures[uid]
	r.failures[uid] = n + 1
	d := time.Second << min(n, 16)
	return min(d, maxSubmitBackoff)
}

func (r *WorkloadReconciler) resetBackoff(uid types.UID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failures, uid)
}

func isTrue(wl *kueue.Workload, cond string) bool {
	return meta.IsStatusConditionTrue(wl.Status.Conditions, cond)
}

func findCheck(wl *kueue.Workload, name kueue.AdmissionCheckReference) *kueue.AdmissionCheckState {
	for i := range wl.Status.AdmissionChecks {
		if wl.Status.AdmissionChecks[i].Name == name {
			return &wl.Status.AdmissionChecks[i]
		}
	}
	return nil
}

func agentMessage(err error) string {
	var ae *AgentError
	if errors.As(err, &ae) && ae.Message != "" {
		return ae.Message
	}
	return err.Error()
}

func submitOutcome(created bool, err error) string {
	var ae *AgentError
	switch {
	case err == nil && created:
		return "created"
	case err == nil:
		return "existing"
	case errors.As(err, &ae) && ae.Permanent:
		return "rejected"
	default:
		return "error"
	}
}
