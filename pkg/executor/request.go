package executor

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/admissioncheck"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// Reasons of the conditions the executor writes, besides Kueue's Succeeded and
// Failed for Finished, StagingIn and StagingOut.
const (
	reasonAccepted       = "Accepted"
	reasonRefused        = "Refused"
	reasonRunning        = "Running"
	reasonSubmitted      = "Submitted"
	reasonQueued         = "Queued"
	reasonRankMissing    = "RankMissing"
	reasonWaitingForSite = "WaitingForSite"
)

// maxConcurrentRequests is how many requests the executor reconciles at once.
// A request is never reconciled twice at once.
const maxConcurrentRequests = 4

// SetupWithManager reconciles the OffloadRequests the manager's cache holds:
// those of the executor's namespace and site.
func (d *Executor) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("offloadrequest").
		For(&v1alpha1.OffloadRequest{}, builder.WithPredicates(requestChanged)).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrentRequests}).
		Complete(d)
}

// requestChanged lets through what the executor acts on: a new request, its
// deletion, and a changed spec or label. Its own status writes would otherwise
// poll the job again at once; polls come every Executor.PollInterval.
var requestChanged = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() ||
			e.ObjectOld.GetDeletionTimestamp().IsZero() != e.ObjectNew.GetDeletionTimestamp().IsZero() ||
			!maps.Equal(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels())
	},
}

// Reconcile runs one OffloadRequest of the executor's site: it takes it and
// submits its job, reports on the job on every poll, and cancels it once the
// request is deleted.
func (d *Executor) Reconcile(ctx context.Context, r ctrl.Request) (ctrl.Result, error) {
	q := &v1alpha1.OffloadRequest{}
	if err := d.Client.Get(ctx, r.NamespacedName, q); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if site, _ := d.Config.Site(); q.Namespace != d.Config.Executor.Namespace || q.Labels[v1alpha1.SiteLabel] != site {
		return ctrl.Result{}, nil
	}
	if q.DeletionTimestamp.IsZero() && q.Status.JobID == "" && !refused(q) {
		// The cache can lag behind the executor's own last status write, and a
		// second submit would start a second job.
		if err := d.Reader.Get(ctx, r.NamespacedName, q); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}
	if !q.DeletionTimestamp.IsZero() {
		return d.finalize(ctx, q)
	}
	if !controllerutil.ContainsFinalizer(q, v1alpha1.ExecutorFinalizer) {
		orig := q.DeepCopy()
		controllerutil.AddFinalizer(q, v1alpha1.ExecutorFinalizer)
		if err := d.Client.Patch(ctx, q, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
	}
	switch {
	case refused(q):
		return ctrl.Result{}, nil
	case jobEnded(q):
		d.endShadow(ctx, q)
		return ctrl.Result{}, nil
	case q.Status.JobID == "":
		return d.take(ctx, q)
	}
	return d.poll(ctx, q)
}

// take runs the request's workload once it passed every check, staging it in
// first, or records why it is refused or waits.
func (d *Executor) take(ctx context.Context, q *v1alpha1.OffloadRequest) (ctrl.Result, error) {
	orig := q.DeepCopy()
	aerr := d.validateSubmit(jobRequestOf(q))
	if aerr == nil {
		aerr = d.checkStageClaims(ctx, jobRequestOf(q))
	}
	if aerr == nil {
		aerr = d.checkWorkload(ctx, q)
	}
	if aerr == nil {
		aerr = d.checkEarlierAttempts(ctx, q)
	}
	if aerr == nil {
		setCondition(q, v1alpha1.OffloadRequestAccepted, metav1.ConditionTrue, reasonAccepted, "")
		aerr = d.submit(ctx, q)
	}
	switch {
	case aerr == nil:
	case aerr.permanent:
		log.L.Warningf("executor: request %s/%s refused: %s", q.Namespace, q.Name, aerr.msg)
		refuse(q, aerr.msg)
	default:
		q.Status.Reason, q.Status.Message = "", aerr.msg
	}
	if err := d.writeStatus(ctx, orig, q, aerr == nil || !aerr.unreachable); err != nil {
		if q.Status.JobID != "" {
			// A job the status does not name would never be cancelled.
			d.deleteMembersQuietly(ctx, string(q.Spec.Workload.UID), createdMembers(q, jobRequestOf(q)))
		}
		return ctrl.Result{}, err
	}
	if refused(q) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: d.Config.Executor.pollInterval}, nil
}

// poll reads the state of the request's job and writes it with the heartbeat.
func (d *Executor) poll(ctx context.Context, q *v1alpha1.OffloadRequest) (ctrl.Result, error) {
	if hasPendingRanks(q) {
		// As before a submit: the ranks waiting for the site are created here.
		if err := d.Reader.Get(ctx, client.ObjectKeyFromObject(q), q); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}
	orig := q.DeepCopy()
	req := jobRequestOf(q)
	pendingMessage, refusedRank := d.createPending(ctx, q)
	st, aerr := d.observe(ctx, q, req, pendingMessage, refusedRank)
	if aerr != nil {
		log.L.Warningf("executor: request %s/%s: %s", q.Namespace, q.Name, aerr.msg)
		// Without a heartbeat: the offload controller gives up after
		// executorLostTimeout.
		if err := d.writeStatus(ctx, orig, q, false); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: d.Config.Executor.pollInterval}, nil
	}
	d.syncShadow(ctx, q, req, st)
	d.stageOut(ctx, req, &st)
	applyJobState(q, st)
	if err := d.writeStatus(ctx, orig, q, true); err != nil {
		return ctrl.Result{}, err
	}
	if jobEnded(q) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: d.Config.Executor.pollInterval}, nil
}

// endShadow renders the shadow of an ended job for its end, unless that was
// done since the executor started.
func (d *Executor) endShadow(ctx context.Context, q *v1alpha1.OffloadRequest) {
	if q.Status.JobID == "" || d.shadowEnded(q.UID) {
		return
	}
	orig := q.DeepCopy()
	d.syncShadow(ctx, q, jobRequestOf(q), jobState{State: jobCompleted, NodeName: q.Status.NodeName, SocketDir: q.Status.SocketDir})
	if err := d.writeStatus(ctx, orig, q, false); err != nil {
		log.L.Warningf("executor: request %s/%s: recording the shadow: %v", q.Namespace, q.Name, err)
	}
}

// finalize cancels the job of a deleted request, copies its outputs back
// (stageOutDeleted), and then removes the executor's finalizer.
func (d *Executor) finalize(ctx context.Context, q *v1alpha1.OffloadRequest) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(q, v1alpha1.ExecutorFinalizer) {
		return ctrl.Result{}, nil
	}
	if !d.wasCancelled(q.UID) {
		if err := d.cancel(ctx, q); err != nil {
			return ctrl.Result{}, err
		}
	}
	if wait := d.stageOutDeleted(ctx, q); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	orig := q.DeepCopy()
	controllerutil.RemoveFinalizer(q, v1alpha1.ExecutorFinalizer)
	if err := d.Client.Patch(ctx, q, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	d.mu.Lock()
	delete(d.cancelled, q.UID)
	d.mu.Unlock()
	return ctrl.Result{}, nil
}

// wasCancelled reports whether this process cancelled the job of the deleted
// request with that UID.
func (d *Executor) wasCancelled(uid types.UID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancelled[uid]
}

// checkWorkload refuses a request the offload controller did not make for the
// Workload it names, as read from the API server: its controller owner is that
// Workload by name and UID, and the Workload's PodSets and admission are the
// request's. A Workload that no longer holds its quota with the request's check
// Pending makes the request wait: the offload controller deletes it.
func (d *Executor) checkWorkload(ctx context.Context, q *v1alpha1.OffloadRequest) *refusal {
	ref := q.Spec.Workload
	owner := metav1.GetControllerOf(q)
	if owner == nil || !strings.HasPrefix(owner.APIVersion, kueue.GroupVersion.Group+"/") || owner.Kind != "Workload" || owner.Name != ref.Name || owner.UID != ref.UID {
		return permanentError("OffloadRequest %s is not controlled by Workload %s (%s)", q.Name, ref.Name, ref.UID)
	}
	wl := &kueue.Workload{}
	if err := d.Reader.Get(ctx, types.NamespacedName{Namespace: q.Namespace, Name: ref.Name}, wl); err != nil {
		if apierrors.IsNotFound(err) {
			return permanentError("Workload %s does not exist", ref.Name)
		}
		return transientError("reading Workload %s: %v", ref.Name, err)
	}
	if wl.UID != ref.UID {
		return permanentError("Workload %s has UID %s, not %s", ref.Name, wl.UID, ref.UID)
	}
	if !kueueworkload.HasQuotaReservation(wl) || kueueworkload.IsEvicted(wl) || !kueueworkload.IsActive(wl) || kueueworkload.IsFinished(wl) {
		return transientError("Workload %s holds no quota reservation", ref.Name)
	}
	st := admissioncheck.FindAdmissionCheck(wl.Status.AdmissionChecks, q.Spec.AdmissionCheck)
	if st == nil {
		return permanentError("Workload %s has no admission check %s", ref.Name, q.Spec.AdmissionCheck)
	}
	if st.State != kueue.CheckStatePending {
		return transientError("admission check %s of Workload %s is %s, not %s", st.Name, ref.Name, st.State, kueue.CheckStatePending)
	}
	if why := podSetsDiffer(wl, q.Spec.PodSets); why != "" {
		return permanentError("OffloadRequest %s does not match Workload %s: %s", q.Name, ref.Name, why)
	}
	return nil
}

// podSetsDiffer says how the request's PodSets differ from the Workload's
// PodSets and their admission, or nothing when they are equal.
func podSetsDiffer(wl *kueue.Workload, podSets []v1alpha1.PodSet) string {
	if len(podSets) != len(wl.Spec.PodSets) {
		return fmt.Sprintf("%d PodSets, the Workload has %d", len(podSets), len(wl.Spec.PodSets))
	}
	assignments := map[kueue.PodSetReference]*kueue.PodSetAssignment{}
	if wl.Status.Admission != nil {
		for i := range wl.Status.Admission.PodSetAssignments {
			a := &wl.Status.Admission.PodSetAssignments[i]
			assignments[a.Name] = a
		}
	}
	for i, ps := range podSets {
		w := wl.Spec.PodSets[i]
		count := w.Count
		var flavors map[v1.ResourceName]kueue.ResourceFlavorReference
		if a := assignments[w.Name]; a != nil {
			if a.Count != nil {
				count = *a.Count
			}
			flavors = a.Flavors
		}
		switch {
		case ps.Name != w.Name:
			return fmt.Sprintf("PodSet %d is %s, the Workload's is %s", i, ps.Name, w.Name)
		case ps.Count != count:
			return fmt.Sprintf("PodSet %s: count %d, admitted %d", ps.Name, ps.Count, count)
		case !maps.Equal(ps.Flavors, flavors):
			return fmt.Sprintf("PodSet %s: flavors %v, admitted %v", ps.Name, ps.Flavors, flavors)
		case !equality.Semantic.DeepEqual(ps.Template, w.Template):
			return fmt.Sprintf("PodSet %s: the template is not the Workload's", ps.Name)
		}
	}
	return ""
}

// checkEarlierAttempts holds a request while another one of its Workload still
// has the executor's finalizer: that attempt's job, shadow and stage-in Job carry
// the same Workload UID, and go only once its cancel ended. An attempt that then
// copies its outputs back holds the next one until the copy ended, as a deleted
// pod's stage-out holds the pod that replaces it under the virtual kubelet: both
// attempts stage to the same paths of the site, and the next one's stage-in or
// job would overwrite outputs not copied back yet. StageOutTimeout bounds that.
func (d *Executor) checkEarlierAttempts(ctx context.Context, q *v1alpha1.OffloadRequest) *refusal {
	list := &v1alpha1.OffloadRequestList{}
	if err := d.Client.List(ctx, list, client.InNamespace(q.Namespace)); err != nil {
		return transientError("listing the OffloadRequests of Workload %s: %v", q.Spec.Workload.Name, err)
	}
	for i := range list.Items {
		o := &list.Items[i]
		if o.UID != q.UID && o.Spec.Workload.UID == q.Spec.Workload.UID && !o.DeletionTimestamp.IsZero() &&
			controllerutil.ContainsFinalizer(o, v1alpha1.ExecutorFinalizer) {
			if d.wasCancelled(o.UID) && d.copiesBackOnDelete(o) {
				return transientError("waiting for OffloadRequest %s to copy back the outputs of its job (Job %s)", o.Name, stageJobName(jobRequestOf(o), stageOut))
			}
			return transientError("waiting for the job of OffloadRequest %s to be cancelled", o.Name)
		}
	}
	return nil
}

// current is the request whose job the logs of a Workload come from: the newest
// submitted one the executor holds.
func (d *Executor) current(ctx context.Context, workloadUID string) *v1alpha1.OffloadRequest {
	list := &v1alpha1.OffloadRequestList{}
	if err := d.Client.List(ctx, list, client.InNamespace(d.Config.Executor.Namespace)); err != nil {
		log.L.Warning("executor: listing the OffloadRequests: ", err)
		return nil
	}
	var newest *v1alpha1.OffloadRequest
	for i := range list.Items {
		q := &list.Items[i]
		if string(q.Spec.Workload.UID) == workloadUID && q.Status.JobID != "" && controllerutil.ContainsFinalizer(q, v1alpha1.ExecutorFinalizer) &&
			(newest == nil || q.Spec.Attempt > newest.Spec.Attempt) {
			newest = q
		}
	}
	return newest
}

// applyJobState writes the job's state to q's status, with the conditions the
// offload controller reads as a remote Workload's.
func applyJobState(q *v1alpha1.OffloadRequest, st jobState) {
	s := &q.Status
	s.JobID, s.NodeName, s.Nodes, s.SocketDir = st.JobID, st.NodeName, st.Nodes, st.SocketDir
	s.StartTime, s.CompletionTime, s.Ranks = st.StartTime, st.CompletionTime, st.Ranks
	s.Reason, s.Message = st.Reason, st.Message
	switch st.State {
	case jobQueued:
		setCondition(q, v1alpha1.OffloadRequestAdmitted, metav1.ConditionFalse, reasonQueued, "")
	case jobRunning:
		setCondition(q, v1alpha1.OffloadRequestAdmitted, metav1.ConditionTrue, reasonRunning, "")
	case jobCompleted:
		setCondition(q, v1alpha1.OffloadRequestFinished, metav1.ConditionTrue, v1alpha1.OffloadRequestSucceeded, st.Message)
	case jobFailed:
		setCondition(q, v1alpha1.OffloadRequestFinished, metav1.ConditionTrue, v1alpha1.OffloadRequestFailed, st.Message)
	case jobCancelled:
		setCondition(q, v1alpha1.OffloadRequestEvicted, metav1.ConditionTrue, st.Reason, st.Message)
	}
	switch {
	case st.StagingOut:
		setCondition(q, v1alpha1.OffloadRequestStagingOut, metav1.ConditionTrue, reasonRunning, "")
	case meta.IsStatusConditionTrue(s.Conditions, v1alpha1.OffloadRequestStagingOut):
		reason := v1alpha1.OffloadRequestSucceeded
		if st.State == jobFailed {
			reason = v1alpha1.OffloadRequestFailed
		}
		setCondition(q, v1alpha1.OffloadRequestStagingOut, metav1.ConditionFalse, reason, "")
	}
}

// refuse records a permanent refusal: Accepted=False, which the offload
// controller turns into a Rejected check.
func refuse(q *v1alpha1.OffloadRequest, msg string) {
	q.Status.Reason, q.Status.Message = reasonRefused, msg
	setCondition(q, v1alpha1.OffloadRequestAccepted, metav1.ConditionFalse, reasonRefused, msg)
}

func setCondition(q *v1alpha1.OffloadRequest, ty string, status metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&q.Status.Conditions, metav1.Condition{
		Type: ty, Status: status, Reason: reason, Message: truncate(msg), ObservedGeneration: q.Generation,
	})
}

// maxMessage is the longest message the API takes.
const maxMessage = 32768

func truncate(msg string) string {
	if len(msg) > maxMessage {
		return msg[:maxMessage]
	}
	return msg
}

// writeStatus patches the status of q from orig, with a heartbeat when beat is
// set: the executor read the request and the state of its job.
func (d *Executor) writeStatus(ctx context.Context, orig, q *v1alpha1.OffloadRequest, beat bool) error {
	q.Status.Message = truncate(q.Status.Message)
	if beat {
		q.Status.LastSyncTime = &metav1.Time{Time: time.Now()}
	}
	if equality.Semantic.DeepEqual(orig.Status, q.Status) {
		return nil
	}
	return d.Client.Status().Patch(ctx, q, client.MergeFrom(orig))
}

func refused(q *v1alpha1.OffloadRequest) bool {
	c := meta.FindStatusCondition(q.Status.Conditions, v1alpha1.OffloadRequestAccepted)
	return c != nil && c.Status == metav1.ConditionFalse
}

// jobEnded reports whether the request's job finished or was lost.
func jobEnded(q *v1alpha1.OffloadRequest) bool {
	return meta.IsStatusConditionTrue(q.Status.Conditions, v1alpha1.OffloadRequestFinished) ||
		meta.IsStatusConditionTrue(q.Status.Conditions, v1alpha1.OffloadRequestEvicted)
}

// started reports whether a rank of the request's job ran: its log exists.
func started(q *v1alpha1.OffloadRequest) bool {
	return q.Status.StartTime != nil || meta.IsStatusConditionTrue(q.Status.Conditions, v1alpha1.OffloadRequestAdmitted) || jobEnded(q)
}
