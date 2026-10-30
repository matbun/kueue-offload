package offload

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// RequestWorkloadKey indexes OffloadRequests by the name of their Workload, as
// Kueue's provisioning check indexes its ProvisioningRequests by owner.
const RequestWorkloadKey = "spec.workload.name"

func indexRequestWorkload(o client.Object) []string {
	q, ok := o.(*v1alpha1.OffloadRequest)
	if !ok {
		return nil
	}
	return []string{q.Spec.Workload.Name}
}

// requestName is <workload>-<check>-<attempt>, hashed when longer than an
// object name may be, as Kueue names ProvisioningRequests
// (pkg/controller/admissionchecks/provisioning: ProvisioningRequestName).
func requestName(workloadName string, check kueue.AdmissionCheckReference, attempt int32) string {
	const hashLength = 5
	const maxPrefix = 252 - hashLength
	full := fmt.Sprintf("%s-%s-%d", workloadName, check, attempt)
	if len(full) <= maxPrefix {
		return full
	}
	h := sha1.Sum([]byte(full))
	return full[:maxPrefix] + "-" + hex.EncodeToString(h[:])[:hashLength]
}

func newRequest(wl *kueue.Workload, check kueue.AdmissionCheckReference, attempt int32, siteName, account string,
	owner v1alpha1.OwnerReference, ownerAnnotations map[string]string, podSets []v1alpha1.PodSet) *v1alpha1.OffloadRequest {
	return &v1alpha1.OffloadRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:       requestName(wl.Name, check, attempt),
			Namespace:  wl.Namespace,
			Labels:     map[string]string{v1alpha1.SiteLabel: siteName},
			Finalizers: []string{v1alpha1.AttemptFinalizer},
		},
		Spec: v1alpha1.OffloadRequestSpec{
			Workload:                    v1alpha1.WorkloadReference{Name: wl.Name, UID: wl.UID},
			AdmissionCheck:              check,
			Attempt:                     attempt,
			Site:                        siteName,
			Account:                     account,
			Owner:                       owner,
			OwnerAnnotations:            ownerAnnotations,
			PodSets:                     podSets,
			MaximumExecutionTimeSeconds: wl.Spec.MaximumExecutionTimeSeconds,
			Priority:                    wl.Spec.Priority,
		},
	}
}

// setOwner makes the Workload the request's controller owner, without
// blockOwnerDeletion, which would need update on workloads/finalizers.
func setOwner(wl *kueue.Workload, q *v1alpha1.OffloadRequest, scheme *runtime.Scheme) error {
	return controllerutil.SetControllerReference(wl, q, scheme, controllerutil.WithBlockOwnerDeletion(false))
}

// nextAttempt is one past the check's newest request, deleted ones included,
// and past Kueue's retryCount while the check state holds one.
func nextAttempt(reqs []v1alpha1.OffloadRequest, st *kueue.AdmissionCheckState) int32 {
	last := ptr.Deref(st.RetryCount, 0)
	for i := range reqs {
		last = max(last, reqs[i].Spec.Attempt)
	}
	return last + 1
}

// requests lists the OffloadRequests of every Workload named key, whatever its
// UID.
func (r *WorkloadReconciler) requests(ctx context.Context, key types.NamespacedName) ([]v1alpha1.OffloadRequest, error) {
	list := &v1alpha1.OffloadRequestList{}
	if err := r.Client.List(ctx, list, client.InNamespace(key.Namespace), client.MatchingFields{RequestWorkloadKey: key.Name}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func ownedBy(wl *kueue.Workload, reqs []v1alpha1.OffloadRequest) []v1alpha1.OffloadRequest {
	var out []v1alpha1.OffloadRequest
	for _, q := range reqs {
		if q.Spec.Workload.UID == wl.UID {
			out = append(out, q)
		}
	}
	return out
}

func ofCheck(reqs []v1alpha1.OffloadRequest, check kueue.AdmissionCheckReference) []v1alpha1.OffloadRequest {
	var out []v1alpha1.OffloadRequest
	for _, q := range reqs {
		if q.Spec.AdmissionCheck == check {
			out = append(out, q)
		}
	}
	return out
}

// newestLive is the newest request that is not being deleted, or nil.
func newestLive(reqs []v1alpha1.OffloadRequest) *v1alpha1.OffloadRequest {
	var live *v1alpha1.OffloadRequest
	for i := range reqs {
		q := &reqs[i]
		if q.DeletionTimestamp.IsZero() && (live == nil || q.Spec.Attempt > live.Spec.Attempt) {
			live = q
		}
	}
	return live
}

// deleteLive deletes the requests of a Workload that lost its reservation; the
// executor's finalizer cancels their jobs.
func (r *WorkloadReconciler) deleteLive(ctx context.Context, reqs []v1alpha1.OffloadRequest) error {
	var errs []error
	for i := range reqs {
		if reqs[i].DeletionTimestamp.IsZero() {
			errs = append(errs, r.deleteRequest(ctx, &reqs[i]))
		}
	}
	return errors.Join(errs...)
}

func (r *WorkloadReconciler) deleteRequest(ctx context.Context, q *v1alpha1.OffloadRequest) error {
	return client.IgnoreNotFound(r.Client.Delete(ctx, q))
}

// releaseAttempts drops the attempt finalizer from the requests no next
// attempt will count from: every request of a Workload that is gone, replaced,
// finished or deactivated, and the deleted requests a newer attempt of their
// check superseded. The newest deleted request of a live Workload keeps it.
func (r *WorkloadReconciler) releaseAttempts(ctx context.Context, wl *kueue.Workload, reqs []v1alpha1.OffloadRequest) error {
	done := wl == nil || kueueworkload.IsFinished(wl) || !kueueworkload.IsActive(wl)
	newest := map[kueue.AdmissionCheckReference]int32{}
	if !done {
		for _, q := range ownedBy(wl, reqs) {
			newest[q.Spec.AdmissionCheck] = max(newest[q.Spec.AdmissionCheck], q.Spec.Attempt)
		}
	}
	var errs []error
	for i := range reqs {
		q := &reqs[i]
		if !controllerutil.ContainsFinalizer(q, v1alpha1.AttemptFinalizer) {
			continue
		}
		released := done || q.Spec.Workload.UID != wl.UID
		superseded := !q.DeletionTimestamp.IsZero() && q.Spec.Attempt < newest[q.Spec.AdmissionCheck]
		if !released && !superseded {
			continue
		}
		orig := q.DeepCopy()
		controllerutil.RemoveFinalizer(q, v1alpha1.AttemptFinalizer)
		err := r.Client.Patch(ctx, q, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
		errs = append(errs, client.IgnoreNotFound(err))
	}
	return errors.Join(errs...)
}
