package offload

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// AdmissionCheckReconciler marks this site's AdmissionChecks Active, which
// Kueue requires before it uses a ClusterQueue that lists them.
type AdmissionCheckReconciler struct {
	Client client.Client
	Site   string
}

func (r *AdmissionCheckReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ac := &kueue.AdmissionCheck{}
	if err := r.Client.Get(ctx, req.NamespacedName, ac); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !servesCheck(ac, r.Site) {
		return ctrl.Result{}, nil
	}
	cond := metav1.Condition{
		Type:               kueue.AdmissionCheckActive,
		Status:             metav1.ConditionTrue,
		Reason:             "Active",
		Message:            fmt.Sprintf("kueue-offload serves site %s", r.Site),
		ObservedGeneration: ac.Generation,
	}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: r.Site}, &v1alpha1.OffloadConfig{}); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		cond.Status = metav1.ConditionFalse
		cond.Reason = "OffloadConfigNotFound"
		cond.Message = fmt.Sprintf("OffloadConfig %s does not exist", r.Site)
	}
	orig := ac.DeepCopy()
	if !meta.SetStatusCondition(&ac.Status.Conditions, cond) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, r.Client.Status().Patch(ctx, ac, client.MergeFrom(orig))
}

// SetupWithManager registers the reconciler. A change to the OffloadConfig
// re-evaluates every AdmissionCheck.
func (r *AdmissionCheckReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("admissioncheck").
		For(&kueue.AdmissionCheck{}).
		Watches(&v1alpha1.OffloadConfig{}, handlerAllChecks(r.Client)).
		Complete(r)
}
