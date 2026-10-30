package offload

import (
	"context"
	"fmt"
	"slices"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// AdmissionCheckUsingConfigKey indexes the offload AdmissionChecks by the
// name of their OffloadConfig.
const AdmissionCheckUsingConfigKey = "spec.offloadConfig"

// AdmissionCheckReconciler keeps the Active condition of every AdmissionCheck
// with controllerName interlink.eu/offload, from the OffloadWorkers its
// OffloadConfig names, as MultiKueue's does from its MultiKueueClusters. Kueue
// uses a ClusterQueue only while all its checks are Active.
type AdmissionCheckReconciler struct {
	Client  client.Client
	Configs *configHelper
}

func (r *AdmissionCheckReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ac := &kueue.AdmissionCheck{}
	if err := r.Client.Get(ctx, req.NamespacedName, ac); err != nil || ac.Spec.ControllerName != ControllerName {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	cond := metav1.Condition{
		Type:               kueue.AdmissionCheckActive,
		Status:             metav1.ConditionTrue,
		Reason:             "Active",
		Message:            "The admission check is active",
		ObservedGeneration: ac.Generation,
	}
	if sc, err := r.Configs.ConfigFromRef(ctx, ac.Spec.Parameters); err != nil {
		cond.Status = metav1.ConditionFalse
		cond.Reason = "BadConfig"
		cond.Message = fmt.Sprintf("Cannot load the AdmissionChecks parameters: %s", err.Error())
	} else {
		var missing, inactive []string
		for _, name := range sc.Spec.Workers {
			w := &v1alpha1.OffloadWorker{}
			err := r.Client.Get(ctx, types.NamespacedName{Name: name}, w)
			if client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
			if err != nil {
				missing = append(missing, name)
			} else if !apimeta.IsStatusConditionTrue(w.Status.Conditions, v1alpha1.OffloadWorkerActive) {
				inactive = append(inactive, name)
			}
		}
		if unusable := len(missing) + len(inactive); unusable > 0 {
			if unusable < len(sc.Spec.Workers) {
				cond.Reason = "SomeActiveWorkers"
			} else {
				cond.Status = metav1.ConditionFalse
				cond.Reason = "NoUsableWorkers"
			}
			var parts []string
			if len(missing) > 0 {
				parts = append(parts, fmt.Sprintf("Missing workers: %v", missing))
			}
			if len(inactive) > 0 {
				parts = append(parts, fmt.Sprintf("Inactive workers: %v", inactive))
			}
			cond.Message = strings.Join(parts, ", ")
		}
	}
	if old := apimeta.FindStatusCondition(ac.Status.Conditions, kueue.AdmissionCheckActive); sameCondition(old, &cond) {
		return ctrl.Result{}, nil
	}
	orig := ac.DeepCopy()
	apimeta.SetStatusCondition(&ac.Status.Conditions, cond)
	return ctrl.Result{}, client.IgnoreNotFound(r.Client.Status().Patch(ctx, ac, client.MergeFrom(orig)))
}

// SetupWithManager registers the reconciler. A change to an OffloadConfig
// re-evaluates the checks that use it, a change to an OffloadWorker those whose
// OffloadConfig names it.
func (r *AdmissionCheckReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("admissioncheck").
		For(&kueue.AdmissionCheck{}).
		Watches(&v1alpha1.OffloadConfig{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.checksUsing(ctx, o.GetName())
		})).
		Watches(&v1alpha1.OffloadWorker{}, handler.EnqueueRequestsFromMapFunc(r.checksOfWorker)).
		Complete(r)
}

func (r *AdmissionCheckReconciler) checksUsing(ctx context.Context, config string) []reconcile.Request {
	list := &kueue.AdmissionCheckList{}
	if err := r.Client.List(ctx, list, client.MatchingFields{AdmissionCheckUsingConfigKey: config}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "listing the AdmissionChecks of an OffloadConfig", "offloadConfig", config)
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(list.Items))
	for _, ac := range list.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Name: ac.Name}})
	}
	return reqs
}

func (r *AdmissionCheckReconciler) checksOfWorker(ctx context.Context, w client.Object) []reconcile.Request {
	configs := &v1alpha1.OffloadConfigList{}
	if err := r.Client.List(ctx, configs); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "listing OffloadConfigs", "offloadWorker", w.GetName())
		return nil
	}
	var reqs []reconcile.Request
	for _, sc := range configs.Items {
		if slices.Contains(sc.Spec.Workers, w.GetName()) {
			reqs = append(reqs, r.checksUsing(ctx, sc.Name)...)
		}
	}
	return reqs
}

// sameCondition compares what a reader of the condition sees, as MultiKueue's
// cmpConditionState.
func sameCondition(a, b *metav1.Condition) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Status == b.Status && a.Reason == b.Reason && a.Message == b.Message && a.ObservedGeneration == b.ObservedGeneration
}
