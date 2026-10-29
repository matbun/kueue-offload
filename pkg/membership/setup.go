package membership

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/interlink-hq/interlink-dispatch/api/v1alpha1"
)

// +kubebuilder:rbac:groups=interlink.eu,resources=siteconfigs;identitymaps;projectquotas,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups="",namespace=interlink-membership,resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=clusterqueues;cohorts,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=localqueues,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;services;serviceaccounts;persistentvolumeclaims,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=create;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=create;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings;clusterrolebindings,verbs=create;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=bind,resourceNames={interlink-agent,"system:auth-delegator"}
// +kubebuilder:rbac:groups="",resources=pods,verbs=list
// +kubebuilder:rbac:groups="",resources=events,verbs=list;create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,namespace=interlink-membership,resources=leases,verbs=get;list;watch;create;update;patch;delete

// SetupWithManager registers the reconciler. Every watched change enqueues
// the one site the controller serves; the reconcile requeues itself after
// syncInterval.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	site := func(context.Context, client.Object) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: r.Site}}}
	}
	ownSite := predicate.NewPredicateFuncs(func(o client.Object) bool { return o.GetName() == r.Site })
	quotaOfSite := predicate.NewPredicateFuncs(func(o client.Object) bool {
		pq, ok := o.(*v1alpha1.ProjectQuota)
		return ok && pq.Spec.Site == r.Site
	})
	ownNamespace := predicate.NewPredicateFuncs(func(o client.Object) bool { return o.GetNamespace() == r.Namespace })
	agentOfSite := predicate.NewPredicateFuncs(func(o client.Object) bool {
		l := o.GetLabels()
		return l[ManagedByLabel] == ManagedByValue && l[SiteLabel] == r.Site
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("membership").
		For(&v1alpha1.SiteConfig{}, builder.WithPredicates(ownSite)).
		Watches(&v1alpha1.IdentityMap{}, handler.EnqueueRequestsFromMapFunc(site)).
		Watches(&v1alpha1.ProjectQuota{}, handler.EnqueueRequestsFromMapFunc(site), builder.WithPredicates(quotaOfSite)).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(site), builder.WithPredicates(ownNamespace)).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(site), builder.WithPredicates(agentOfSite)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
