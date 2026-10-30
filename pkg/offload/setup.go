package offload

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/admissioncheck"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/status,verbs=patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=admissionchecks,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=admissionchecks/status,verbs=get;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=clusterqueues,verbs=get;list;watch
// +kubebuilder:rbac:groups=interlink.eu,resources=offloadconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=interlink.eu,resources=offloadworkers,verbs=get;list;watch
// +kubebuilder:rbac:groups=interlink.eu,resources=offloadworkers/status,verbs=get;patch
// +kubebuilder:rbac:groups=interlink.eu,resources=offloadrequests,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=patch
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets,verbs=get
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets/status,verbs=patch
// +kubebuilder:rbac:groups=trainer.kubeflow.org,resources=trainjobs,verbs=get
// +kubebuilder:rbac:groups=trainer.kubeflow.org,resources=trainjobs/status,verbs=patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;patch
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,namespace=kueue-offload,resources=leases,verbs=get;list;watch;create;update;patch;delete

// SetupIndexes registers the field indexes the reconcilers list by.
func SetupIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &v1alpha1.OffloadRequest{}, RequestWorkloadKey, indexRequestWorkload); err != nil {
		return fmt.Errorf("indexing OffloadRequests by Workload: %w", err)
	}
	configGVK := v1alpha1.GroupVersion.WithKind("OffloadConfig")
	if err := indexer.IndexField(ctx, &kueue.AdmissionCheck{}, AdmissionCheckUsingConfigKey, admissioncheck.IndexerByConfigFunction(ControllerName, configGVK)); err != nil {
		return fmt.Errorf("indexing AdmissionChecks by OffloadConfig: %w", err)
	}
	return nil
}

// SetupWithManager registers the Workload reconciler, which a change to a
// Workload or to one of its OffloadRequests triggers.
func (r *WorkloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("workload").
		For(&kueue.Workload{}).
		Owns(&v1alpha1.OffloadRequest{}).
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](time.Second, time.Minute),
		}).
		Complete(r)
}
