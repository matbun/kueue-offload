package offload

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/admissioncheck"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/status,verbs=patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=admissionchecks,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=admissionchecks/status,verbs=get;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=clusterqueues,verbs=get;list;watch
// +kubebuilder:rbac:groups=interlink.eu,resources=offloadconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=interlink.eu,resources=offloadworkers,verbs=get;list;watch
// +kubebuilder:rbac:groups=interlink.eu,resources=offloadworkers/status,verbs=get;patch
// +kubebuilder:rbac:groups=interlink.eu,resources=offloadrequests,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets,verbs=get
// +kubebuilder:rbac:groups=trainer.kubeflow.org,resources=trainjobs,verbs=get
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups="",resources=pods/binding,verbs=create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,namespace=kueue-offload,resources=leases,verbs=get;list;watch;create;update;patch;delete

// PodWorkloadKey indexes Pods by the Workload they belong to, which the check's
// podSetUpdates name in v1alpha1.WorkloadAnnotation.
const PodWorkloadKey = "metadata.annotations." + v1alpha1.WorkloadAnnotation

func indexPodWorkload(o client.Object) []string {
	if name := o.GetAnnotations()[v1alpha1.WorkloadAnnotation]; name != "" {
		return []string{name}
	}
	return nil
}

// SetupIndexes registers the field indexes the reconcilers list by.
func SetupIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &v1alpha1.OffloadRequest{}, RequestWorkloadKey, indexRequestWorkload); err != nil {
		return fmt.Errorf("indexing OffloadRequests by Workload: %w", err)
	}
	if err := indexer.IndexField(ctx, &corev1.Pod{}, PodWorkloadKey, indexPodWorkload); err != nil {
		return fmt.Errorf("indexing Pods by Workload: %w", err)
	}
	configGVK := v1alpha1.GroupVersion.WithKind("OffloadConfig")
	if err := indexer.IndexField(ctx, &kueue.AdmissionCheck{}, AdmissionCheckUsingConfigKey, admissioncheck.IndexerByConfigFunction(ControllerName, configGVK)); err != nil {
		return fmt.Errorf("indexing AdmissionChecks by OffloadConfig: %w", err)
	}
	return nil
}

// SetupWithManager registers the Workload reconciler, which a change to a
// Workload or to one of its OffloadRequests triggers, and the binder of the
// Workloads' pods, which a change to a Workload or to one of its pods
// triggers.
func (r *WorkloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		Named("workload").
		For(&kueue.Workload{}).
		Owns(&v1alpha1.OffloadRequest{}).
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](time.Second, time.Minute),
		}).
		Complete(r)
	if err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("binder").
		For(&kueue.Workload{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(podWorkload)).
		Complete(&binder{client: r.Client, configs: r.Configs, recorder: r.Recorder})
}

// podWorkload maps a pod to the Workload its podSetUpdates name.
func podWorkload(_ context.Context, o client.Object) []reconcile.Request {
	name := o.GetAnnotations()[v1alpha1.WorkloadAnnotation]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: name}}}
}
