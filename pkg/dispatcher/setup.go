package dispatcher

import (
	"context"
	"time"

	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/status,verbs=patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=admissionchecks,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=admissionchecks/status,verbs=get;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=clusterqueues,verbs=get;list;watch
// +kubebuilder:rbac:groups=interlink.eu,resources=siteconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=interlink.eu,resources=submitrecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=interlink.eu,resources=submitrecords/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=patch
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets,verbs=get
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets/status,verbs=patch
// +kubebuilder:rbac:groups=trainer.kubeflow.org,resources=trainjobs,verbs=get
// +kubebuilder:rbac:groups=trainer.kubeflow.org,resources=trainjobs/status,verbs=patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get
// +kubebuilder:rbac:groups="",resources=pods,verbs=get
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,namespace=interlink-dispatch,resources=leases,verbs=get;list;watch;create;update;patch;delete

// SetupWithManager registers the Workload reconciler and the SubmitRecord
// resync.
func (r *WorkloadReconciler) SetupWithManager(mgr ctrl.Manager, resync time.Duration) error {
	err := ctrl.NewControllerManagedBy(mgr).
		Named("workload").
		For(&kueue.Workload{}).
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](time.Second, maxSubmitBackoff),
		}).
		Complete(r)
	if err != nil {
		return err
	}
	return mgr.Add(&resyncer{r: r, every: resync})
}

// resyncer periodically removes the records of deleted Workloads.
type resyncer struct {
	r     *WorkloadReconciler
	every time.Duration
}

func (s *resyncer) NeedLeaderElection() bool { return true }

func (s *resyncer) Start(ctx context.Context) error {
	t := time.NewTicker(s.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := s.r.GarbageCollect(ctx); err != nil {
				log.FromContext(ctx).Error(err, "SubmitRecord resync")
			}
		}
	}
}

func handlerAllChecks(c client.Client) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, _ client.Object) []reconcile.Request {
		list := &kueue.AdmissionCheckList{}
		if err := c.List(ctx, list); err != nil {
			return nil
		}
		reqs := make([]reconcile.Request, 0, len(list.Items))
		for _, ac := range list.Items {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&ac)})
		}
		return reqs
	})
}
