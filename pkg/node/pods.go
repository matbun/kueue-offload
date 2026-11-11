package node

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/offload"
)

// PodStatusReconciler is the virtual nodes' kubelet: it shows on each pod bound
// to a virtual node the state of the member of the job the pod stands for, and
// removes the pods being deleted. A change to the pod, to its Workload or to
// one of the Workload's OffloadRequests triggers it.
type PodStatusReconciler struct {
	Client client.Client
	// NodeIP is the virtual nodes' address, the hostIP of their pods.
	NodeIP string
	Now    func() time.Time
}

func (r *PodStatusReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// onVirtualNode reports whether a pod is bound to the virtual node its
// podSetUpdates label names.
func onVirtualNode(pod *corev1.Pod) bool {
	return pod.Spec.NodeName != "" && pod.Spec.NodeName == pod.Labels[v1alpha1.NodeLabel]
}

// SetupWithManager registers the reconciler.
func (r *PodStatusReconciler) SetupWithManager(mgr ctrl.Manager) error {
	bound := predicate.NewPredicateFuncs(func(o client.Object) bool {
		pod, ok := o.(*corev1.Pod)
		return ok && onVirtualNode(pod)
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("node-pods").
		For(&corev1.Pod{}, builder.WithPredicates(bound)).
		Watches(&kueue.Workload{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.podsOf(ctx, o.GetNamespace(), o.GetName())
		})).
		Watches(&v1alpha1.OffloadRequest{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			q, ok := o.(*v1alpha1.OffloadRequest)
			if !ok {
				return nil
			}
			return r.podsOf(ctx, q.Namespace, q.Spec.Workload.Name)
		})).
		Complete(r)
}

// podsOf are the pods on a virtual node of the Workload named workload.
func (r *PodStatusReconciler) podsOf(ctx context.Context, namespace, workload string) []reconcile.Request {
	pods := &corev1.PodList{}
	if err := r.Client.List(ctx, pods, client.InNamespace(namespace), client.MatchingFields{offload.PodWorkloadKey: workload}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "listing the pods of a Workload", "workload", workload)
		return nil
	}
	var out []reconcile.Request
	for i := range pods.Items {
		if onVirtualNode(&pods.Items[i]) {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&pods.Items[i])})
		}
	}
	return out
}

func (r *PodStatusReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pod := &corev1.Pod{}
	if err := r.Client.Get(ctx, req.NamespacedName, pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !onVirtualNode(pod) {
		return ctrl.Result{}, nil
	}
	wl, q, err := r.job(ctx, pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !pod.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.remove(ctx, pod, q)
	}
	if podFinished(pod) {
		return ctrl.Result{}, nil
	}
	st := podStatus(pod, wl, q, rankAnnotation(pod), r.NodeIP, r.now())
	if equality.Semantic.DeepEqual(pod.Status, st) {
		return ctrl.Result{}, nil
	}
	if st.Phase == corev1.PodFailed {
		ok, err := r.notRetriableInGroup(ctx, pod)
		if err != nil || !ok {
			return ctrl.Result{}, err
		}
		st = podStatus(pod, wl, q, rankAnnotation(pod), r.NodeIP, r.now())
	}
	done := pod.DeepCopy()
	done.Status = st
	return ctrl.Result{}, client.IgnoreNotFound(r.Client.Status().Patch(ctx, done, client.StrategicMergeFrom(pod)))
}

// job is the Workload of pod and its live request: the newest attempt of that
// Workload that is not being deleted. Either is nil when there is none.
func (r *PodStatusReconciler) job(ctx context.Context, pod *corev1.Pod) (*kueue.Workload, *v1alpha1.OffloadRequest, error) {
	name := pod.Annotations[v1alpha1.WorkloadAnnotation]
	if name == "" {
		return nil, nil, nil
	}
	wl := &kueue.Workload{}
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: name}, wl); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	reqs := &v1alpha1.OffloadRequestList{}
	if err := r.Client.List(ctx, reqs, client.InNamespace(pod.Namespace), client.MatchingFields{offload.RequestWorkloadKey: name}); err != nil {
		return nil, nil, err
	}
	var live *v1alpha1.OffloadRequest
	for i := range reqs.Items {
		q := &reqs.Items[i]
		if q.Spec.Workload.UID == wl.UID && q.DeletionTimestamp.IsZero() && (live == nil || q.Spec.Attempt > live.Spec.Attempt) {
			live = q
		}
	}
	return wl, live, nil
}

// remove deletes a pod being deleted at once, as a kubelet does once its
// containers stopped: the job goes with its request. A member of a pod group
// whose job has not finished is first marked not retriable, since no
// allocation can take a replacement and Kueue would wait for one.
func (r *PodStatusReconciler) remove(ctx context.Context, pod *corev1.Pod, q *v1alpha1.OffloadRequest) error {
	if q == nil || !meta.IsStatusConditionTrue(q.Status.Conditions, v1alpha1.OffloadRequestFinished) {
		if ok, err := r.notRetriableInGroup(ctx, pod); err != nil || !ok {
			return err
		}
	}
	if ptr.Deref(pod.DeletionGracePeriodSeconds, 1) == 0 {
		return nil
	}
	err := r.Client.Delete(ctx, pod, client.GracePeriodSeconds(0), client.Preconditions(metav1.Preconditions{UID: &pod.UID}))
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return err
}

// notRetriableInGroup marks a member of a pod group not retriable, so that
// Kueue finishes the group when the member fails or goes instead of waiting for
// a replacement. It reports false when the pod is gone.
func (r *PodStatusReconciler) notRetriableInGroup(ctx context.Context, pod *corev1.Pod) (bool, error) {
	if pod.Labels[podconstants.GroupNameLabel] == "" ||
		pod.Annotations[podconstants.RetriableInGroupAnnotationKey] == podconstants.RetriableInGroupAnnotationValue {
		return true, nil
	}
	orig := pod.DeepCopy()
	metav1.SetMetaDataAnnotation(&pod.ObjectMeta, podconstants.RetriableInGroupAnnotationKey, podconstants.RetriableInGroupAnnotationValue)
	if err := r.Client.Patch(ctx, pod, client.MergeFrom(orig)); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
