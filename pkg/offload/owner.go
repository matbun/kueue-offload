package offload

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// workloadOwner is what a Workload runs for: its controller, a batch/v1 Job, a
// JobSet, a TrainJob, a RayJob or a RayCluster, or the pods of Kueue's pod
// integration, one pod or a pod group.
type workloadOwner struct {
	ref  *metav1.OwnerReference
	pods []metav1.OwnerReference
	// workload is the Workload's key and uid its UID: a Ray owner's run ends
	// by deactivating it.
	workload types.NamespacedName
	uid      types.UID
}

// ownerKind is a controller owner the offload controller handles.
type ownerKind struct {
	gvk schema.GroupVersionKind
}

var (
	jobKind        = ownerKind{gvk: batchv1.SchemeGroupVersion.WithKind("Job")}
	jobSetKind     = ownerKind{gvk: schema.GroupVersionKind{Group: "jobset.x-k8s.io", Version: "v1alpha2", Kind: "JobSet"}}
	trainJobKind   = ownerKind{gvk: schema.GroupVersionKind{Group: "trainer.kubeflow.org", Version: "v1alpha1", Kind: "TrainJob"}}
	rayJobKind     = ownerKind{gvk: schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayJob"}}
	rayClusterKind = ownerKind{gvk: schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayCluster"}}
	ownerKinds     = []ownerKind{jobKind, jobSetKind, trainJobKind, rayJobKind, rayClusterKind}
)

func kindOf(ref *metav1.OwnerReference) (ownerKind, bool) {
	for _, k := range ownerKinds {
		if ref.APIVersion == k.gvk.GroupVersion().String() && ref.Kind == k.gvk.Kind {
			return k, true
		}
	}
	return ownerKind{}, false
}

// ownerOf returns the Workload's owner, or nil and a message saying why the
// offload controller cannot handle it.
func ownerOf(wl *kueue.Workload) (*workloadOwner, string) {
	key := types.NamespacedName{Namespace: wl.Namespace, Name: wl.Name}
	c := metav1.GetControllerOf(wl)
	if c != nil {
		if _, ok := kindOf(c); ok {
			return &workloadOwner{ref: c, workload: key, uid: wl.UID}, ""
		}
	}
	if c != nil && !isPod(c) {
		return nil, unsupportedOwner(c)
	}
	// A single pod is the Workload's controller; the members of a pod group
	// own it without a controller.
	var pods []metav1.OwnerReference
	for i := range wl.OwnerReferences {
		ref := &wl.OwnerReferences[i]
		if !isPod(ref) {
			return nil, unsupportedOwner(ref)
		}
		pods = append(pods, *ref)
	}
	if len(pods) == 0 {
		return nil, unsupportedOwner(nil)
	}
	return &workloadOwner{pods: pods, workload: key, uid: wl.UID}, ""
}

// ray reports whether the owner is a RayJob or a RayCluster.
func (o *workloadOwner) ray() bool {
	if o == nil || o.ref == nil {
		return false
	}
	k, _ := kindOf(o.ref)
	return k == rayJobKind || k == rayClusterKind
}

func isPod(ref *metav1.OwnerReference) bool {
	return ref.APIVersion == "v1" && ref.Kind == "Pod"
}

func unsupportedOwner(ref *metav1.OwnerReference) string {
	what := "no owner"
	if ref != nil {
		what = fmt.Sprintf("owner %s %s", ref.APIVersion, ref.Kind)
	}
	return "kueue-offload handles only batch/v1 Job, jobset.x-k8s.io/v1alpha2 JobSet, " +
		"trainer.kubeflow.org/v1alpha1 TrainJob, ray.io/v1 RayJob and RayCluster, and v1 Pod workloads, this one has " + what
}

// readOwner returns the owner the executor is told about and the object it
// read: the controller, or the first owner pod that still exists, named after
// its pod group. A nil reference means the owner is gone.
func readOwner(ctx context.Context, r client.Reader, namespace string, o *workloadOwner) (*v1alpha1.OwnerReference, metav1.Object, error) {
	if o.ref != nil {
		k, _ := kindOf(o.ref)
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(k.gvk)
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: o.ref.Name}, obj); err != nil {
			return nil, nil, client.IgnoreNotFound(err)
		}
		if obj.GetUID() != o.ref.UID {
			return nil, nil, nil
		}
		ref := &v1alpha1.OwnerReference{APIVersion: o.ref.APIVersion, Kind: o.ref.Kind, Name: o.ref.Name, UID: o.ref.UID}
		return ref, obj, nil
	}
	for _, p := range o.pods {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: p.Name}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, nil, err
		}
		if pod.UID != p.UID {
			continue
		}
		name := pod.Name
		if g := pod.Labels[podconstants.GroupNameLabel]; g != "" {
			name = g
		}
		return &v1alpha1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: name, UID: pod.UID}, pod, nil
	}
	return nil, nil, nil
}
