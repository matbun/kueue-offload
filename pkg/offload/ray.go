package offload

import (
	"context"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// KubeRay's RayJob and RayCluster run as one batch job, which the site's
// launcher turns into the Ray cluster, head on the first node. Once the job
// has started Kueue admits the Workload and KubeRay runs the cluster's pods on
// the virtual node, the head with the shadow's IP: KubeRay reaches the head's
// dashboard through the shadow, submits a RayJob's entrypoint there and
// completes it as it would in the cluster.

// +kubebuilder:rbac:groups=ray.io,resources=rayjobs;rayclusters,verbs=get
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=patch

// RayClusterLauncher is the site launcher (interlink.eu/launcher) Ray owners
// run with, unless their pod templates name another: it starts the cluster and
// blocks on the head.
const RayClusterLauncher = "ray"

const (
	launcherAnnotation = "interlink.eu/launcher"
	// Kueue's PodSet names (pkg/controller/jobs/rayjob, raycluster).
	rayHeadPodSet      = kueue.PodSetReference("head")
	raySubmitterPodSet = kueue.PodSetReference("submitter")
)

// rayOwnerOf returns the Workload's controller when it is a RayJob or a
// RayCluster.
func rayOwnerOf(wl *kueue.Workload) *metav1.OwnerReference {
	c := metav1.GetControllerOf(wl)
	if c == nil {
		return nil
	}
	if k, ok := kindOf(c); ok && (k == rayJobKind || k == rayClusterKind) {
		return c
	}
	return nil
}

// runPodSets are the Workload's PodSets that run, with their assignments: all
// of them, less a Ray owner's worker groups of no replicas and a RayJob's
// submitter, which runs in the cluster when KubeRay makes it, and submits the
// entrypoint to the head through the shadow.
func runPodSets(wl *kueue.Workload) ([]kueue.PodSet, []kueue.PodSetAssignment) {
	var assignments []kueue.PodSetAssignment
	if wl.Status.Admission != nil {
		assignments = wl.Status.Admission.PodSetAssignments
	}
	c := rayOwnerOf(wl)
	if c == nil {
		return wl.Spec.PodSets, assignments
	}
	skip := map[kueue.PodSetReference]bool{}
	for _, ps := range wl.Spec.PodSets {
		if ps.Count == 0 || (c.Kind == rayJobKind.gvk.Kind && ps.Name == raySubmitterPodSet) {
			skip[ps.Name] = true
		}
	}
	podSets := slices.DeleteFunc(slices.Clone(wl.Spec.PodSets), func(ps kueue.PodSet) bool { return skip[ps.Name] })
	assignments = slices.DeleteFunc(slices.Clone(assignments), func(a kueue.PodSetAssignment) bool { return skip[a.Name] })
	return podSets, assignments
}

// Ports of a Ray head that the shadow forwards: the dashboard, which KubeRay
// polls and submits a RayJob's entrypoint to, and the client port. KubeRay's
// head Service exposes them, and a head template declares none of them by
// default.
var rayHeadPorts = []corev1.ContainerPort{
	{Name: "dashboard", ContainerPort: 8265, Protocol: corev1.ProtocolTCP},
	{Name: "client", ContainerPort: 10001, Protocol: corev1.ProtocolTCP},
}

// rayPodSets prepares the PodSets of a Ray owner: every template names the
// site launcher, unless it names one, and the head declares the dashboard and
// client ports in its first container, which the shadow then forwards. A Ray
// owner of its head alone runs as a pod group of one, which the executor makes
// for a member that names a launcher.
func rayPodSets(podSets []v1alpha1.PodSet) {
	for i := range podSets {
		t := &podSets[i].Template
		if t.Annotations[launcherAnnotation] == "" {
			if t.Annotations == nil {
				t.Annotations = map[string]string{}
			}
			t.Annotations[launcherAnnotation] = RayClusterLauncher
		}
		if podSets[i].Name != rayHeadPodSet || len(t.Spec.Containers) == 0 {
			continue
		}
		ctr := &t.Spec.Containers[0]
		for _, p := range rayHeadPorts {
			if !slices.ContainsFunc(ctr.Ports, func(q corev1.ContainerPort) bool { return q.ContainerPort == p.ContainerPort }) {
				ctr.Ports = append(ctr.Ports, p)
			}
		}
	}
}

// RequestPodSets is what an OffloadRequest for wl carries of the Workload's
// PodSets, as the offload controller prepares them: those that run
// (runPodSets), with the templates a Ray owner's launcher needs (rayPodSets),
// and the admission of each. The executor checks a request against them.
func RequestPodSets(wl *kueue.Workload) ([]kueue.PodSet, []kueue.PodSetAssignment) {
	podSets, admitted := runPodSets(wl)
	if rayOwnerOf(wl) == nil {
		return podSets, admitted
	}
	prepared := make([]v1alpha1.PodSet, len(podSets))
	for i, ps := range podSets {
		prepared[i] = v1alpha1.PodSet{Name: ps.Name, Count: ps.Count, Template: *ps.Template.DeepCopy()}
	}
	rayPodSets(prepared)
	out := slices.Clone(podSets)
	for i := range out {
		out[i].Template = prepared[i].Template
	}
	return out, admitted
}

// deactivateWorkload ends the run of a Ray owner whose job ended at the site:
// the Workload is deactivated, as a user stops one, and Kueue evicts it, which
// suspends the owner and releases its quota; the request goes with the
// eviction, and with it the shadow. Setting spec.active again starts the
// cluster anew.
func deactivateWorkload(ctx context.Context, c client.Client, r client.Reader, key types.NamespacedName, uid types.UID) error {
	wl := &kueue.Workload{}
	if err := r.Get(ctx, key, wl); err != nil {
		return client.IgnoreNotFound(err)
	}
	if wl.UID != uid || !ptr.Deref(wl.Spec.Active, true) {
		return nil
	}
	inactive := wl.DeepCopy()
	inactive.Spec.Active = ptr.To(false)
	err := c.Patch(ctx, inactive, client.MergeFrom(wl))
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
