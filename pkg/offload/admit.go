package offload

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// The check goes Ready, and Kueue admits the Workload, once the attempt's job
// has started at the site: the owner then makes or ungates its pods, which the
// check's podSetUpdates send to the virtual node of its OffloadConfig, and the
// binder binds there. Every attempt given up before its start goes on as
// before the admission, with the quota released and no pod deleted.

// started reports whether q's job has started at the site, or ended before
// the offload controller saw it run.
func started(q *v1alpha1.OffloadRequest) bool {
	conds := q.Status.Conditions
	return q.Status.StartTime != nil || meta.IsStatusConditionTrue(conds, v1alpha1.OffloadRequestAdmitted) ||
		meta.IsStatusConditionTrue(conds, v1alpha1.OffloadRequestFinished)
}

// podSetUpdates are what the check sets on the pods of each PodSet that runs
// (runPodSets): the node's label and nodeSelector, so that no scheduler but
// the binder places them, the Workload's name for the binder and the node, and
// tolerations of a node that is not ready, without a time limit, so that a
// restart of the offload controller evicts none of them. Kueue refuses a
// change to them while the check stays Ready, so they depend on the Workload's
// PodSets and the node alone, in the Workload's order.
func podSetUpdates(wl *kueue.Workload, node string) []kueue.PodSetUpdate {
	podSets, _ := runPodSets(wl)
	out := make([]kueue.PodSetUpdate, 0, len(podSets))
	for _, ps := range podSets {
		out = append(out, kueue.PodSetUpdate{
			Name:         ps.Name,
			Labels:       map[string]string{v1alpha1.NodeLabel: node},
			Annotations:  map[string]string{v1alpha1.WorkloadAnnotation: wl.Name},
			NodeSelector: map[string]string{v1alpha1.NodeLabel: node},
			Tolerations: []corev1.Toleration{
				{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
				{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
			},
		})
	}
	return out
}

// readyState is the check Ready with msg and the podSetUpdates for the node
// of the check's OffloadConfig.
func readyState(s site, wl *kueue.Workload, name kueue.AdmissionCheckReference, msg string) kueue.AdmissionCheckState {
	return kueue.AdmissionCheckState{Name: name, State: kueue.CheckStateReady, Message: msg,
		PodSetUpdates: podSetUpdates(wl, v1alpha1.NodeName(s.config))}
}
