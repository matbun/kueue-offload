package offload

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueconstants "sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/util/admissioncheck"
	kueueworkload "sigs.k8s.io/kueue/pkg/workload"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// Labels and annotations the pods' controllers set, which give a pod its index
// within its PodSet.
const (
	jobSetJobIndexKey = "jobset.sigs.k8s.io/job-index"
	jobSetReplicasKey = "jobset.sigs.k8s.io/replicatedjob-replicas"
	rayNodeTypeLabel  = "ray.io/node-type"
)

// binder binds the pods of a Workload Kueue admitted through an offload check
// to the check's virtual node, as a scheduler binds a pod: no scheduler takes
// them, since the check's podSetUpdates select that node. It reconciles one
// Workload at a time, so that each of its pods gets the rank of another member
// of the job, numbered as the executor numbers members: the PodSets that run,
// in Workload order, and the pod's index within its PodSet.
type binder struct {
	client   client.Client
	configs  *configHelper
	recorder events.EventRecorder
}

func (b *binder) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	wl := &kueue.Workload{}
	if err := b.client.Get(ctx, req.NamespacedName, wl); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !kueueworkload.IsAdmitted(wl) || kueueworkload.IsFinished(wl) {
		return ctrl.Result{}, nil
	}
	node, err := b.node(ctx, wl)
	if err != nil || node == "" {
		return ctrl.Result{}, err
	}
	pods := &corev1.PodList{}
	if err := b.client.List(ctx, pods, client.InNamespace(wl.Namespace), client.MatchingFields{PodWorkloadKey: wl.Name}); err != nil {
		return ctrl.Result{}, err
	}
	var errs []error
	for _, a := range ranks(ctx, wl, pods.Items) {
		errs = append(errs, b.bind(ctx, a.pod, a.rank, node))
	}
	return ctrl.Result{}, errors.Join(errs...)
}

// node is the virtual node of the Workload's offload check, or empty while
// the check is not Ready.
func (b *binder) node(ctx context.Context, wl *kueue.Workload) (string, error) {
	checks, err := admissioncheck.FilterForController(ctx, b.client, wl.Status.AdmissionChecks, ControllerName)
	if err != nil {
		return "", err
	}
	for _, name := range checks {
		if st := admissioncheck.FindAdmissionCheck(wl.Status.AdmissionChecks, name); st == nil || st.State != kueue.CheckStateReady {
			continue
		}
		sc, err := b.configs.ConfigForAdmissionCheck(ctx, name)
		if err != nil {
			if apierrors.IsNotFound(err) || errors.Is(err, admissioncheck.ErrBadParametersRef) || errors.Is(err, admissioncheck.ErrNilParametersRef) {
				return "", nil
			}
			return "", err
		}
		return v1alpha1.NodeName(sc.Name), nil
	}
	return "", nil
}

// bind writes the pod's rank, unless it has one, then binds it to node, and
// warns of a time limit on its tolerations of a node that is not ready
// (timedNodeToleration). A pod the podSetUpdates labelled for another node is
// left alone.
func (b *binder) bind(ctx context.Context, pod *corev1.Pod, rank int32, node string) error {
	if v := pod.Labels[v1alpha1.NodeLabel]; v != node {
		log.FromContext(ctx).Info("not binding a pod labelled for another node", "pod", pod.Name, "label", v, "node", node)
		return nil
	}
	ranked := pod.DeepCopy()
	if _, ok := ranked.Annotations[v1alpha1.RankAnnotation]; !ok {
		metav1.SetMetaDataAnnotation(&ranked.ObjectMeta, v1alpha1.RankAnnotation, strconv.Itoa(int(rank)))
	}
	if !equality.Semantic.DeepEqual(ranked, pod) {
		if err := b.client.Patch(ctx, ranked, client.MergeFrom(pod)); err != nil {
			return client.IgnoreNotFound(err)
		}
		pod = ranked
	}
	if seconds, ok := timedNodeToleration(pod.Spec.Tolerations); ok {
		b.recorder.Eventf(pod, nil, corev1.EventTypeWarning, "TimedNodeTolerations", "Bind",
			"the pod tolerates a virtual node that is not ready for %d s only: past that the pod is evicted and its job cancelled; the site's admission policy should drop tolerationSeconds of node.kubernetes.io/not-ready and unreachable at create", seconds)
	}
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID},
		Target:     corev1.ObjectReference{Kind: "Node", Name: node},
	}
	// Not found: the pod is gone; conflict: it is bound already, or replaced.
	if err := b.client.SubResource("binding").Create(ctx, pod, binding); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return err
	}
	return nil
}

// timedNodeToleration is the shortest time limit of the pod's tolerations of a
// node that is not ready or unreachable. The API server gives a pod 300 s of
// each at create when it has none, as a plain pod has before Kueue applies the
// check's podSetUpdates, whose tolerations have no limit; the shortest limit
// counts.
func timedNodeToleration(tolerations []corev1.Toleration) (int64, bool) {
	var shortest *int64
	for _, t := range tolerations {
		if t.Effect == corev1.TaintEffectNoExecute && (t.Key == corev1.TaintNodeNotReady || t.Key == corev1.TaintNodeUnreachable) &&
			t.TolerationSeconds != nil && (shortest == nil || *t.TolerationSeconds < *shortest) {
			shortest = t.TolerationSeconds
		}
	}
	if shortest == nil {
		return 0, false
	}
	return *shortest, true
}

type rankedPod struct {
	pod  *corev1.Pod
	rank int32
}

// ranks are the Workload's pods to bind, with their ranks: the offset of the
// pod's PodSet among the PodSets that run, counted from the admission, plus
// the pod's index within it (podSetRanks).
func ranks(ctx context.Context, wl *kueue.Workload, pods []corev1.Pod) []rankedPod {
	podSets, admitted := runPodSets(wl)
	counts := map[kueue.PodSetReference]int32{}
	for _, ps := range podSets {
		counts[ps.Name] = ps.Count
	}
	for _, a := range admitted {
		if a.Count != nil {
			counts[a.Name] = *a.Count
		}
	}
	byPodSet := map[kueue.PodSetReference][]*corev1.Pod{}
	for i := range pods {
		p := &pods[i]
		if p.Annotations[v1alpha1.WorkloadAnnotation] == wl.Name {
			ps := kueue.PodSetReference(p.Labels[kueueconstants.PodSetLabel])
			byPodSet[ps] = append(byPodSet[ps], p)
		}
	}
	var (
		out    []rankedPod
		offset int32
	)
	for _, ps := range podSets {
		out = append(out, podSetRanks(ctx, byPodSet[ps.Name], offset, counts[ps.Name])...)
		offset += counts[ps.Name]
	}
	return out
}

// podSetRanks ranks the pods of one PodSet of count members that wait to be
// bound. A pod keeps the rank it has. Otherwise its index is the one its
// controller gives it (podIndex), modulo count: an Indexed Job with more
// completions than parallelism shows the member of that index's end. A pod
// without one takes the lowest index no other pod of the PodSet holds that is
// bound, alive and not being deleted, in natural name order, then creation
// time.
func podSetRanks(ctx context.Context, pods []*corev1.Pod, offset, count int32) []rankedPod {
	if count <= 0 {
		return nil
	}
	held := map[int32]bool{}
	var (
		out  []rankedPod
		free []*corev1.Pod
	)
	for _, p := range pods {
		rank, ranked, err := rankOf(p)
		if err != nil {
			log.FromContext(ctx).Info("not binding a pod whose rank does not parse", "pod", p.Name, "error", err.Error())
			continue
		}
		if p.Spec.NodeName != "" {
			if ranked && p.DeletionTimestamp == nil && !podTerminal(p) {
				held[rank-offset] = true
			}
			continue
		}
		if len(p.Spec.SchedulingGates) > 0 || p.DeletionTimestamp != nil || podTerminal(p) {
			continue
		}
		if ranked {
			held[rank-offset] = true
			out = append(out, rankedPod{pod: p, rank: rank})
			continue
		}
		if i, ok := podIndex(p, count); ok {
			held[i%count] = true
			out = append(out, rankedPod{pod: p, rank: offset + i%count})
			continue
		}
		free = append(free, p)
	}
	slices.SortStableFunc(free, func(a, b *corev1.Pod) int {
		if c := naturalCompare(a.Name, b.Name); c != 0 {
			return c
		}
		return a.CreationTimestamp.Compare(b.CreationTimestamp.Time)
	})
	next := int32(0)
	for _, p := range free {
		for next < count && held[next] {
			next++
		}
		if next == count {
			break
		}
		held[next] = true
		out = append(out, rankedPod{pod: p, rank: offset + next})
	}
	return out
}

// rankOf reads the pod's v1alpha1.RankAnnotation.
func rankOf(p *corev1.Pod) (int32, bool, error) {
	v, ok := p.Annotations[v1alpha1.RankAnnotation]
	if !ok {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 0 {
		return 0, false, errors.New(v1alpha1.RankAnnotation + " " + strconv.Quote(v) + " is not a rank")
	}
	return int32(n), true, nil
}

// podIndex is the pod's index within its PodSet of count members that the
// pod's controller gives it: a JobSet's job index times the members of each of
// its Jobs, plus the completion index within the Job; an Indexed Job's
// completion index; a StatefulSet's ordinal; 0 for a Ray head. Pods of a
// NonIndexed Job, Ray workers and plain pods have none.
func podIndex(p *corev1.Pod, count int32) (int32, bool) {
	if job, ok := intKey(p, jobSetJobIndexKey); ok {
		replicas, ok := intKey(p, jobSetReplicasKey)
		if !ok || replicas <= 0 {
			replicas = 1
		}
		completion, _ := intKey(p, batchv1.JobCompletionIndexAnnotation)
		return job*(count/replicas) + completion, true
	}
	if i, ok := intKey(p, batchv1.JobCompletionIndexAnnotation); ok {
		return i, true
	}
	if i, ok := intKey(p, appsv1.PodIndexLabel); ok {
		return i, true
	}
	if p.Labels[rayNodeTypeLabel] == "head" {
		return 0, true
	}
	return 0, false
}

// intKey reads a non-negative number from the pod's label key, else from its
// annotation key.
func intKey(p *corev1.Pod, key string) (int32, bool) {
	v, ok := p.Labels[key]
	if !ok {
		v, ok = p.Annotations[key]
	}
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 0 {
		return 0, false
	}
	return int32(n), true
}

func podTerminal(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
}

// naturalCompare compares names with their runs of digits compared as
// numbers: train-2 before train-10.
func naturalCompare(a, b string) int {
	for a != "" && b != "" {
		da, db := digits(a), digits(b)
		if da == 0 || db == 0 {
			if a[0] != b[0] {
				return cmp.Compare(a[0], b[0])
			}
			a, b = a[1:], b[1:]
			continue
		}
		na, nb := trimZeros(a[:da]), trimZeros(b[:db])
		if c := cmp.Compare(len(na), len(nb)); c != 0 {
			return c
		}
		if c := cmp.Compare(na, nb); c != 0 {
			return c
		}
		a, b = a[da:], b[db:]
	}
	return cmp.Compare(len(a), len(b))
}

// digits is the length of the run of digits s starts with.
func digits(s string) int {
	n := 0
	for n < len(s) && s[n] >= '0' && s[n] <= '9' {
		n++
	}
	return n
}

func trimZeros(s string) string {
	for len(s) > 1 && s[0] == '0' {
		s = s[1:]
	}
	return s
}
