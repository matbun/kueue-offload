package offload

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueconstants "sigs.k8s.io/kueue/pkg/constants"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// bindEnv runs the binder against a fake client, which has no binding
// subresource: a binding sets the pod's nodeName, as the API server does, and
// a pod bound already is a conflict.
type bindEnv struct {
	t        *testing.T
	c        client.Client
	b        *binder
	bindings []string
	patches  int
	events   *events.FakeRecorder
}

func newBindEnv(t *testing.T, wl *kueue.Workload, pods ...*corev1.Pod) *bindEnv {
	t.Helper()
	e := &bindEnv{t: t}
	objs := []client.Object{offloadConfig(), admissionCheck("hpc-a", ControllerName, testSite), admissionCheck("other-check", "example.org/other", "x"), wl}
	for _, p := range pods {
		objs = append(objs, p)
	}
	e.c = testClient(t, objs, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				e.patches++
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
		SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj, subResource client.Object, opts ...client.SubResourceCreateOption) error {
			b, ok := subResource.(*corev1.Binding)
			if sub != "binding" || !ok {
				return fmt.Errorf("unexpected create of %s", sub)
			}
			pod := &corev1.Pod{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), pod); err != nil {
				return err
			}
			if pod.Spec.NodeName != "" || pod.UID != b.UID {
				return apierrors.NewConflict(schema.GroupResource{Resource: "pods/binding"}, pod.Name, fmt.Errorf("pod %s is already assigned to node %q", pod.Name, pod.Spec.NodeName))
			}
			e.bindings = append(e.bindings, pod.Name+"="+b.Target.Name)
			pod.Spec.NodeName = b.Target.Name
			return c.Update(ctx, pod)
		},
	})
	configs, err := NewConfigHelper(e.c)
	if err != nil {
		t.Fatal(err)
	}
	e.events = events.NewFakeRecorder(10)
	e.b = &binder{client: e.c, configs: configs, recorder: e.events}
	return e
}

func (e *bindEnv) reconcile() {
	e.t.Helper()
	if _, err := e.b.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: wlName}}); err != nil {
		e.t.Fatalf("reconcile: %v", err)
	}
}

// ranks are the rank annotations of the pods bound to the check's node.
func (e *bindEnv) ranks() map[string]string {
	e.t.Helper()
	pods := &corev1.PodList{}
	if err := e.c.List(context.Background(), pods, client.InNamespace(testNS)); err != nil {
		e.t.Fatal(err)
	}
	out := map[string]string{}
	for _, p := range pods.Items {
		if p.Spec.NodeName == testNode {
			out[p.Name] = p.Annotations[v1alpha1.RankAnnotation]
		}
	}
	return out
}

// admitted is wl as Kueue admits it: every check Ready, Admitted.
func admitted(wl *kueue.Workload) *kueue.Workload {
	for i := range wl.Status.AdmissionChecks {
		wl.Status.AdmissionChecks[i].State = kueue.CheckStateReady
	}
	meta.SetStatusCondition(&wl.Status.Conditions, metav1.Condition{Type: kueue.WorkloadAdmitted, Status: metav1.ConditionTrue, Reason: "Admitted"})
	return wl
}

// withPodSets gives wl these PodSets of these counts, each admitted with its
// count.
func withPodSets(wl *kueue.Workload, counts ...any) *kueue.Workload {
	wl.Spec.PodSets, wl.Status.Admission.PodSetAssignments = nil, nil
	for i := 0; i < len(counts); i += 2 {
		name, n := kueue.PodSetReference(counts[i].(string)), int32(counts[i+1].(int))
		wl.Spec.PodSets = append(wl.Spec.PodSets, kueue.PodSet{Name: name, Count: n, Template: podTemplate()})
		wl.Status.Admission.PodSetAssignments = append(wl.Status.Admission.PodSetAssignments, kueue.PodSetAssignment{Name: name, Count: ptr.To(n)})
	}
	return wl
}

// offloadPod is a pod Kueue ungated with the check's podSetUpdates, of PodSet
// podSet, with labels from kv.
func offloadPod(name, podSet string, kv ...string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: testNS, UID: types.UID("uid-" + name),
			Labels:      map[string]string{v1alpha1.NodeLabel: testNode, kueueconstants.PodSetLabel: podSet},
			Annotations: map[string]string{v1alpha1.WorkloadAnnotation: wlName},
		},
		Spec:   podTemplate().Spec,
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	for i := 0; i < len(kv); i += 2 {
		p.Labels[kv[i]] = kv[i+1]
	}
	return p
}

// bound is p bound to the check's node with rank.
func bound(p *corev1.Pod, rank string, phase corev1.PodPhase) *corev1.Pod {
	p.Spec.NodeName = testNode
	p.Annotations[v1alpha1.RankAnnotation] = rank
	p.Status.Phase = phase
	return p
}

// Each pod is bound with the rank of the member it shows, as the executor
// numbers members, for every owner kind.
func TestBinderRanks(t *testing.T) {
	const completion, jobIndex, replicas = "batch.kubernetes.io/job-completion-index", "jobset.sigs.k8s.io/job-index", "jobset.sigs.k8s.io/replicatedjob-replicas"
	deleting := bound(offloadPod("train-c", "main"), "2", corev1.PodRunning)
	deleting.DeletionTimestamp, deleting.Finalizers = ptr.To(metav1.Now()), []string{"example.org/keep"}
	for _, tc := range []struct {
		name string
		wl   *kueue.Workload
		pods []*corev1.Pod
		want map[string]string
	}{
		{"Indexed Job", workload(), []*corev1.Pod{
			offloadPod("train-a", "main", completion, "3"), offloadPod("train-b", "main", completion, "0"),
			offloadPod("train-c", "main", completion, "2"), offloadPod("train-d", "main", completion, "1"),
		}, map[string]string{"train-a": "3", "train-b": "0", "train-c": "2", "train-d": "1"}},
		{"Indexed Job, more completions than parallelism", workload(), []*corev1.Pod{
			offloadPod("train-f", "main", completion, "5"),
		}, map[string]string{"train-f": "1"}},
		// A Failed and a deleted pod hold no index; train-10 comes after train-9,
		// and finds none left.
		{"NonIndexed Job", withPodSets(workload(), "main", 3), []*corev1.Pod{
			bound(offloadPod("train-a", "main"), "1", corev1.PodRunning), bound(offloadPod("train-b", "main"), "0", corev1.PodFailed), deleting,
			offloadPod("train-10", "main"), offloadPod("train-9", "main"), offloadPod("train-11", "main"),
		}, map[string]string{"train-a": "1", "train-b": "0", "train-c": "2", "train-9": "0", "train-10": "2"}},
		// A JobSet of a leader Job and two worker Jobs of two pods, as a
		// TrainJob's.
		{"JobSet", withPodSets(workload(), "leader", 1, "worker", 4), []*corev1.Pod{
			offloadPod("train-worker-1-1", "worker", jobIndex, "1", replicas, "2", completion, "1"),
			offloadPod("train-worker-0-0", "worker", jobIndex, "0", replicas, "2", completion, "0"),
			offloadPod("train-worker-1-0", "worker", jobIndex, "1", replicas, "2", completion, "0"),
			offloadPod("train-leader-0-0", "leader", jobIndex, "0", replicas, "1", completion, "0"),
		}, map[string]string{"train-leader-0-0": "0", "train-worker-0-0": "1", "train-worker-1-0": "3", "train-worker-1-1": "4"}},
		// Kueue admitted two of four: the next PodSet starts at 2.
		{"partial admission", func() *kueue.Workload {
			wl := withPodSets(workload(), "a", 4, "b", 1)
			wl.Status.Admission.PodSetAssignments[0].Count = ptr.To[int32](2)
			return wl
		}(), []*corev1.Pod{offloadPod("train-b", "b")}, map[string]string{"train-b": "2"}},
		{"notebook", withPodSets(workload(), podSetKey, 1), []*corev1.Pod{
			offloadPod("nb-0", podSetKey, "apps.kubernetes.io/pod-index", "0"),
		}, map[string]string{"nb-0": "0"}},
		// A StatefulSet's pods as one group: the ordinal, also before the pod of
		// a lower one exists.
		{"StatefulSet group", withPodSets(workload(), podSetKey, 2), []*corev1.Pod{
			offloadPod("web-1", podSetKey, "apps.kubernetes.io/pod-index", "1"),
		}, map[string]string{"web-1": "1"}},
		{"pod group", withPodSets(workload(), podSetKey, 2), []*corev1.Pod{
			offloadPod("train-1", podSetKey), offloadPod("train-0", podSetKey),
		}, map[string]string{"train-0": "0", "train-1": "1"}},
		{"RayCluster", rayWorkload(rayClusterKind, 2, false), []*corev1.Pod{
			offloadPod("ray-gpu-worker-b", "gpu", "ray.io/node-type", "worker"), offloadPod("ray-gpu-worker-a", "gpu", "ray.io/node-type", "worker"),
			offloadPod("ray-head-x", "head", "ray.io/node-type", "head"),
		}, map[string]string{"ray-head-x": "0", "ray-gpu-worker-a": "1", "ray-gpu-worker-b": "2"}},
		// The head is the first member, also while the head it replaces still
		// shows that member.
		{"Ray head", rayWorkload(rayClusterKind, 0, false), []*corev1.Pod{
			bound(offloadPod("ray-head-a", "head", "ray.io/node-type", "head"), "0", corev1.PodRunning),
			offloadPod("ray-head-b", "head", "ray.io/node-type", "head"),
		}, map[string]string{"ray-head-a": "0", "ray-head-b": "0"}},
		// The submitter runs in the cluster: never bound.
		{"RayJob", rayWorkload(rayJobKind, 1, true), []*corev1.Pod{
			offloadPod("ray-head-x", "head", "ray.io/node-type", "head"), offloadPod("ray-gpu-worker-a", "gpu", "ray.io/node-type", "worker"),
			offloadPod("ray-submitter", "submitter"),
		}, map[string]string{"ray-head-x": "0", "ray-gpu-worker-a": "1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newBindEnv(t, admitted(tc.wl), tc.pods...)
			e.reconcile()
			if got := e.ranks(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ranks %v, want %v", got, tc.want)
			}
			for _, b := range e.bindings {
				if !slices.Contains(slices.Collect(maps.Keys(tc.want)), b[:len(b)-len("="+testNode)]) {
					t.Errorf("binding %s", b)
				}
			}
		})
	}
}

// A rank once written is kept: a pod whose binding failed after it is bound
// with it, and nothing is written again once the pods are bound.
func TestBinderKeepsTheRank(t *testing.T) {
	ranked := offloadPod("train-a", "main", "batch.kubernetes.io/job-completion-index", "3")
	ranked.Annotations[v1alpha1.RankAnnotation] = "1"
	e := newBindEnv(t, admitted(workload()), ranked, offloadPod("train-b", "main", "batch.kubernetes.io/job-completion-index", "2"))
	e.reconcile()
	if got := e.ranks(); !reflect.DeepEqual(got, map[string]string{"train-a": "1", "train-b": "2"}) {
		t.Errorf("ranks %v", got)
	}
	if e.patches != 1 {
		t.Errorf("%d pod patches, want the rank of train-b only", e.patches)
	}
	bindings := len(e.bindings)
	e.reconcile()
	if e.patches != 1 || len(e.bindings) != bindings {
		t.Errorf("bound pods written again: %d patches, %d bindings", e.patches, len(e.bindings)-bindings)
	}

	// The index of a pod ranked before is not given to another.
	ranked = offloadPod("train-z", "main")
	ranked.Annotations[v1alpha1.RankAnnotation] = "0"
	e = newBindEnv(t, admitted(withPodSets(workload(), "main", 2)), ranked, offloadPod("train-b", "main"))
	e.reconcile()
	if got := e.ranks(); !reflect.DeepEqual(got, map[string]string{"train-z": "0", "train-b": "1"}) {
		t.Errorf("ranks %v, want train-b next to the rank train-z holds", got)
	}
}

// Nothing is bound before Kueue admitted the Workload through a Ready check,
// nor after it finished; nor a pod that is gated, being deleted, of another
// node or of another Workload.
func TestBinderBindsOnlyAdmittedPods(t *testing.T) {
	gated := offloadPod("train-g", "main")
	gated.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}}
	gone := offloadPod("train-d", "main")
	gone.DeletionTimestamp, gone.Finalizers = ptr.To(metav1.Now()), []string{"example.org/keep"}
	other := offloadPod("train-n", "main")
	other.Labels[v1alpha1.NodeLabel] = "kueue-offload-elsewhere"
	foreign := offloadPod("train-w", "main")
	foreign.Annotations[v1alpha1.WorkloadAnnotation] = "another-workload"
	for _, tc := range []struct {
		name string
		wl   *kueue.Workload
		pods []*corev1.Pod
	}{
		{"quota reserved, check Pending", workload(), []*corev1.Pod{offloadPod("train-a", "main")}},
		{"check Ready, not admitted", func() *kueue.Workload {
			wl := workload()
			findCheck(wl, "hpc-a").State = kueue.CheckStateReady
			return wl
		}(), []*corev1.Pod{offloadPod("train-a", "main")}},
		{"check Retry while Admitted", func() *kueue.Workload {
			wl := admitted(workload())
			findCheck(wl, "hpc-a").State = kueue.CheckStateRetry
			return wl
		}(), []*corev1.Pod{offloadPod("train-a", "main")}},
		{"finished", func() *kueue.Workload {
			wl := admitted(workload())
			meta.SetStatusCondition(&wl.Status.Conditions, metav1.Condition{Type: kueue.WorkloadFinished, Status: metav1.ConditionTrue, Reason: "Succeeded"})
			return wl
		}(), []*corev1.Pod{offloadPod("train-a", "main")}},
		{"pods that are not to bind", admitted(workload()), []*corev1.Pod{gated, gone, other, foreign}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newBindEnv(t, tc.wl, tc.pods...)
			e.reconcile()
			if len(e.bindings) != 0 || e.patches != 0 {
				t.Errorf("bindings %v, %d patches", e.bindings, e.patches)
			}
		})
	}
}

// A pod that still tolerates a node that is not ready for 300 s only, the API
// server's default, would be evicted, and its job cancelled, by a virtual node
// not ready for that long. The site's admission policy lifts the limit at create
// (a policy may freeze a pod's tolerations after); the binder binds the pod as
// it is and warns, with an event on the pod.
func TestBinderWarnsOfTimedNodeTolerations(t *testing.T) {
	pod := func(seconds *int64) *corev1.Pod {
		p := offloadPod("train-a", "main")
		p.Spec.Tolerations = []corev1.Toleration{
			{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: seconds},
			{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: seconds},
		}
		return p
	}
	for name, tt := range map[string]struct {
		seconds *int64
		warned  bool
	}{"timed": {ptr.To[int64](300), true}, "untimed": {nil, false}} {
		e := newBindEnv(t, admitted(workload()), pod(tt.seconds))
		e.reconcile()
		if !reflect.DeepEqual(e.ranks(), map[string]string{"train-a": "0"}) {
			t.Errorf("%s: ranks %v, want train-a bound", name, e.ranks())
		}
		p := &corev1.Pod{}
		if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "train-a"}, p); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(p.Spec.Tolerations[0].TolerationSeconds, tt.seconds) {
			t.Errorf("%s: tolerations changed to %v", name, p.Spec.Tolerations)
		}
		warned := false
		for len(e.events.Events) > 0 {
			warned = warned || strings.Contains(<-e.events.Events, "Warning TimedNodeTolerations")
		}
		if warned != tt.warned {
			t.Errorf("%s: warned %v, want %v", name, warned, tt.warned)
		}
	}
}

// A pod finds its Workload through the annotation the podSetUpdates add.
func TestPodWorkloadMapsTheAnnotation(t *testing.T) {
	got := podWorkload(context.Background(), offloadPod("train-a", "main"))
	if want := []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: testNS, Name: wlName}}}; !reflect.DeepEqual(got, want) {
		t.Errorf("requests %v, want %v", got, want)
	}
	if got := podWorkload(context.Background(), &corev1.Pod{}); got != nil {
		t.Errorf("a pod without the annotation maps to %v", got)
	}
}

func TestNaturalCompare(t *testing.T) {
	sorted := []string{"a", "a1", "a2", "a10", "a10b", "b", "train-2-x", "train-10"}
	got := slices.Clone(sorted)
	slices.Reverse(got)
	slices.SortFunc(got, naturalCompare)
	if !slices.Equal(got, sorted) {
		t.Errorf("sorted %v, want %v", got, sorted)
	}
}
