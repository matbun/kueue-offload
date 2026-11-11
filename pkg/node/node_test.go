package node

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// The virtual node is a kubelet's node: labelled so that OS-selecting
// DaemonSets and load balancers skip it, tainted so that only bound pods reach
// it, small, and reachable at the controller's address.
// The OffloadConfig's nodeLabels follow the cluster's own conventions, such as the
// label its DaemonSets exclude virtual nodes by, and never replace the node's own.
func TestBuildNodeExtraLabels(t *testing.T) {
	cfg := testConfig()
	cfg.Spec.NodeLabels = map[string]string{"cluster.example.org/virtual": "true", v1alpha1.NodeLabel: "other", "kubernetes.io/os": "linux"}
	n := buildNode(cfg, testHostIP, 10250, t0)
	if n.Labels["cluster.example.org/virtual"] != "true" {
		t.Errorf("extra label missing: %v", n.Labels)
	}
	if n.Labels[v1alpha1.NodeLabel] != testNode || n.Labels["kubernetes.io/os"] != "virtual-kubelet" {
		t.Errorf("nodeLabels replaced the node's own: %v", n.Labels)
	}
}

func TestBuildNode(t *testing.T) {
	n := buildNode(testConfig(), testHostIP, 10250, t0)
	if n.Name != testNode {
		t.Errorf("name %s", n.Name)
	}
	for k, v := range map[string]string{
		"kubernetes.io/hostname": testNode, "kubernetes.io/os": "virtual-kubelet", "kubernetes.io/role": "agent",
		"type": "virtual-kubelet", "node.kubernetes.io/exclude-from-external-load-balancers": "true",
		v1alpha1.NodeLabel: testNode, "offload.interlink.eu/site": "hpc-a",
	} {
		if n.Labels[k] != v {
			t.Errorf("label %s=%q, want %q", k, n.Labels[k], v)
		}
	}
	if len(n.Spec.Taints) != 1 || n.Spec.Taints[0] != (corev1.Taint{Key: v1alpha1.NodeLabel, Value: testNode, Effect: corev1.TaintEffectNoSchedule}) {
		t.Errorf("taints %+v", n.Spec.Taints)
	}
	if len(n.OwnerReferences) != 1 || n.OwnerReferences[0].Kind != "OffloadConfig" || n.OwnerReferences[0].UID != "uid-cfg" || !*n.OwnerReferences[0].Controller {
		t.Errorf("owner %+v", n.OwnerReferences)
	}
	for res, want := range map[corev1.ResourceName]int64{corev1.ResourceCPU: 1, corev1.ResourceMemory: 1 << 30, corev1.ResourcePods: 1000} {
		if q := n.Status.Capacity[res]; q.Value() != want {
			t.Errorf("capacity %s %s, want %d", res, q.String(), want)
		}
		if q := n.Status.Allocatable[res]; q.Value() != want {
			t.Errorf("allocatable %s %s, want %d", res, q.String(), want)
		}
	}
	if len(n.Status.Addresses) != 1 || n.Status.Addresses[0] != (corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: testHostIP}) {
		t.Errorf("addresses %+v, want only the InternalIP", n.Status.Addresses)
	}
	if n.Status.DaemonEndpoints.KubeletEndpoint.Port != 10250 {
		t.Errorf("kubelet port %d", n.Status.DaemonEndpoints.KubeletEndpoint.Port)
	}
	if i := n.Status.NodeInfo; i.KubeletVersion != KubeletVersion || i.OperatingSystem != "linux" || i.Architecture != "amd64" {
		t.Errorf("nodeInfo %+v", i)
	}
	want := map[corev1.NodeConditionType]corev1.ConditionStatus{
		corev1.NodeReady: corev1.ConditionTrue, corev1.NodeMemoryPressure: corev1.ConditionFalse, corev1.NodeDiskPressure: corev1.ConditionFalse,
		corev1.NodePIDPressure: corev1.ConditionFalse, corev1.NodeNetworkUnavailable: corev1.ConditionFalse,
	}
	for _, c := range n.Status.Conditions {
		if want[c.Type] != c.Status {
			t.Errorf("condition %s %s", c.Type, c.Status)
		}
		delete(want, c.Type)
	}
	if len(want) > 0 {
		t.Errorf("missing conditions %v", want)
	}
}

type nodeEnv struct {
	r      *NodeReconciler
	c      client.Client
	kube   *k8sfake.Clientset
	events *events.FakeRecorder
}

func newNodeEnv(t *testing.T, objs ...client.Object) *nodeEnv {
	t.Helper()
	var calls []apiCall
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	e := &nodeEnv{c: fakeClient(t, &calls, objs...), kube: k8sfake.NewClientset(), events: events.NewFakeRecorder(10)}
	// The fake answers every access review allowed=false: the controller may do
	// what it asks, unless a test says otherwise.
	e.kube.PrependReactor("create", "selfsubjectaccessreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authv1.SelfSubjectAccessReview{Status: authv1.SubjectAccessReviewStatus{Allowed: true}}, nil
	})
	e.r = &NodeReconciler{Client: e.c, Kube: e.kube, Recorder: e.events, NodeIP: testHostIP, KubeletPort: 10250,
		ctx: ctx, running: map[string]*runningNode{}, exited: make(chan event.GenericEvent, 1)}
	t.Cleanup(func() { e.r.stop("hpc-a") })
	return e
}

func (e *nodeEnv) reconcile(t *testing.T, name string) {
	t.Helper()
	if _, err := e.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: name}}); err != nil {
		t.Fatal(err)
	}
}

// eventually waits for cond, the library's loops being asynchronous.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// The reconciler registers the node with its labels, taint and owner, the
// library keeps its status Ready and its Lease, and both go with the
// OffloadConfig.
func TestNodeLifecycle(t *testing.T) {
	e := newNodeEnv(t, testConfig())
	e.reconcile(t, "hpc-a")
	n := &corev1.Node{}
	if err := e.c.Get(context.Background(), client.ObjectKey{Name: testNode}, n); err != nil {
		t.Fatal(err)
	}
	if n.Labels[v1alpha1.NodeLabel] != testNode || len(n.Spec.Taints) != 1 || len(n.OwnerReferences) != 1 {
		t.Errorf("node metadata %+v %+v", n.ObjectMeta, n.Spec)
	}
	eventually(t, "the node's Lease", func() bool {
		l, err := e.kube.CoordinationV1().Leases(corev1.NamespaceNodeLease).Get(context.Background(), testNode, metav1.GetOptions{})
		return err == nil && l.Spec.LeaseDurationSeconds != nil && *l.Spec.LeaseDurationSeconds == leaseDurationSeconds
	})
	eventually(t, "the node Ready", func() bool {
		n, err := e.kube.CoreV1().Nodes().Get(context.Background(), testNode, metav1.GetOptions{})
		if err != nil {
			return false
		}
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady {
				return c.Status == corev1.ConditionTrue
			}
		}
		return false
	})

	// A second pass changes nothing and keeps the loop.
	loop := e.r.running["hpc-a"]
	e.reconcile(t, "hpc-a")
	if e.r.running["hpc-a"] != loop {
		t.Error("the node's loop was restarted with nothing changed")
	}

	cfg := testConfig()
	if err := e.c.Delete(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	e.reconcile(t, "hpc-a")
	if err := e.c.Get(context.Background(), client.ObjectKey{Name: testNode}, n); !apierrors.IsNotFound(err) {
		t.Errorf("node still there: %v", err)
	}
	if _, err := e.kube.CoordinationV1().Leases(corev1.NamespaceNodeLease).Get(context.Background(), testNode, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("Lease still there: %v", err)
	}
	if len(e.r.running) != 0 {
		t.Errorf("loops still running: %v", e.r.running)
	}
}

// The node names a provider no cloud controller manager serves: one that finds
// no instance of a node without a providerID deletes it once it is not ready,
// and with it every pod bound to it. A node made before the field was set gets
// it on the next pass.
func TestNodeProviderID(t *testing.T) {
	if got := buildNode(testConfig(), testHostIP, 10250, t0).Spec.ProviderID; got != "external:///"+testNode {
		t.Errorf("providerID %q", got)
	}
	old := buildNode(testConfig(), testHostIP, 10250, t0)
	old.Spec.ProviderID, old.Status = "", corev1.NodeStatus{}
	e := newNodeEnv(t, testConfig(), old)
	e.reconcile(t, "hpc-a")
	n := &corev1.Node{}
	if err := e.c.Get(context.Background(), client.ObjectKey{Name: testNode}, n); err != nil {
		t.Fatal(err)
	}
	if n.Spec.ProviderID != "external:///"+testNode {
		t.Errorf("existing node's providerID %q, want it set", n.Spec.ProviderID)
	}
}

// Writing the node and its status is the deployment's to grant by name: a role
// that is missing, or names another node, gets a Warning event on the
// OffloadConfig instead of failing in the controller's log alone.
func TestNodeAccessWarning(t *testing.T) {
	e := newNodeEnv(t, testConfig())
	var asked []string
	e.kube.PrependReactor("create", "selfsubjectaccessreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
		ra := a.(k8stesting.CreateAction).GetObject().(*authv1.SelfSubjectAccessReview).Spec.ResourceAttributes
		asked = append(asked, ra.Verb+" "+ra.Resource+"/"+ra.Subresource+" "+ra.Name)
		return true, &authv1.SelfSubjectAccessReview{Status: authv1.SubjectAccessReviewStatus{Allowed: ra.Subresource == ""}}, nil
	})
	e.reconcile(t, "hpc-a")
	// the writes the controller and the library make, and no other
	if want := "patch nodes/ " + testNode + ";delete nodes/ " + testNode + ";patch nodes/status " + testNode; strings.Join(asked, ";") != want {
		t.Errorf("access asked %v, want %s", asked, want)
	}
	select {
	case ev := <-e.events.Events:
		if !strings.Contains(ev, "Warning "+EventNodeAccess) || !strings.Contains(ev, "may not patch nodes/status") || !strings.Contains(ev, testNode) {
			t.Errorf("event %q", ev)
		}
	default:
		t.Error("no event for a missing node role")
	}
}

// An OffloadConfig whose node name is no label value gets an event and no node.
func TestNodeNameNotALabelValue(t *testing.T) {
	cfg := testConfig()
	cfg.Name = strings.Repeat("a", 61)
	e := newNodeEnv(t, cfg)
	e.reconcile(t, cfg.Name)
	nodes := &corev1.NodeList{}
	if err := e.c.List(context.Background(), nodes); err != nil || len(nodes.Items) != 0 {
		t.Errorf("nodes %v, %v", nodes.Items, err)
	}
	select {
	case ev := <-e.events.Events:
		if !strings.Contains(ev, EventNoNode) || !strings.Contains(ev, "label value") {
			t.Errorf("event %q", ev)
		}
	default:
		t.Error("no event")
	}
}

// A node of the same name that is not the offload controller's is left alone.
func TestNodeNameTaken(t *testing.T) {
	other := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: testNode, Labels: map[string]string{"team": "gpu"}}}
	e := newNodeEnv(t, testConfig(), other)
	e.reconcile(t, "hpc-a")
	n := &corev1.Node{}
	if err := e.c.Get(context.Background(), client.ObjectKey{Name: testNode}, n); err != nil {
		t.Fatal(err)
	}
	if len(n.Labels) != 1 || len(n.OwnerReferences) != 0 || len(n.Spec.Taints) != 0 {
		t.Errorf("foreign node changed: %+v %+v", n.ObjectMeta, n.Spec)
	}
	if len(e.r.running) != 0 {
		t.Error("a loop runs for a foreign node")
	}
	select {
	case ev := <-e.events.Events:
		if !strings.Contains(ev, "is not the offload controller's") {
			t.Errorf("event %q", ev)
		}
	default:
		t.Error("no event")
	}
	if err := e.c.Delete(context.Background(), testConfig()); err != nil {
		t.Fatal(err)
	}
	e.reconcile(t, "hpc-a")
	if err := e.c.Get(context.Background(), client.ObjectKey{Name: testNode}, n); err != nil {
		t.Errorf("foreign node deleted with the OffloadConfig: %v", err)
	}
}

// A loop that ends, for example on an API server that refuses the node, makes
// the next pass fail, so that the controller backs off, and the pass after
// starts it again.
func TestNodeLoopRestarted(t *testing.T) {
	e := newNodeEnv(t, testConfig())
	var refuse atomic.Bool
	refuse.Store(true)
	e.kube.PrependReactor("*", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if refuse.Load() {
			return true, nil, apierrors.NewForbidden(corev1.Resource("nodes"), testNode, errors.New("denied"))
		}
		return false, nil, nil
	})
	e.reconcile(t, "hpc-a")
	select {
	case <-e.r.exited:
	case <-time.After(10 * time.Second):
		t.Fatal("the loop did not end on a refused node")
	}
	if _, err := e.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "hpc-a"}}); err == nil {
		t.Error("the pass after the loop ended did not fail")
	}
	refuse.Store(false)
	e.reconcile(t, "hpc-a")
	eventually(t, "the node Ready after the restart", func() bool {
		n, err := e.kube.CoreV1().Nodes().Get(context.Background(), testNode, metav1.GetOptions{})
		return err == nil && len(n.Status.Conditions) > 0
	})
}
