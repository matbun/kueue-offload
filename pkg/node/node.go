package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	vknode "github.com/virtual-kubelet/virtual-kubelet/node"
	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// KubeletVersion is the kubelet version the virtual nodes report: the kueue-offload release,
// set at build time (-ldflags -X .../pkg/node.KubeletVersion=<release>).
var KubeletVersion = "dev"

// DefaultKubeletPort is the port of the virtual nodes' kubelet API.
const DefaultKubeletPort = 10250

// leaseDurationSeconds is the duration of a virtual node's Lease, as a
// kubelet's.
const leaseDurationSeconds = 40

// EventNoNode is the event on an OffloadConfig that gets no virtual node.
const EventNoNode = "NoVirtualNode"

// EventNodeAccess is the event on an OffloadConfig whose node the controller may
// not write: the deployment grants that by the node's name.
const EventNodeAccess = "NodeAccess"

// errNodeTaken is returned for a node of the virtual node's name that the
// OffloadConfig does not own and that is not one of the offload controller's.
var errNodeTaken = errors.New("taken")

// NodeReconciler runs the virtual node of each OffloadConfig: it registers the
// node, keeps its status and Lease fresh through the virtual kubelet library,
// and deletes them with the OffloadConfig. It runs on the leader only.
type NodeReconciler struct {
	Client client.Client
	// Kube writes the nodes' status and Leases.
	Kube     kubernetes.Interface
	Recorder events.EventRecorder
	// NodeIP is every virtual node's InternalIP: the API server reaches their
	// kubelet API there, on KubeletPort.
	NodeIP      string
	KubeletPort int32

	mu      sync.Mutex
	ctx     context.Context
	running map[string]*runningNode
	exited  chan event.GenericEvent
}

// runningNode is the virtual kubelet library's loop of one node.
type runningNode struct {
	uid    string
	labels map[string]string
	cancel context.CancelFunc
	done   chan struct{}
	// err is why the loop ended, once done is closed.
	err error
}

// Start holds the nodes' loops for as long as the manager runs, then stops
// them.
func (r *NodeReconciler) Start(ctx context.Context) error {
	r.mu.Lock()
	r.ctx = ctx
	r.mu.Unlock()
	<-ctx.Done()
	r.mu.Lock()
	names := make([]string, 0, len(r.running))
	for name := range r.running {
		names = append(names, name)
	}
	r.mu.Unlock()
	for _, name := range names {
		r.stop(name)
	}
	return nil
}

// SetupWithManager registers the reconciler, which a change to an
// OffloadConfig or to its node triggers, and a node loop that ended.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.running = map[string]*runningNode{}
	r.exited = make(chan event.GenericEvent)
	if err := mgr.Add(r); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("node").
		For(&v1alpha1.OffloadConfig{}).
		Owns(&corev1.Node{}).
		WatchesRawSource(source.Channel(r.exited, &handler.EnqueueRequestForObject{})).
		Complete(r)
}

func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.mu.Lock()
	base := r.ctx
	r.mu.Unlock()
	if base == nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	cfg := &v1alpha1.OffloadConfig{}
	if err := r.Client.Get(ctx, req.NamespacedName, cfg); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		cfg = nil
	}
	name := v1alpha1.NodeName(req.Name)
	if cfg == nil || !cfg.DeletionTimestamp.IsZero() {
		r.stop(req.Name)
		return ctrl.Result{}, r.deleteNode(ctx, name)
	}
	if errs := validation.IsValidLabelValue(name); len(errs) > 0 {
		r.stop(req.Name)
		r.Recorder.Eventf(cfg, nil, corev1.EventTypeWarning, EventNoNode, "Register",
			"no virtual node: its name %s is not a label value: %s", name, strings.Join(errs, "; "))
		return ctrl.Result{}, nil
	}
	if missing, err := r.missingAccess(ctx, name); err != nil {
		return ctrl.Result{}, err
	} else if len(missing) > 0 {
		r.Recorder.Eventf(cfg, nil, corev1.EventTypeWarning, EventNodeAccess, "Register",
			"the offload controller may not %s: grant patch and delete on nodes, and patch on nodes/status, by the name %s",
			strings.Join(missing, ", "), name)
	}
	want := buildNode(cfg, r.NodeIP, r.KubeletPort, time.Now())
	if err := r.applyMetadata(ctx, cfg, want); err != nil {
		if errors.Is(err, errNodeTaken) {
			r.stop(req.Name)
			r.Recorder.Eventf(cfg, nil, corev1.EventTypeWarning, EventNoNode, "Register",
				"no virtual node: node %s exists and is not the offload controller's", name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.ensureRunning(ctx, base, cfg, want)
}

// missingAccess is what the controller may not do to the node name, asked of the
// API server: the node and its status are written by name, which RBAC grants
// only when the deployment names the node.
func (r *NodeReconciler) missingAccess(ctx context.Context, name string) ([]string, error) {
	var missing []string
	for _, a := range []authv1.ResourceAttributes{
		{Verb: "patch", Resource: "nodes"},
		{Verb: "delete", Resource: "nodes"},
		{Verb: "patch", Resource: "nodes", Subresource: "status"},
	} {
		a.Name = name
		review := &authv1.SelfSubjectAccessReview{Spec: authv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &a}}
		got, err := r.Kube.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{})
		if err != nil {
			return nil, err
		}
		if !got.Status.Allowed {
			what := a.Verb + " " + a.Resource
			if a.Subresource != "" {
				what += "/" + a.Subresource
			}
			missing = append(missing, what)
		}
	}
	return missing, nil
}

// buildNode is the virtual node of cfg. Its InternalIP is the only address: the
// API server dials the first address of its preferred types, and a Hostname
// the API server cannot resolve would come first by default.
func buildNode(cfg *v1alpha1.OffloadConfig, nodeIP string, port int32, now time.Time) *corev1.Node {
	name := v1alpha1.NodeName(cfg.Name)
	labels := maps.Clone(cfg.Spec.NodeLabels)
	if labels == nil {
		labels = map[string]string{}
	}
	maps.Copy(labels, map[string]string{
		corev1.LabelHostname:             name,
		corev1.LabelOSStable:             "virtual-kubelet",
		"kubernetes.io/role":             "agent",
		"type":                           "virtual-kubelet",
		corev1.LabelNodeExcludeBalancers: "true",
		v1alpha1.NodeLabel:               name,
	})
	if len(cfg.Spec.Workers) > 0 {
		labels[v1alpha1.SiteLabel] = cfg.Spec.Workers[0]
	}
	at := metav1.NewTime(now.Truncate(time.Second))
	condition := func(ty corev1.NodeConditionType, status corev1.ConditionStatus, reason, msg string) corev1.NodeCondition {
		return corev1.NodeCondition{Type: ty, Status: status, Reason: reason, Message: msg, LastHeartbeatTime: at, LastTransitionTime: at}
	}
	resources := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1"),
		corev1.ResourceMemory: resource.MustParse("1Gi"),
		corev1.ResourcePods:   resource.MustParse("1000"),
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         v1alpha1.GroupVersion.String(),
				Kind:               "OffloadConfig",
				Name:               cfg.Name,
				UID:                cfg.UID,
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(false),
			}},
		},
		Spec: corev1.NodeSpec{
			// A provider no cloud controller manager serves: one that finds no
			// instance for a node deletes it once it is not ready, and with it the
			// pods bound to it.
			ProviderID: "external:///" + name,
			Taints:     []corev1.Taint{{Key: v1alpha1.NodeLabel, Value: name, Effect: corev1.TaintEffectNoSchedule}},
		},
		Status: corev1.NodeStatus{
			Capacity:    resources,
			Allocatable: resources.DeepCopy(),
			Conditions: []corev1.NodeCondition{
				condition(corev1.NodeReady, corev1.ConditionTrue, "KubeletReady", "kubelet is posting ready status"),
				condition(corev1.NodeMemoryPressure, corev1.ConditionFalse, "KubeletHasSufficientMemory", "kubelet has sufficient memory available"),
				condition(corev1.NodeDiskPressure, corev1.ConditionFalse, "KubeletHasNoDiskPressure", "kubelet has no disk pressure"),
				condition(corev1.NodePIDPressure, corev1.ConditionFalse, "KubeletHasSufficientPID", "kubelet has sufficient PID available"),
				condition(corev1.NodeNetworkUnavailable, corev1.ConditionFalse, "VirtualNode", "the pods' networking is the shadows'"),
			},
			Addresses:       []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: nodeIP}},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: port}},
			NodeInfo: corev1.NodeSystemInfo{
				KubeletVersion:  KubeletVersion,
				OperatingSystem: "linux",
				Architecture:    "amd64",
			},
		},
	}
}

// applyMetadata creates want, or brings the labels, the taint and the owner of
// the existing node to want's. The library only writes the status of a node
// that exists.
func (r *NodeReconciler) applyMetadata(ctx context.Context, cfg *v1alpha1.OffloadConfig, want *corev1.Node) error {
	cur := &corev1.Node{}
	if err := r.Client.Get(ctx, client.ObjectKey{Name: want.Name}, cur); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		create := want.DeepCopy()
		create.Status = corev1.NodeStatus{}
		return client.IgnoreAlreadyExists(r.Client.Create(ctx, create))
	}
	owned := false
	for _, ref := range cur.OwnerReferences {
		owned = owned || ref.Kind == "OffloadConfig" && ref.Name == cfg.Name
	}
	if !owned && cur.Labels[v1alpha1.NodeLabel] != want.Name {
		return errNodeTaken
	}
	patched := cur.DeepCopy()
	if patched.Labels == nil {
		patched.Labels = map[string]string{}
	}
	maps.Copy(patched.Labels, want.Labels)
	patched.OwnerReferences = want.OwnerReferences
	taint := want.Spec.Taints[0]
	patched.Spec.Taints = append([]corev1.Taint{taint}, deleteTaint(patched.Spec.Taints, taint.Key)...)
	// Set once: the field cannot change after.
	if patched.Spec.ProviderID == "" {
		patched.Spec.ProviderID = want.Spec.ProviderID
	}
	if equality.Semantic.DeepEqual(cur.Labels, patched.Labels) && equality.Semantic.DeepEqual(cur.OwnerReferences, patched.OwnerReferences) &&
		taintsEqual(cur.Spec.Taints, patched.Spec.Taints) && cur.Spec.ProviderID == patched.Spec.ProviderID {
		return nil
	}
	return r.Client.Patch(ctx, patched, client.MergeFrom(cur))
}

func deleteTaint(taints []corev1.Taint, key string) []corev1.Taint {
	var out []corev1.Taint
	for _, t := range taints {
		if t.Key != key {
			out = append(out, t)
		}
	}
	return out
}

// taintsEqual compares taints as a set, whatever their order.
func taintsEqual(a, b []corev1.Taint) bool {
	if len(a) != len(b) {
		return false
	}
	for _, t := range a {
		found := false
		for _, u := range b {
			found = found || t.Key == u.Key && t.Value == u.Value && t.Effect == u.Effect
		}
		if !found {
			return false
		}
	}
	return true
}

// ensureRunning starts the library's loop of cfg's node unless it runs with the
// same labels: the loop writes the labels it was given with every status
// update.
func (r *NodeReconciler) ensureRunning(ctx, base context.Context, cfg *v1alpha1.OffloadConfig, want *corev1.Node) error {
	r.mu.Lock()
	n := r.running[cfg.Name]
	r.mu.Unlock()
	if n != nil {
		select {
		case <-n.done:
			// The next pass, after the controller's backoff, starts it again.
			r.stop(cfg.Name)
			if n.err == nil {
				n.err = errors.New("no error")
			}
			return fmt.Errorf("virtual node %s: its loop ended: %w", want.Name, n.err)
		default:
			if n.uid == string(cfg.UID) && maps.Equal(n.labels, want.Labels) {
				return nil
			}
		}
		r.stop(cfg.Name)
	}
	leases := r.Kube.CoordinationV1().Leases(corev1.NamespaceNodeLease)
	nc, err := vknode.NewNodeController(nodeProvider{}, want, r.Kube.CoreV1().Nodes(), vknode.WithNodeEnableLeaseV1(leases, leaseDurationSeconds))
	if err != nil {
		return fmt.Errorf("virtual node %s: %w", want.Name, err)
	}
	runCtx, cancel := context.WithCancel(base)
	n = &runningNode{uid: string(cfg.UID), labels: want.Labels, cancel: cancel, done: make(chan struct{})}
	r.mu.Lock()
	r.running[cfg.Name] = n
	r.mu.Unlock()
	logger := log.FromContext(ctx).WithValues("node", want.Name)
	logger.Info("starting the virtual node")
	go func() {
		n.err = nc.Run(runCtx)
		close(n.done)
		if runCtx.Err() != nil {
			return
		}
		logger.Error(n.err, "the virtual node's loop ended")
		select {
		case r.exited <- event.GenericEvent{Object: cfg}:
		case <-runCtx.Done():
		}
	}()
	return nil
}

// stop ends the loop of the node of the OffloadConfig named name, if one runs,
// and waits for it.
func (r *NodeReconciler) stop(name string) {
	r.mu.Lock()
	n := r.running[name]
	delete(r.running, name)
	r.mu.Unlock()
	if n == nil {
		return
	}
	n.cancel()
	select {
	case <-n.done:
	case <-time.After(10 * time.Second):
	}
}

// deleteNode deletes the node named name and its Lease, when the node is one
// of the offload controller's. The garbage collector would, after the
// OffloadConfig.
func (r *NodeReconciler) deleteNode(ctx context.Context, name string) error {
	cur := &corev1.Node{}
	if err := r.Client.Get(ctx, client.ObjectKey{Name: name}, cur); err != nil {
		return client.IgnoreNotFound(err)
	}
	if cur.Labels[v1alpha1.NodeLabel] != name {
		return nil
	}
	if err := r.Client.Delete(ctx, cur, client.Preconditions{UID: &cur.UID}); client.IgnoreNotFound(err) != nil {
		return err
	}
	err := r.Kube.CoordinationV1().Leases(corev1.NamespaceNodeLease).Delete(ctx, name, metav1.DeleteOptions{})
	return client.IgnoreNotFound(err)
}

// nodeProvider is always reachable: the node stands for the controller itself,
// whose liveness probe restarts it when it is not.
type nodeProvider struct{}

func (nodeProvider) Ping(context.Context) error { return nil }

func (nodeProvider) NotifyNodeStatus(context.Context, func(*corev1.Node)) {}
