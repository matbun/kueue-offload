// Package shadow renders and maintains the shadow of an offloaded job: a Deployment
// in the job's namespace that tunnels the job's exposed ports from the cluster to
// the compute node over SSH and streams the job's logs, plus the Service in front
// of the ports and the ConfigMap telling the tunnel which node to reach. It is the
// virtual kubelet's SSH shadow, moved into the executor with its template intact.
package shadow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"strings"
	"sync"

	"github.com/containerd/containerd/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

const (
	// LabelShadow marks every shadow object, and the shadow's pods, with "true".
	LabelShadow = "interlink.eu/shadow"
	// LabelShadowOf carries the UID of the workload a shadow object belongs to.
	LabelShadowOf = "interlink.eu/shadow-of"

	// componentLabel selects the shadow's pods; the template sets it.
	componentLabel = "app.kubernetes.io/component"
	// specHashAnnotation records what a Deployment or Service was rendered from, so
	// that applying the same Spec again writes nothing and restarts nothing.
	specHashAnnotation = "interlink.eu/shadow-spec-hash"
	// nodeAnnotation is set on the shadow's pods when the compute node changes.
	nodeAnnotation = "interlink.eu/shadow-node"
)

// ErrNameTaken is returned by Ensure when an object with a shadow's name exists
// and does not belong to that workload: a previous job of the same name whose
// shadow is not deleted yet, or an object somebody else created.
var ErrNameTaken = errors.New("shadow name taken")

// Spec is what the executor knows of a job when it ensures its shadow.
type Spec struct {
	WorkloadUID  string
	WorkloadName string // the owner's name, base of the shadow's resource names
	Namespace    string
	Template     corev1.PodTemplateSpec // rank 0's template: ports, annotations, labels
	NodeName     string                 // empty until running
	SocketDir    string
	Terminal     bool // job ended: logs-only shadow
	// Reverse is the job's tunnel back into the cluster, nil unless it asked for
	// one (ParseReverseTunnel).
	Reverse *ReverseTunnel
	// Labels are the owner pod's, for a workload of one plain pod. A shadow that
	// tunnels carries those a Service selects the pod by (serviceLabels), so that
	// the Service selects the shadow too.
	Labels map[string]string
}

// Ref names the objects of a shadow, and the IP and readiness of its pod. Service
// is empty when it forwards no port.
type Ref struct {
	Deployment    string `json:"deployment,omitempty"`
	Service       string `json:"service,omitempty"`
	NodeConfigMap string `json:"nodeConfigMap,omitempty"`
	PodIP         string `json:"podIP,omitempty"`
	Ready         bool   `json:"ready,omitempty"`
}

// Manager creates, updates and deletes shadows.
type Manager interface {
	// Ensure creates or updates the shadow for spec: Deployment, Service (only with exposed
	// ports), node ConfigMap with compute-node and socket-dir. Idempotent; called on every poll
	// once the job runs.
	Ensure(ctx context.Context, spec Spec) (Ref, error)
	// Delete removes every object labelled for this workload. Idempotent.
	Delete(ctx context.Context, workloadUID string) error
}

type manager struct {
	client    kubernetes.Interface
	namespace string
	cfg       Config
	// cfgErr is what NormalizeConfig said of cfg; Ensure returns it.
	cfgErr error
	// locks keep an Ensure and a Delete of one workload from interleaving.
	locks [32]sync.Mutex
}

// NewManager returns a Manager for the shadows of namespace. cfg is normalized
// here; an invalid one makes every Ensure fail, so call NormalizeConfig at startup
// to refuse it there.
func NewManager(client kubernetes.Interface, namespace string, cfg Config) Manager {
	err := NormalizeConfig(&cfg)
	return &manager{client: client, namespace: namespace, cfg: cfg, cfgErr: err}
}

func (m *manager) lock(workloadUID string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(workloadUID))
	mu := &m.locks[h.Sum32()%uint32(len(m.locks))]
	mu.Lock()
	return mu.Unlock
}

func ownerLabels(workloadUID string) map[string]string {
	return map[string]string{LabelShadow: "true", LabelShadowOf: workloadUID}
}

func setLabels(meta *metav1.ObjectMeta, add map[string]string) {
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	maps.Copy(meta.Labels, add)
}

func checkOwner(kind string, meta metav1.ObjectMeta, workloadUID string) error {
	if owner := meta.Labels[LabelShadowOf]; owner != workloadUID {
		return fmt.Errorf("%w: %s %s/%s belongs to workload %q, not %q", ErrNameTaken, kind, meta.Namespace, meta.Name, owner, workloadUID)
	}
	return nil
}

func (m *manager) Ensure(ctx context.Context, spec Spec) (Ref, error) {
	if m.cfgErr != nil {
		return Ref{}, fmt.Errorf("invalid shadow configuration: %w", m.cfgErr)
	}
	if spec.Namespace == "" {
		spec.Namespace = m.namespace
	}
	switch {
	case spec.WorkloadName == "":
		return Ref{}, fmt.Errorf("shadow of workload %q: no workload name", spec.WorkloadUID)
	case spec.Namespace != m.namespace:
		return Ref{}, fmt.Errorf("shadow of %s/%s: this executor serves namespace %s only", spec.Namespace, spec.WorkloadName, m.namespace)
	}
	if errs := validation.IsValidLabelValue(spec.WorkloadUID); spec.WorkloadUID == "" || len(errs) > 0 {
		return Ref{}, fmt.Errorf("shadow of %s/%s: invalid workload UID %q", spec.Namespace, spec.WorkloadName, spec.WorkloadUID)
	}
	// The shadow passes both to ssh and to shell commands, so they are refused
	// rather than escaped.
	if spec.NodeName != "" && !isValidComputeNodeName(spec.NodeName) {
		return Ref{}, fmt.Errorf("shadow of %s/%s: refusing compute node %q: not a hostname or IP address", spec.Namespace, spec.WorkloadName, spec.NodeName)
	}
	socketDir := spec.SocketDir
	if socketDir != "" && !isValidSocketDir(socketDir) {
		log.G(ctx).Errorf("Shadow of %s/%s: refusing socket directory %q: not an absolute path of at most %d bytes in [A-Za-z0-9._-]. Publishing the node without it.",
			spec.Namespace, spec.WorkloadName, socketDir, maxSocketDirLen)
		socketDir = ""
	}

	if spec.Reverse != nil && !m.cfg.Reverse.Enabled {
		return Ref{}, fmt.Errorf("shadow of %s/%s: a reverse tunnel, but Shadow.Reverse.Enabled is false", spec.Namespace, spec.WorkloadName)
	}
	ports := exposedPorts(spec.Template)
	forwards := hasTCPPort(ports)
	tunnels := forwards || spec.Reverse != nil
	if !tunnels && !m.cfg.alwaysShadow() {
		return Ref{}, nil
	}
	logsOnly := spec.Terminal || !tunnels
	if !logsOnly && m.cfg.LoginHost == "" {
		return Ref{}, fmt.Errorf("shadow of %s/%s: Shadow.LoginHost is empty, so the shadow streams logs only and cannot tunnel", spec.Namespace, spec.WorkloadName)
	}
	if !logsOnly {
		if err := markSocketPorts(spec.Template.Annotations[annSocketPorts], m.cfg.ForwardTarget, ports); err != nil {
			return Ref{}, fmt.Errorf("shadow of %s/%s: %w", spec.Namespace, spec.WorkloadName, err)
		}
	}

	var err error
	if !logsOnly {
		if spec.Labels, err = m.serviceLabels(ctx, spec.Labels); err != nil {
			return Ref{}, err
		}
	}
	name := resourceName(spec.WorkloadName, spec.Namespace)
	deployment, service, err := render(m.cfg, spec, name, ports, logsOnly)
	if err != nil {
		return Ref{}, err
	}

	defer m.lock(spec.WorkloadUID)()
	// The ConfigMap goes first: the Deployment's pods mount it.
	if err := m.applyNodeConfigMap(ctx, spec.WorkloadUID, name, spec.NodeName, socketDir); err != nil {
		return Ref{}, err
	}
	if err := m.applyDeployment(ctx, spec.WorkloadUID, deployment); err != nil {
		return Ref{}, err
	}
	ref := Ref{Deployment: name, NodeConfigMap: nodeConfigMapName(name)}
	if service != nil {
		if err := m.applyService(ctx, spec.WorkloadUID, service); err != nil {
			return Ref{}, err
		}
		ref.Service = name
	} else if err := m.deleteService(ctx, spec.WorkloadUID, name); err != nil {
		return Ref{}, err
	}
	if ref.PodIP, ref.Ready, err = m.podState(ctx, spec.WorkloadUID, name); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

// kueueLabelPrefix marks Kueue's labels. Kueue gates a pod that carries its queue
// label, so the shadow carries none of them.
const kueueLabelPrefix = "kueue.x-k8s.io/"

// serviceLabels are the labels by which the namespace's Services select a pod
// labelled pod, less Kueue's. The shadow carries no other label of the pod: a
// PodDefault or a policy that selects the pod by one would change the shadow too.
func (m *manager) serviceLabels(ctx context.Context, pod map[string]string) (map[string]string, error) {
	if len(pod) == 0 {
		return nil, nil
	}
	list, err := m.client.CoreV1().Services(m.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list the services of %s: %w", m.namespace, err)
	}
	out := map[string]string{}
	for _, svc := range list.Items {
		selector := svc.Spec.Selector
		if len(selector) == 0 || !labels.SelectorFromSet(selector).Matches(labels.Set(pod)) {
			continue
		}
		for k, v := range selector {
			if !strings.HasPrefix(k, kueueLabelPrefix) {
				out[k] = v
			}
		}
	}
	return out, nil
}

// podState is the IP and readiness of the shadow's pod: a ready one if there is
// one, else one with an IP. Pods being deleted do not count.
func (m *manager) podState(ctx context.Context, workloadUID, name string) (string, bool, error) {
	list, err := m.client.CoreV1().Pods(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{componentLabel: name, LabelShadowOf: workloadUID}.String(),
	})
	if err != nil {
		return "", false, fmt.Errorf("failed to list the pods of shadow %s/%s: %w", m.namespace, name, err)
	}
	ip := ""
	for _, pod := range list.Items {
		if pod.DeletionTimestamp != nil || pod.Status.PodIP == "" {
			continue
		}
		if podReady(&pod) {
			return pod.Status.PodIP, true, nil
		}
		if ip == "" {
			ip = pod.Status.PodIP
		}
	}
	return ip, false, nil
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// applyNodeConfigMap publishes the compute node and the socket directory. Both keys
// go in one write, so the shadow never sees a new node with the previous job's
// directory. The ConfigMap is mounted as a directory, which the kubelet refreshes
// in place: a node change rebuilds the tunnel without restarting the shadow.
func (m *manager) applyNodeConfigMap(ctx context.Context, workloadUID, name, nodeName, socketDir string) error {
	configMaps := m.client.CoreV1().ConfigMaps(m.namespace)
	cmName := nodeConfigMapName(name)
	data := map[string]string{nodeNameKey: nodeName, socketDirKey: socketDir}

	current, err := configMaps.Get(ctx, cmName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: cmName, Namespace: m.namespace, Labels: ownerLabels(workloadUID)},
			Data:       data,
		}
		if _, err := configMaps.Create(ctx, cm, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create shadow configmap %s/%s: %w", m.namespace, cmName, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read shadow configmap %s/%s: %w", m.namespace, cmName, err)
	}
	if err := checkOwner("configmap", current.ObjectMeta, workloadUID); err != nil {
		return err
	}
	if maps.Equal(current.Data, data) {
		return nil
	}
	previous := current.Data[nodeNameKey]
	current.Data = data
	// The update carries the resourceVersion read, so it fails rather than
	// overwrite a change made meanwhile; the next poll tries again.
	if _, err := configMaps.Update(ctx, current, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update shadow configmap %s/%s: %w", m.namespace, cmName, err)
	}
	log.G(ctx).Infof("Published compute node %q, socket directory %q for shadow %s/%s", nodeName, socketDir, m.namespace, name)
	if nodeName != "" && nodeName != previous {
		m.touchPods(ctx, workloadUID, name, nodeName)
	}
	return nil
}

// touchPods annotates the shadow's pods with the published node. The kubelet
// refreshes a ConfigMap volume on its next sync of the pod, up to a minute later;
// a change to the pod makes it sync, and the shadow sees the node within a second.
// Best effort: without it the shadow only learns the node later.
func (m *manager) touchPods(ctx context.Context, workloadUID, name, nodeName string) {
	pods := m.client.CoreV1().Pods(m.namespace)
	list, err := pods.List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{componentLabel: name, LabelShadowOf: workloadUID}.String(),
	})
	if err != nil {
		log.G(ctx).Warningf("Failed to list the pods of shadow %s/%s: %v", m.namespace, name, err)
		return
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]string{nodeAnnotation: nodeName}}})
	if err != nil {
		return
	}
	for _, pod := range list.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if _, err := pods.Patch(ctx, pod.Name, k8stypes.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			log.G(ctx).Warningf("Failed to annotate shadow pod %s/%s with its node: %v", m.namespace, pod.Name, err)
		}
	}
}

// specHash fingerprints what an object was rendered from. encoding/json writes map
// keys sorted, so equal inputs give equal hashes.
func specHash(meta metav1.ObjectMeta, spec any) (string, error) {
	b, err := json.Marshal(struct {
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
		Spec        any               `json:"spec"`
	}{meta.Labels, meta.Annotations, spec})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16]), nil
}

func stampHash(meta *metav1.ObjectMeta, spec any) (string, error) {
	hash, err := specHash(*meta, spec)
	if err != nil {
		return "", err
	}
	if meta.Annotations == nil {
		meta.Annotations = map[string]string{}
	}
	meta.Annotations[specHashAnnotation] = hash
	return hash, nil
}

// applyDeployment creates the Deployment, or replaces it when what it was rendered
// from changed. An unchanged one is not written, so its pod is not restarted.
func (m *manager) applyDeployment(ctx context.Context, workloadUID string, desired *appsv1.Deployment) error {
	hash, err := stampHash(&desired.ObjectMeta, desired.Spec)
	if err != nil {
		return err
	}
	deployments := m.client.AppsV1().Deployments(m.namespace)
	current, err := deployments.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := deployments.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create shadow deployment %s/%s: %w", m.namespace, desired.Name, err)
		}
		log.G(ctx).Infof("Created shadow deployment %s/%s", m.namespace, desired.Name)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read shadow deployment %s/%s: %w", m.namespace, desired.Name, err)
	}
	if err := checkOwner("deployment", current.ObjectMeta, workloadUID); err != nil {
		return err
	}
	if current.Annotations[specHashAnnotation] == hash {
		return nil
	}
	desired.ResourceVersion = current.ResourceVersion
	if _, err := deployments.Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update shadow deployment %s/%s: %w", m.namespace, desired.Name, err)
	}
	log.G(ctx).Infof("Updated shadow deployment %s/%s", m.namespace, desired.Name)
	return nil
}

func (m *manager) applyService(ctx context.Context, workloadUID string, desired *corev1.Service) error {
	hash, err := stampHash(&desired.ObjectMeta, desired.Spec)
	if err != nil {
		return err
	}
	services := m.client.CoreV1().Services(m.namespace)
	current, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := services.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create shadow service %s/%s: %w", m.namespace, desired.Name, err)
		}
		log.G(ctx).Infof("Created shadow service %s/%s", m.namespace, desired.Name)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read shadow service %s/%s: %w", m.namespace, desired.Name, err)
	}
	if err := checkOwner("service", current.ObjectMeta, workloadUID); err != nil {
		return err
	}
	if current.Annotations[specHashAnnotation] == hash {
		return nil
	}
	desired.ResourceVersion = current.ResourceVersion
	desired.Spec.ClusterIP = current.Spec.ClusterIP
	desired.Spec.ClusterIPs = current.Spec.ClusterIPs
	if _, err := services.Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update shadow service %s/%s: %w", m.namespace, desired.Name, err)
	}
	log.G(ctx).Infof("Updated shadow service %s/%s", m.namespace, desired.Name)
	return nil
}

// deleteService removes the workload's Service from a shadow that no longer
// forwards anything. A Service of that name belonging to anything else stays.
func (m *manager) deleteService(ctx context.Context, workloadUID, name string) error {
	services := m.client.CoreV1().Services(m.namespace)
	current, err := services.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read shadow service %s/%s: %w", m.namespace, name, err)
	}
	if current.Labels[LabelShadowOf] != workloadUID {
		return nil
	}
	uid := current.UID
	err = services.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete shadow service %s/%s: %w", m.namespace, name, err)
	}
	log.G(ctx).Infof("Deleted shadow service %s/%s: the shadow forwards nothing any more", m.namespace, name)
	return nil
}

func (m *manager) Delete(ctx context.Context, workloadUID string) error {
	if workloadUID == "" || len(validation.IsValidLabelValue(workloadUID)) > 0 {
		return fmt.Errorf("invalid workload UID %q", workloadUID)
	}
	defer m.lock(workloadUID)()

	selector := metav1.ListOptions{LabelSelector: labels.Set(ownerLabels(workloadUID)).String()}
	background := metav1.DeletePropagationBackground
	opts := metav1.DeleteOptions{PropagationPolicy: &background}
	var errs []error
	collect := func(kind, name string, err error) {
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("failed to delete shadow %s %s/%s: %w", kind, m.namespace, name, err))
		}
	}

	deployments := m.client.AppsV1().Deployments(m.namespace)
	if list, err := deployments.List(ctx, selector); err != nil {
		errs = append(errs, fmt.Errorf("failed to list shadow deployments: %w", err))
	} else {
		for _, o := range list.Items {
			collect("deployment", o.Name, deployments.Delete(ctx, o.Name, opts))
		}
	}
	services := m.client.CoreV1().Services(m.namespace)
	if list, err := services.List(ctx, selector); err != nil {
		errs = append(errs, fmt.Errorf("failed to list shadow services: %w", err))
	} else {
		for _, o := range list.Items {
			collect("service", o.Name, services.Delete(ctx, o.Name, opts))
		}
	}
	configMaps := m.client.CoreV1().ConfigMaps(m.namespace)
	if list, err := configMaps.List(ctx, selector); err != nil {
		errs = append(errs, fmt.Errorf("failed to list shadow configmaps: %w", err))
	} else {
		for _, o := range list.Items {
			collect("configmap", o.Name, configMaps.Delete(ctx, o.Name, opts))
		}
	}
	if len(errs) == 0 {
		log.G(ctx).Infof("Deleted the shadow of workload %s in %s", workloadUID, m.namespace)
	}
	return errors.Join(errs...)
}
