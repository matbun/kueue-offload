package shadow

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

// sha256 of pkg/virtualkubelet/templates/shadow-ssh-template.yaml in interLink.
const upstreamTemplateSHA256 = "6f713de0bd899fc1e2e7c7d428ab94f92444374beb6de9a2d53d7610ec524f10"

const (
	testNamespace = "alice"
	testUID       = "0b6c1d2e-3f40-4a5b-8c6d-7e8f9a0b1c2d"
	testName      = "nb"
	testShadow    = "shadow-nb-alice"
	testSocketDir = "/scratch/alice/interlink/sockets/abc"
)

func testConfig() Config {
	return Config{
		ExecutorImage:       "registry.example.org/kueue-offload-executor:1.0",
		ExecutorURL:         "http://kueue-offload-executor:8000",
		LoginHost:           "login.hpc-a.example.org",
		User:                "alice",
		KeySecret:           "hpc-a-tunnel-key",
		KnownHostsConfigMap: "hpc-a-known-hosts",
		ForwardTarget:       ForwardTargetNode,
	}
}

func podTemplate(ports ...int32) corev1.PodTemplateSpec {
	c := corev1.Container{Name: "main", Image: "jupyter/base-notebook"}
	for _, p := range ports {
		c.Ports = append(c.Ports, corev1.ContainerPort{ContainerPort: p})
	}
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{"kueue.x-k8s.io/queue-name": "proj42"},
			Annotations: map[string]string{},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{c}},
	}
}

func notebookSpec() Spec {
	return Spec{
		WorkloadUID:  testUID,
		WorkloadName: testName,
		Namespace:    testNamespace,
		Template:     podTemplate(8888),
		NodeName:     "node07",
		SocketDir:    testSocketDir,
	}
}

func newTestManager(t *testing.T, cfg Config, objects ...runtime.Object) (*fake.Clientset, Manager) {
	t.Helper()
	client := fake.NewClientset(objects...)
	if err := NormalizeConfig(&cfg); err != nil {
		t.Fatalf("test config: %v", err)
	}
	return client, NewManager(client, testNamespace, cfg)
}

func getDeployment(t *testing.T, client *fake.Clientset, name string) *appsv1.Deployment {
	t.Helper()
	d, err := client.AppsV1().Deployments(testNamespace).Get(t.Context(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment %s: %v", name, err)
	}
	return d
}

func containerNames(cs []corev1.Container) []string {
	var names []string
	for _, c := range cs {
		names = append(names, c.Name)
	}
	return names
}

func container(t *testing.T, d *appsv1.Deployment, name string) corev1.Container {
	t.Helper()
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no container %s in %v", name, containerNames(d.Spec.Template.Spec.Containers))
	return corev1.Container{}
}

func assertOwnerLabels(t *testing.T, kind string, labels map[string]string, uid string) {
	t.Helper()
	if labels[LabelShadow] != "true" || labels[LabelShadowOf] != uid {
		t.Errorf("%s labels %v: want %s=true and %s=%s", kind, labels, LabelShadow, LabelShadowOf, uid)
	}
}

// writes returns the actions other than get and list.
func writes(client *fake.Clientset) []string {
	var out []string
	for _, a := range client.Actions() {
		if a.GetVerb() == "get" || a.GetVerb() == "list" {
			continue
		}
		out = append(out, a.GetVerb()+" "+a.GetResource().Resource)
	}
	return out
}

func TestTemplateIsTheVirtualKubelets(t *testing.T) {
	sum := sha256.Sum256([]byte(sshTemplate))
	if got := hex.EncodeToString(sum[:]); got != upstreamTemplateSHA256 {
		t.Fatalf("templates/shadow-ssh-template.yaml has sha256 %s, want %s: it must stay byte-identical to the virtual kubelet's", got, upstreamTemplateSHA256)
	}
}

func TestEnsureNotebook(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	ref, err := m.Ensure(t.Context(), notebookSpec())
	if err != nil {
		t.Fatal(err)
	}
	want := Ref{Deployment: testShadow, Service: testShadow, NodeConfigMap: testShadow + "-node"}
	if ref != want {
		t.Fatalf("ref %+v, want %+v", ref, want)
	}

	d := getDeployment(t, client, testShadow)
	assertOwnerLabels(t, "deployment", d.Labels, testUID)
	assertOwnerLabels(t, "pod template", d.Spec.Template.Labels, testUID)
	if _, ok := d.Spec.Template.Labels["kueue.x-k8s.io/queue-name"]; ok {
		t.Error("the shadow must not carry the workload's queue label")
	}
	if got := containerNames(d.Spec.Template.Spec.InitContainers); !slices.Equal(got, []string{"wait-for-node"}) {
		t.Errorf("init containers %v", got)
	}
	if got := containerNames(d.Spec.Template.Spec.Containers); !slices.Equal(got, []string{"ssh-forward", "job-logs"}) {
		t.Errorf("containers %v", got)
	}

	forward := container(t, d, "ssh-forward")
	script := forward.Args[0]
	for _, s := range []string{`-L "0.0.0.0:8888:127.0.0.1:8888"`, "target=alice@login.hpc-a.example.org", "-i /interlink/ssh/ssh-privatekey"} {
		if !strings.Contains(script, s) {
			t.Errorf("ssh-forward script lacks %q", s)
		}
	}
	if forward.ReadinessProbe == nil || forward.ReadinessProbe.TCPSocket.Port.IntVal != 8888 {
		t.Errorf("readiness probe %+v, want tcp 8888", forward.ReadinessProbe)
	}

	logs := container(t, d, "job-logs")
	wantCmd := []string{"/executor", "logs", "--executor", "http://kueue-offload-executor:8000", "--workload", testUID, "--follow"}
	if !slices.Equal(logs.Command, wantCmd) {
		t.Errorf("job-logs command %v, want %v", logs.Command, wantCmd)
	}
	if logs.Image != "registry.example.org/kueue-offload-executor:1.0" {
		t.Errorf("job-logs image %s", logs.Image)
	}
	r := logs.Resources
	if r.Requests.Cpu().String() != "10m" || r.Requests.Memory().String() != "32Mi" ||
		r.Limits.Cpu().String() != "100m" || r.Limits.Memory().String() != "64Mi" {
		t.Errorf("job-logs resources %+v", r)
	}

	var volumes []string
	for _, v := range d.Spec.Template.Spec.Volumes {
		volumes = append(volumes, v.Name)
	}
	if !slices.Equal(volumes, []string{"shadow-node", "shadow-ssh-key", "shadow-known-hosts"}) {
		t.Errorf("volumes %v", volumes)
	}
	if got := d.Spec.Template.Spec.Volumes[0].ConfigMap.Name; got != testShadow+"-node" {
		t.Errorf("node volume mounts %s", got)
	}

	svc, err := client.CoreV1().Services(testNamespace).Get(t.Context(), testShadow, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertOwnerLabels(t, "service", svc.Labels, testUID)
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 8888 || svc.Spec.Ports[0].TargetPort.IntVal != 8888 {
		t.Errorf("service ports %+v", svc.Spec.Ports)
	}
	if svc.Spec.Selector["app.kubernetes.io/component"] != testShadow {
		t.Errorf("service selector %v", svc.Spec.Selector)
	}

	cm, err := client.CoreV1().ConfigMaps(testNamespace).Get(t.Context(), testShadow+"-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertOwnerLabels(t, "configmap", cm.Labels, testUID)
	if cm.Data["compute-node"] != "node07" || cm.Data["socket-dir"] != testSocketDir {
		t.Errorf("configmap data %v", cm.Data)
	}
}

func assertLogsOnly(t *testing.T, client *fake.Clientset, ref Ref) {
	t.Helper()
	if ref.Service != "" {
		t.Errorf("logs-only ref names service %q", ref.Service)
	}
	d := getDeployment(t, client, testShadow)
	if got := containerNames(d.Spec.Template.Spec.Containers); !slices.Equal(got, []string{"job-logs"}) {
		t.Errorf("logs-only containers %v", got)
	}
	if len(d.Spec.Template.Spec.InitContainers) != 0 {
		t.Errorf("logs-only init containers %v", containerNames(d.Spec.Template.Spec.InitContainers))
	}
	// Nothing mounts the tunnel key, so a missing Secret cannot hold the pod.
	if len(d.Spec.Template.Spec.Volumes) != 0 {
		t.Errorf("logs-only volumes %+v", d.Spec.Template.Spec.Volumes)
	}
	assertOwnerLabels(t, "pod template", d.Spec.Template.Labels, testUID)
	_, err := client.CoreV1().Services(testNamespace).Get(t.Context(), testShadow, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("logs-only shadow has a service (err %v)", err)
	}
}

func TestEnsureWithoutPortsIsLogsOnly(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.Template = podTemplate()
	ref, err := m.Ensure(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	assertLogsOnly(t, client, ref)
	if ref.NodeConfigMap != testShadow+"-node" {
		t.Errorf("ref %+v", ref)
	}
}

func TestEnsureUDPOnlyIsLogsOnly(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.Template = podTemplate()
	spec.Template.Annotations[annExtraPorts] = "5353:dns:UDP"
	ref, err := m.Ensure(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	assertLogsOnly(t, client, ref)
}

func TestAlwaysShadowFalse(t *testing.T) {
	cfg := testConfig()
	never := false
	cfg.AlwaysShadow = &never
	client, m := newTestManager(t, cfg)

	spec := notebookSpec()
	spec.Template = podTemplate()
	ref, err := m.Ensure(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if ref != (Ref{}) || len(writes(client)) != 0 {
		t.Fatalf("a job without ports got a shadow: ref %+v, writes %v", ref, writes(client))
	}

	if _, err := m.Ensure(t.Context(), notebookSpec()); err != nil {
		t.Fatal(err)
	}
	getDeployment(t, client, testShadow)
}

func TestTerminalFlipsToLogsOnly(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	spec.Terminal = true
	ref, err := m.Ensure(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	assertLogsOnly(t, client, ref)
	if ref.Deployment != testShadow {
		t.Errorf("ref %+v", ref)
	}
}

// notebookPodLabels are a Kubeflow Notebook's pod as Kueue holds it gated: its
// StatefulSet's, the spawner's PodDefault selector, Kueue's, and its
// controller's.
func notebookPodLabels() map[string]string {
	return map[string]string{
		"statefulset": testName, "notebook-name": testName, "hpc-a-proj42": "true",
		"kueue.x-k8s.io/queue-name": "hpc-a-proj42", "kueue.x-k8s.io/managed": "true",
		"controller-revision-hash": testName + "-5d565", "statefulset.kubernetes.io/pod-name": testName + "-0",
		"app.kubernetes.io/component": "jupyter",
	}
}

func service(name string, selector map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec:       corev1.ServiceSpec{Selector: selector},
	}
}

// The shadow carries the labels the namespace's Services select the pod by, so
// they select the shadow too, and no other: a PodDefault selecting hpc-a-proj42
// would give the shadow Kueue's queue label, and Kueue would gate it.
func TestShadowCarriesServiceLabels(t *testing.T) {
	client, m := newTestManager(t, testConfig(),
		service(testName, map[string]string{"statefulset": testName}),
		service("per-pod", map[string]string{"statefulset.kubernetes.io/pod-name": testName + "-0", "kueue.x-k8s.io/managed": "true"}),
		service("component", map[string]string{"app.kubernetes.io/component": "jupyter"}),
		service("other", map[string]string{"statefulset": "other", "notebook-name": testName}),
		service("headless", nil),
	)
	spec := notebookSpec()
	spec.Labels = notebookPodLabels()
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	d := getDeployment(t, client, testShadow)
	want := map[string]string{
		"statefulset": testName, "statefulset.kubernetes.io/pod-name": testName + "-0",
		"app.kubernetes.io/component": testShadow, LabelShadow: "true", LabelShadowOf: testUID,
	}
	if !reflect.DeepEqual(d.Spec.Template.Labels, want) {
		t.Errorf("pod template labels %v, want %v", d.Spec.Template.Labels, want)
	}
	if !reflect.DeepEqual(d.Spec.Selector.MatchLabels, map[string]string{"app.kubernetes.io/component": testShadow}) {
		t.Errorf("selector %v", d.Spec.Selector.MatchLabels)
	}
	if _, ok := d.Labels["statefulset"]; ok {
		t.Errorf("the Deployment itself carries the pod's labels: %v", d.Labels)
	}

	// Ended, the shadow forwards nothing: no Service may select it any more.
	spec.Terminal = true
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	d = getDeployment(t, client, testShadow)
	if _, ok := d.Spec.Template.Labels["statefulset"]; ok {
		t.Errorf("the logs-only shadow carries the pod's labels: %v", d.Spec.Template.Labels)
	}
}

// Without a pod of its own, the shadow stands for rank 0's pod by its template:
// a RayCluster's head template carries the labels KubeRay's head Service
// <name>-head-svc selects. The shadow carries none of them, which KubeRay counts
// its pods by, and is published to that Service with an EndpointSlice of the
// ports it forwards; a Service it forwards no port of gets none.
func TestShadowPublishedToServicesOfTemplate(t *testing.T) {
	head := map[string]string{
		"ray.io/cluster": testName, "ray.io/node-type": "head", "ray.io/identifier": testName + "-head",
		"app.kubernetes.io/name": "kuberay", "app.kubernetes.io/created-by": "kuberay-operator",
	}
	headSvc := service(testName+"-head-svc", head)
	headSvc.Spec.ClusterIP, headSvc.Spec.PublishNotReadyAddresses = corev1.ClusterIPNone, true
	headSvc.Spec.Ports = []corev1.ServicePort{
		{Name: "client", Port: 10001}, {Name: "dashboard", Port: 8265}, {Name: "gcs-server", Port: 6379},
		{Name: "metrics", Port: 8080, TargetPort: intstr.FromString("metrics")},
	}
	metricsOnly := service("metrics", map[string]string{"ray.io/cluster": testName})
	metricsOnly.Spec.Ports = []corev1.ServicePort{{Name: "metrics", Port: 8080}}
	shadowPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: testShadow + "-abc", Namespace: testNamespace, UID: "pod-uid",
			Labels: map[string]string{"app.kubernetes.io/component": testShadow, LabelShadowOf: testUID}},
		Spec:   corev1.PodSpec{NodeName: "worker-3"},
		Status: corev1.PodStatus{PodIP: "10.0.0.7"},
	}
	client, m := newTestManager(t, testConfig(), headSvc, metricsOnly,
		service("workers", map[string]string{"ray.io/cluster": testName, "ray.io/node-type": "worker"}),
		shadowPod,
	)
	spec := notebookSpec()
	spec.Template = podTemplate(8265, 10001)
	maps.Copy(spec.Template.Labels, head)
	spec.Template.Labels["ray.io/group"] = "headgroup"
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"app.kubernetes.io/component": testShadow, LabelShadow: "true", LabelShadowOf: testUID}
	if got := getDeployment(t, client, testShadow).Spec.Template.Labels; !reflect.DeepEqual(got, want) {
		t.Errorf("pod template labels %v, want %v", got, want)
	}
	published := endpointSlices(t, client)
	if len(published) != 1 {
		t.Fatalf("endpoint slices %v, want one for the head Service", published)
	}
	es := published[0]
	if es.Name != testShadow+"-"+headSvc.Name || es.Labels["kubernetes.io/service-name"] != headSvc.Name ||
		es.Labels["endpointslice.kubernetes.io/managed-by"] != endpointSliceManager || es.Labels[LabelShadowOf] != testUID {
		t.Errorf("endpoint slice %s labels %v", es.Name, es.Labels)
	}
	var ports []string
	for _, p := range es.Ports {
		ports = append(ports, fmt.Sprintf("%s:%d", *p.Name, *p.Port))
	}
	if !slices.Equal(ports, []string{"client:10001", "dashboard:8265"}) {
		t.Errorf("ports %v, want those the shadow forwards", ports)
	}
	ep := es.Endpoints[0]
	if es.AddressType != "IPv4" || len(es.Endpoints) != 1 || !slices.Equal(ep.Addresses, []string{"10.0.0.7"}) || ep.TargetRef.Name != shadowPod.Name ||
		*ep.NodeName != "worker-3" || !*ep.Conditions.Ready || *ep.Conditions.Serving {
		t.Errorf("endpoint %+v, want the shadow pod, ready as the Service publishes not ready addresses, not serving", ep)
	}

	// Unchanged, nothing is written; a new pod IP is.
	client.ClearActions()
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if w := writes(client); len(w) != 0 {
		t.Fatalf("re-applying the same spec wrote %v", w)
	}
	shadowPod.Status.PodIP = "10.0.0.8"
	if _, err := client.CoreV1().Pods(testNamespace).UpdateStatus(t.Context(), shadowPod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if w := writes(client); !slices.Equal(w, []string{"update endpointslices"}) {
		t.Errorf("writes %v, want the endpoint slice updated", w)
	}
	if got := endpointSlices(t, client)[0].Endpoints[0].Addresses; !slices.Equal(got, []string{"10.0.0.8"}) {
		t.Errorf("addresses %v after the pod IP changed", got)
	}

	// Ended, the shadow forwards nothing: its endpoint slices go.
	spec.Terminal = true
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if got := endpointSlices(t, client); len(got) != 0 {
		t.Errorf("endpoint slices %v left on a logs-only shadow", got)
	}

	// The owner pod's labels, when there is one, are what Services select.
	spec.Terminal = false
	spec.Labels = map[string]string{"statefulset": testName}
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if got := endpointSlices(t, client); len(got) != 0 {
		t.Errorf("endpoint slices %v for a shadow of an owner pod", got)
	}

	spec.Labels = nil
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if err := m.Delete(t.Context(), testUID); err != nil {
		t.Fatal(err)
	}
	if got := endpointSlices(t, client); len(got) != 0 {
		t.Errorf("endpoint slices %v left after Delete", got)
	}
}

func endpointSlices(t *testing.T, client *fake.Clientset) []discoveryv1.EndpointSlice {
	t.Helper()
	list, err := client.DiscoveryV1().EndpointSlices(testNamespace).List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return list.Items
}

func TestEnsureReportsShadowPod(t *testing.T) {
	pod := func(name, ip string, ready, deleting bool) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace,
				Labels: map[string]string{"app.kubernetes.io/component": testShadow, LabelShadowOf: testUID}},
			Status: corev1.PodStatus{PodIP: ip},
		}
		status := corev1.ConditionFalse
		if ready {
			status = corev1.ConditionTrue
		}
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}
		if deleting {
			p.DeletionTimestamp = &metav1.Time{}
			p.Finalizers = []string{"example.org/hold"}
		}
		return p
	}
	cases := []struct {
		name  string
		pods  []runtime.Object
		ip    string
		ready bool
	}{
		{"no pod yet", nil, "", false},
		{"starting", []runtime.Object{pod("a", "", false, false)}, "", false},
		{"not ready", []runtime.Object{pod("a", "10.0.0.1", false, false)}, "10.0.0.1", false},
		{"a ready one wins", []runtime.Object{pod("a", "10.0.0.1", false, false), pod("b", "10.0.0.2", true, false)}, "10.0.0.2", true},
		{"deleting ones do not count", []runtime.Object{pod("a", "10.0.0.1", true, true), pod("b", "10.0.0.2", false, false)}, "10.0.0.2", false},
		{"another workload's", []runtime.Object{func() *corev1.Pod {
			p := pod("a", "10.0.0.1", true, false)
			p.Labels[LabelShadowOf] = "another"
			return p
		}()}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, m := newTestManager(t, testConfig(), tc.pods...)
			ref, err := m.Ensure(t.Context(), notebookSpec())
			if err != nil {
				t.Fatal(err)
			}
			if ref.PodIP != tc.ip || ref.Ready != tc.ready {
				t.Errorf("pod %q ready %v, want %q %v", ref.PodIP, ref.Ready, tc.ip, tc.ready)
			}
		})
	}
}

func TestEnsureTwiceWritesNothing(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	// Two containers and the extra-ports annotation: the order the ports are
	// gathered in must not change the rendering.
	spec.Template.Spec.Containers = append(spec.Template.Spec.Containers, corev1.Container{
		Name: "metrics", Ports: []corev1.ContainerPort{{ContainerPort: 9100}, {ContainerPort: 6006}},
	})
	spec.Template.Annotations[annExtraPorts] = "7000:api,5353:dns:UDP"
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	before := getDeployment(t, client, testShadow)
	client.ClearActions()

	for range 3 {
		if _, err := m.Ensure(t.Context(), spec); err != nil {
			t.Fatal(err)
		}
	}
	if w := writes(client); len(w) != 0 {
		t.Fatalf("re-applying the same spec wrote %v", w)
	}
	if after := getDeployment(t, client, testShadow); after.Annotations[specHashAnnotation] != before.Annotations[specHashAnnotation] {
		t.Fatal("the deployment changed")
	}

	svc, err := client.CoreV1().Services(testNamespace).Get(t.Context(), testShadow, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var ports []int32
	for _, p := range svc.Spec.Ports {
		ports = append(ports, p.Port)
	}
	if !slices.Equal(ports, []int32{6006, 7000, 8888, 9100}) {
		t.Errorf("service ports %v, want the TCP ports sorted", ports)
	}
}

func TestNodeChangeUpdatesConfigMapOnly(t *testing.T) {
	shadowPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: testShadow + "-abc", Namespace: testNamespace,
		Labels: map[string]string{"app.kubernetes.io/component": testShadow, LabelShadowOf: testUID},
	}}
	client, m := newTestManager(t, testConfig(), shadowPod)
	spec := notebookSpec()
	spec.NodeName, spec.SocketDir = "", ""
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	hash := getDeployment(t, client, testShadow).Annotations[specHashAnnotation]

	for _, node := range []string{"node07", "node08"} {
		client.ClearActions()
		spec.NodeName, spec.SocketDir = node, testSocketDir+"-"+node
		if _, err := m.Ensure(t.Context(), spec); err != nil {
			t.Fatal(err)
		}
		if w := writes(client); !slices.Equal(w, []string{"update configmaps", "patch pods"}) {
			t.Fatalf("node %s: writes %v, want the configmap updated and the shadow pod touched", node, w)
		}
		cm, err := client.CoreV1().ConfigMaps(testNamespace).Get(t.Context(), testShadow+"-node", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if cm.Data["compute-node"] != node || cm.Data["socket-dir"] != spec.SocketDir {
			t.Errorf("configmap data %v", cm.Data)
		}
		pod, err := client.CoreV1().Pods(testNamespace).Get(t.Context(), shadowPod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if pod.Annotations[nodeAnnotation] != node {
			t.Errorf("shadow pod annotations %v", pod.Annotations)
		}
	}
	if got := getDeployment(t, client, testShadow).Annotations[specHashAnnotation]; got != hash {
		t.Error("a node change changed the deployment")
	}
}

func TestDeleteRemovesAll(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	if _, err := m.Ensure(t.Context(), notebookSpec()); err != nil {
		t.Fatal(err)
	}
	other := notebookSpec()
	other.WorkloadUID, other.WorkloadName = "11111111-2222-3333-4444-555555555555", "train"
	if _, err := m.Ensure(t.Context(), other); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if err := m.Delete(t.Context(), testUID); err != nil {
			t.Fatal(err)
		}
	}
	ctx := t.Context()
	if _, err := client.AppsV1().Deployments(testNamespace).Get(ctx, testShadow, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("deployment left: %v", err)
	}
	if _, err := client.CoreV1().Services(testNamespace).Get(ctx, testShadow, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("service left: %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps(testNamespace).Get(ctx, testShadow+"-node", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("configmap left: %v", err)
	}

	getDeployment(t, client, "shadow-train-alice")
	if _, err := client.CoreV1().Services(testNamespace).Get(ctx, "shadow-train-alice", metav1.GetOptions{}); err != nil {
		t.Errorf("another workload's service went: %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps(testNamespace).Get(ctx, "shadow-train-alice-node", metav1.GetOptions{}); err != nil {
		t.Errorf("another workload's configmap went: %v", err)
	}
}

func TestEnsureRefusesAnotherWorkloadsName(t *testing.T) {
	previous := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: testShadow, Namespace: testNamespace,
		Labels: map[string]string{LabelShadow: "true", LabelShadowOf: "a-previous-job"},
	}}
	client, m := newTestManager(t, testConfig(), previous)
	_, err := m.Ensure(t.Context(), notebookSpec())
	if !errors.Is(err, ErrNameTaken) {
		t.Fatalf("err %v, want ErrNameTaken", err)
	}
	if got := getDeployment(t, client, testShadow).Labels[LabelShadowOf]; got != "a-previous-job" {
		t.Errorf("the previous job's deployment was taken over (%s)", got)
	}
}

func TestEnsureRefusesUnsafeNode(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.NodeName = "node07 -oProxyCommand=id"
	if _, err := m.Ensure(t.Context(), spec); err == nil {
		t.Fatal("a node name with a space was accepted")
	}
	if w := writes(client); len(w) != 0 {
		t.Errorf("writes %v", w)
	}
}

func TestEnsureDropsUnsafeSocketDir(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.SocketDir = "/scratch/alice/.interlink"
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	cm, err := client.CoreV1().ConfigMaps(testNamespace).Get(t.Context(), testShadow+"-node", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data["compute-node"] != "node07" || cm.Data["socket-dir"] != "" {
		t.Errorf("configmap data %v", cm.Data)
	}
}

func TestSocketPorts(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.Template.Annotations[annSocketPorts] = "8888"
	if _, err := m.Ensure(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	d := getDeployment(t, client, testShadow)
	if s := container(t, d, "ssh-forward").Args[0]; !strings.Contains(s, `-L "0.0.0.0:8888:$sd/fwd-8888.sock"`) {
		t.Errorf("socket port not forwarded to its socket:\n%s", s)
	}
	if s := d.Spec.Template.Spec.InitContainers[0].Args[0]; !strings.Contains(s, "/interlink/node/socket-dir") {
		t.Errorf("wait-for-node does not wait for the socket directory:\n%s", s)
	}

	for _, c := range []struct{ target, value string }{
		{ForwardTargetLogin, "8888"},
		{ForwardTargetNode, "9999"},
		{ForwardTargetNode, "http"},
	} {
		cfg := testConfig()
		cfg.ForwardTarget = c.target
		_, m := newTestManager(t, cfg)
		spec := notebookSpec()
		spec.Template.Annotations[annSocketPorts] = c.value
		if _, err := m.Ensure(t.Context(), spec); err == nil {
			t.Errorf("target %s, socket ports %q: accepted", c.target, c.value)
		}
	}
}

// TestTemplateRendersEveryMode renders every forward target, forward mode and host
// key check the virtual kubelet supports with public key auth, and runs each
// container script through the shell parser.
func TestTemplateRendersEveryMode(t *testing.T) {
	for _, target := range []string{ForwardTargetLogin, ForwardTargetNode} {
		for _, mode := range []string{ForwardModePortForward, ForwardModeExec} {
			for _, keys := range []string{NodeHostKeysVerify, NodeHostKeysAcceptNew} {
				t.Run(target+"/"+mode+"/"+keys, func(t *testing.T) {
					cfg := testConfig()
					cfg.ForwardTarget, cfg.ForwardMode, cfg.NodeHostKeys = target, mode, keys
					if err := NormalizeConfig(&cfg); err != nil {
						t.Fatal(err)
					}
					spec := notebookSpec()
					spec.Template = podTemplate(8888, 6006)
					ports := exposedPorts(spec.Template)
					if target == ForwardTargetNode {
						if err := markSocketPorts("6006", target, ports); err != nil {
							t.Fatal(err)
						}
					}
					d, svc, err := render(cfg, spec, testShadow, ports, false)
					if err != nil {
						t.Fatal(err)
					}
					if svc == nil {
						t.Fatal("no service")
					}
					pod := d.Spec.Template.Spec
					for _, c := range append(pod.InitContainers, pod.Containers...) {
						assertShellParses(t, c)
					}
					script := container(t, d, "ssh-forward").Args[0]
					var want string
					switch {
					case mode == ForwardModeExec:
						want = "connect_cmd='nc'"
					case target == ForwardTargetLogin:
						want = `-L "0.0.0.0:8888:$node:8888"`
					default:
						want = "-W %s:22"
					}
					if !strings.Contains(script, want) {
						t.Errorf("script lacks %q", want)
					}
				})
			}
		}
	}
}

// assertShellParses runs a container's script through `sh -n` and `bash -n`.
func assertShellParses(t *testing.T, c corev1.Container) {
	t.Helper()
	if len(c.Args) == 0 {
		return
	}
	for _, shell := range []string{"sh", "bash"} {
		path, err := exec.LookPath(shell)
		if err != nil {
			t.Logf("%s not found, not checking the %s script", shell, c.Name)
			continue
		}
		cmd := exec.Command(path, "-n")
		cmd.Stdin = strings.NewReader(c.Args[0])
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Errorf("%s -n on the %s script: %v: %s", shell, c.Name, err, stderr.String())
		}
	}
}

// The expected names come from the virtual kubelet's
// computeShadowResourceNamesForSameNamespace.
func TestResourceName(t *testing.T) {
	for _, c := range []struct{ name, namespace, want string }{
		{"nb", "alice", "shadow-nb-alice"},
		{"web.0", "alice", "shadow-web-0-alice"},
		{"train-a-very-long-job-name-that-goes-on-and-on-forever", "alice", "shadow-train-a-very-long-j-b329a129-alice"},
		{"nb", "a-namespace-whose-name-is-rather-long-too", "shadow-nb-a-1bcf97dc"},
		{"Job_With.Caps", "proj42-team", "shadow-job-with-caps-proj42-team"},
		{"x-really-long-name-of-a-pytorch-distributed-training-job-0123456789", "a-namespace-whose-name-is-rather-long-too-yes", "shadow-x-050adff1"},
	} {
		if got := resourceName(c.name, c.namespace); got != c.want {
			t.Errorf("resourceName(%q, %q) = %q, want %q", c.name, c.namespace, got, c.want)
		}
	}
}

func TestNormalizeConfig(t *testing.T) {
	t.Setenv(ExecutorImageEnv, "registry.example.org/kueue-offload-executor:from-env")
	cfg := testConfig()
	cfg.ExecutorImage = ""
	cfg.ForwardTarget = ""
	if err := NormalizeConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	want := Config{
		Image: DefaultImage, ExecutorImage: "registry.example.org/kueue-offload-executor:from-env",
		ExecutorURL: cfg.ExecutorURL, LoginHost: cfg.LoginHost, User: cfg.User, Port: 22,
		KeySecret: cfg.KeySecret, KeySecretKey: "ssh-privatekey", KnownHostsConfigMap: cfg.KnownHostsConfigMap,
		NodeHostKeys: NodeHostKeysVerify, ForwardTarget: ForwardTargetLogin, ForwardMode: ForwardModePortForward,
		NodeWaitTimeout: "2h",
		Reverse:         ReverseConfig{NodeExec: ReverseNodeExecSSH, Sshd: ReverseSshdNode, SshdPath: DefaultReverseSshdPath, PythonPath: DefaultReversePythonPath},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("normalized\n%+v\nwant\n%+v", cfg, want)
	}
	if !cfg.alwaysShadow() || cfg.nodeWaitSeconds() != 7200 {
		t.Errorf("alwaysShadow %v, nodeWaitSeconds %d", cfg.alwaysShadow(), cfg.nodeWaitSeconds())
	}

	execMode := testConfig()
	execMode.ForwardMode = "Exec"
	if err := NormalizeConfig(&execMode); err != nil || execMode.ForwardMode != ForwardModeExec || execMode.ExecConnectCommand != "nc" {
		t.Errorf("exec mode: %v, %+v", err, execMode)
	}

	t.Setenv(ExecutorImageEnv, "")
	for name, broken := range map[string]func(*Config){
		"no executor image":     func(c *Config) { c.ExecutorImage = "" },
		"no executor url":       func(c *Config) { c.ExecutorURL = "" },
		"executor url not http": func(c *Config) { c.ExecutorURL = "kueue-offload-executor:8000" },
		"reverse without login": func(c *Config) { c.LoginHost, c.Reverse.Enabled = "", true },
		"login host injection":  func(c *Config) { c.LoginHost = "login;id" },
		"user with space":       func(c *Config) { c.User = "alice -oProxyCommand=id" },
		"no key secret":         func(c *Config) { c.KeySecret = "" },
		"bad key secret key":    func(c *Config) { c.KeySecretKey = "../id" },
		"port out of range":     func(c *Config) { c.Port = 70000 },
		"unknown mode":          func(c *Config) { c.ForwardMode = "socks" },
		"unknown target":        func(c *Config) { c.ForwardTarget = "both" },
		"unknown host keys":     func(c *Config) { c.NodeHostKeys = "trust" },
		"quoted exec command":   func(c *Config) { c.ForwardMode, c.ExecConnectCommand = ForwardModeExec, "nc'" },
		"bad wait":              func(c *Config) { c.NodeWaitTimeout = "forever" },
		"unknown node exec":     func(c *Config) { c.Reverse.NodeExec = "pbsdsh" },
		"unknown sshd":          func(c *Config) { c.Reverse.Sshd = "dropbear" },
		"quoted srun command":   func(c *Config) { c.Reverse.NodeExec, c.ExecConnectCommand = ReverseNodeExecSrun, "nc'" },
		"target without port":   func(c *Config) { c.Reverse.AllowedTargets = []string{"mlflow.alice.svc.cluster.local"} },
		"target injection":      func(c *Config) { c.Reverse.AllowedTargets = []string{"a;id:443"} },
		"socks with targets": func(c *Config) {
			c.Reverse.AllowSocks, c.Reverse.AllowedTargets = true, []string{"mlflow.alice.svc.cluster.local:443"}
		},
		"negative wait": func(c *Config) { c.NodeWaitTimeout = "-1m" },
	} {
		cfg := testConfig()
		broken(&cfg)
		if err := NormalizeConfig(&cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Without a login node the shadow only streams logs: no user or key is needed, and
// a tunnel is refused rather than rendered.
func TestNoLoginHostIsLogsOnly(t *testing.T) {
	cfg := testConfig()
	cfg.LoginHost, cfg.User, cfg.KeySecret, cfg.KnownHostsConfigMap = "", "", "", ""
	client, m := newTestManager(t, cfg)
	spec := notebookSpec()
	spec.NodeName = ""
	if _, err := m.Ensure(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "Shadow.LoginHost is empty") {
		t.Fatalf("a forward without a login node: %v", err)
	}
	if len(writes(client)) != 0 {
		t.Fatalf("writes %v", writes(client))
	}
	spec.Template = podTemplate()
	ref, err := m.Ensure(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	assertLogsOnly(t, client, ref)
	if !ForwardsPorts(podTemplate(8888)) || ForwardsPorts(podTemplate()) {
		t.Errorf("ForwardsPorts")
	}
}

func TestEnsureWithInvalidConfigFails(t *testing.T) {
	cfg := testConfig()
	cfg.KeySecret = ""
	client := fake.NewClientset()
	m := NewManager(client, testNamespace, cfg)
	if _, err := m.Ensure(t.Context(), notebookSpec()); err == nil {
		t.Fatal("Ensure ran on an invalid configuration")
	}
	if len(client.Actions()) != 0 {
		t.Errorf("actions %v", client.Actions())
	}
}

func TestEnsureRefusesOtherNamespace(t *testing.T) {
	client, m := newTestManager(t, testConfig())
	spec := notebookSpec()
	spec.Namespace = "bob"
	if _, err := m.Ensure(t.Context(), spec); err == nil {
		t.Fatal("a shadow in another namespace was accepted")
	}
	if len(client.Actions()) != 0 {
		t.Errorf("actions %v", client.Actions())
	}
}
