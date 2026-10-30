package executor

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/executorapi"
	"github.com/matbun/kueue-offload/pkg/shadow"
)

const (
	reverseTarget = "mlflow.alice.svc.cluster.local"
	testSocketDir = "/scratch/alice/interlink/sockets/9f3e0a1b2c4d"
)

// reverseEnv is an executor whose shadows are rendered by the real shadow manager
// on a fake cluster, with the reverse tunnel enabled.
func reverseEnv(t *testing.T, reverse shadow.ReverseConfig) *executorEnv {
	t.Helper()
	e := newExecutorEnv(t)
	e.d.Config.Shadow = shadowBlock()
	e.d.Config.Shadow.Reverse = reverse
	m, err := NewShadowManager(e.kube, e.d.Config)
	if err != nil {
		t.Fatal(err)
	}
	e.d.Shadow = m
	return e
}

func reverseRequest(count int32, forwards string) *v1alpha1.OffloadRequest {
	req := testRequest(count)
	req.Spec.PodSets[0].Template.Annotations = map[string]string{shadow.AnnReverseForwards: forwards}
	return req
}

// The head rank runs the tunnel's set-up before its own pre-exec, the other ranks
// do not, and the shadow carries the ssh-reverse container to the head's node
// and socket directory, as the plugin reports them.
func TestReverseTunnel(t *testing.T) {
	e := reverseEnv(t, shadow.ReverseConfig{Enabled: true, AllowedTargets: []string{reverseTarget + ":5000"}})
	req := reverseRequest(2, reverseTarget+":5000")
	req.Spec.PodSets[0].Template.Annotations[preExecAnnotation] = "echo own pre-exec\n"
	e.submit(req)

	pods := e.plugin.createdPods()
	head := pods[0].Annotations[preExecAnnotation]
	names := reverseTarget + " mlflow.alice.svc mlflow.alice mlflow"
	for _, want := range []string{"interLink reverse tunnel", names, "rev-relay.py"} {
		if !strings.Contains(head, want) {
			t.Errorf("rank 0's pre-exec lacks %q\n%s", want, head)
		}
	}
	if !strings.HasSuffix(head, "echo own pre-exec\n") {
		t.Errorf("the template's own pre-exec does not come last\n%s", head)
	}
	if worker := pods[1].Annotations[preExecAnnotation]; strings.Contains(worker, "interLink reverse tunnel") {
		t.Errorf("a worker rank sets up the tunnel\n%s", worker)
	}
	if out, err := exec.Command("bash", "-n", "-c", head).CombinedOutput(); err != nil {
		t.Errorf("bash -n on rank 0's pre-exec: %v: %s", err, out)
	}

	for i, node := range []string{"node07", "node08"} {
		e.setStatus(i, node, runningContainer(executorCtn, time.Now()))
	}
	e.plugin.with(func(p *fakePlugin) {
		st := p.status[memberUID(executorWorkload, 0)]
		st.SocketDir = testSocketDir
		p.status[memberUID(executorWorkload, 0)] = st
	})
	q := e.poll()
	st := q.Status
	if state(q) != "running" || st.Shadow == nil || st.Shadow.Service != "" {
		t.Fatalf("running: state %s, shadow %+v", state(q), st.Shadow)
	}
	dep, err := e.kube.AppsV1().Deployments(executorNS).Get(context.Background(), st.Shadow.Deployment, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if names := containerNames(dep); names != "ssh-reverse,job-logs" {
		t.Fatalf("shadow containers %s", names)
	}
	env := envOf(dep.Spec.Template.Spec.Containers[0])
	if env["REV_FORWARDS"] != "5000:"+reverseTarget+":5000" || env["REV_JOB_NAME"] != executorWorkload {
		t.Errorf("ssh-reverse env %v", env)
	}
	cm := e.nodeConfigMap()
	if cm.Data["compute-node"] != "node07" || cm.Data["socket-dir"] != testSocketDir {
		t.Fatalf("node ConfigMap %v: the tunnel must end in the head's socket directory", cm.Data)
	}
}

// The single-pod job of the Slurm plugin is named after its pod: the srun carrier
// finds it by the member's UID.
func TestReverseSinglePod(t *testing.T) {
	e := reverseEnv(t, shadow.ReverseConfig{Enabled: true})
	req := reverseRequest(1, "8443:"+reverseTarget+":443")
	rt, aerr := e.d.reverseTunnel(jobRequestOf(req))
	if aerr != nil || rt.JobName != memberUID(executorWorkload, 0) {
		t.Fatalf("tunnel %+v, %v", rt, aerr)
	}
	e.submit(req)
	if pre := e.plugin.createdPods()[0].Annotations[preExecAnnotation]; !strings.Contains(pre, "interLink reverse tunnel") {
		t.Fatalf("the tunnel is not set up\n%s", pre)
	}
}

func TestReverseRefusals(t *testing.T) {
	enabled := shadow.ReverseConfig{Enabled: true}
	tests := []struct {
		name    string
		reverse shadow.ReverseConfig
		req     *v1alpha1.OffloadRequest
		want    string
	}{
		{"reverse disabled", shadow.ReverseConfig{}, reverseRequest(1, reverseTarget+":5000"), "Shadow.Reverse.Enabled"},
		{"target not allowed", shadow.ReverseConfig{Enabled: true, AllowedTargets: []string{reverseTarget + ":5000"}},
			reverseRequest(1, reverseTarget+":5000 minio.alice.svc.cluster.local:9000"), "target minio.alice.svc.cluster.local:9000 is not in Shadow.Reverse.AllowedTargets"},
		{"socks not allowed", enabled, func() *v1alpha1.OffloadRequest {
			r := testRequest(1)
			r.Spec.PodSets[0].Template.Annotations = map[string]string{shadow.AnnReverseSocksPort: "1080"}
			return r
		}(), "Shadow.Reverse.AllowSocks"},
		{"site launcher", enabled, func() *v1alpha1.OffloadRequest {
			r := reverseRequest(2, reverseTarget+":5000")
			r.Spec.PodSets[0].Template.Annotations[LauncherAnnotation] = "torchrun"
			return r
		}(), LauncherAnnotation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := reverseEnv(t, tt.reverse)
			e.refusedWith(e.take(tt.req), tt.want)
			if n := len(e.plugin.createdPods()); n != 0 {
				t.Fatalf("the plugin got %d pods", n)
			}
		})
	}
}

func TestValidateConfigChecksReverse(t *testing.T) {
	t.Setenv(shadow.ExecutorImageEnv, "")
	base := func() Config {
		c := testConfig("http://localhost:3000")
		c.Shadow = shadowBlock()
		c.Shadow.Reverse = shadow.ReverseConfig{Enabled: true, AllowedTargets: []string{reverseTarget + ":443"}}
		return c
	}
	c := base()
	if err := c.Validate(); err != nil {
		t.Fatalf("valid Reverse block refused: %v", err)
	}
	for name, mutate := range map[string]func(*Config){
		"no shadows":         func(c *Config) { c.Shadow.Enabled = false },
		"bad target":         func(c *Config) { c.Shadow.Reverse.AllowedTargets = []string{reverseTarget} },
		"socks with targets": func(c *Config) { c.Shadow.Reverse.AllowSocks = true },
	} {
		c := base()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// tokenRequests answers TokenRequests with a token naming the ServiceAccount, and
// records them.
func tokenRequests(e *executorEnv) *[]k8stesting.CreateActionImpl {
	var seen []k8stesting.CreateActionImpl
	e.kube.PrependReactor("create", "serviceaccounts", func(a k8stesting.Action) (bool, runtime.Object, error) {
		create := a.(k8stesting.CreateActionImpl)
		if create.GetSubresource() != "token" {
			return false, nil, nil
		}
		seen = append(seen, create)
		tr := create.GetObject().(*authv1.TokenRequest).DeepCopy()
		tr.Status.Token = "minted-for-" + create.Name
		return true, tr, nil
	})
	return &seen
}

func kubeAPIAccessVolume() v1.Volume {
	expiration := int64(3607)
	return v1.Volume{Name: "kube-api-access-x7k2p", VolumeSource: v1.VolumeSource{Projected: &v1.ProjectedVolumeSource{Sources: []v1.VolumeProjection{
		{ServiceAccountToken: &v1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}},
		{ConfigMap: &v1.ConfigMapProjection{LocalObjectReference: v1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []v1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
		{DownwardAPI: &v1.DownwardAPIProjection{Items: []v1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}}}},
	}}}}
}

// projectedFile returns a file of a projected volume as the plugin received it
// for a container of member i.
func (e *executorEnv) projectedFile(i int, container, volume, file string) (string, bool) {
	e.t.Helper()
	for _, cm := range e.plugin.container(e.t, i, container).ProjectedVolumeMaps {
		if cm.Name == volume {
			v, ok := cm.Data[file]
			return v, ok
		}
	}
	return "", false
}

// The API server's kube-api-access volume of a plain pod that asks for its token
// gets one minted for the pod's ServiceAccount, in the projected volume data the
// plugin writes.
func TestServiceAccountToken(t *testing.T) {
	e := newExecutorEnv(t)
	seen := tokenRequests(e)
	req := testRequest(2)
	ps := &req.Spec.PodSets[0]
	ps.Template.Annotations = map[string]string{executorapi.ServiceAccountTokenAnnotation: "true"}
	ps.ConfigMapRefs = []string{"kube-root-ca.crt"}
	ps.Template.Spec.ServiceAccountName = "trainer"
	ps.Template.Spec.Volumes = []v1.Volume{kubeAPIAccessVolume()}
	ps.Template.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{{Name: "kube-api-access-x7k2p", MountPath: serviceAccountDir, ReadOnly: true}}
	e.submit(req)

	if len(*seen) != 1 {
		t.Fatalf("%d TokenRequests, want 1", len(*seen))
	}
	got := (*seen)[0]
	tr := got.GetObject().(*authv1.TokenRequest)
	if got.Namespace != executorNS || got.Name != "trainer" || tr.Spec.ExpirationSeconds == nil || *tr.Spec.ExpirationSeconds != 3607 || len(tr.Spec.Audiences) != 0 {
		t.Fatalf("TokenRequest %s/%s %+v", got.Namespace, got.Name, tr.Spec)
	}
	for i := 0; i < 2; i++ {
		if token, ok := e.projectedFile(i, executorCtn, "kube-api-access-x7k2p", "token"); !ok || token != "minted-for-trainer" {
			t.Errorf("member %d: token %q (found %v)", i, token, ok)
		}
		if ns, ok := e.projectedFile(i, executorCtn, "kube-api-access-x7k2p", "namespace"); !ok || ns != executorNS {
			t.Errorf("member %d: namespace %q (found %v)", i, ns, ok)
		}
		if ca, ok := e.projectedFile(i, executorCtn, "kube-api-access-x7k2p", "ca.crt"); !ok || !strings.HasPrefix(ca, "-----BEGIN CERTIFICATE") {
			t.Errorf("member %d: ca.crt %q (found %v)", i, ca, ok)
		}
	}
}

// Without the annotation the token of the API server's kube-api-access volume never
// leaves the cluster: no TokenRequest, and the job gets only namespace and ca.crt.
func TestInjectedTokenDropped(t *testing.T) {
	e := newExecutorEnv(t)
	seen := tokenRequests(e)
	req := testRequest(1)
	ps := &req.Spec.PodSets[0]
	ps.ConfigMapRefs = []string{"kube-root-ca.crt"}
	ps.Template.Spec.Volumes = []v1.Volume{kubeAPIAccessVolume()}
	ps.Template.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{{Name: "kube-api-access-x7k2p", MountPath: serviceAccountDir, ReadOnly: true}}
	e.submit(req)

	if len(*seen) != 0 {
		t.Fatalf("%d TokenRequests, want none", len(*seen))
	}
	if token, ok := e.projectedFile(0, executorCtn, "kube-api-access-x7k2p", "token"); ok {
		t.Errorf("token %q reached the plugin", token)
	}
	if ns, ok := e.projectedFile(0, executorCtn, "kube-api-access-x7k2p", "namespace"); !ok || ns != executorNS {
		t.Errorf("namespace %q (found %v)", ns, ok)
	}
}

func TestServiceAccountTokenDefaults(t *testing.T) {
	e := newExecutorEnv(t)
	seen := tokenRequests(e)
	req := testRequest(1)
	ps := &req.Spec.PodSets[0]
	ps.Template.Spec.Volumes = []v1.Volume{{Name: "mlflow-token", VolumeSource: v1.VolumeSource{Projected: &v1.ProjectedVolumeSource{Sources: []v1.VolumeProjection{
		{ServiceAccountToken: &v1.ServiceAccountTokenProjection{Path: "jwt", Audience: "mlflow"}},
	}}}}}
	ps.Template.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{{Name: "mlflow-token", MountPath: "/var/run/mlflow"}}
	e.submit(req)
	if len(*seen) != 1 {
		t.Fatalf("%d TokenRequests, want 1", len(*seen))
	}
	got := (*seen)[0]
	tr := got.GetObject().(*authv1.TokenRequest)
	if got.Name != "default" || *tr.Spec.ExpirationSeconds != 3600 || len(tr.Spec.Audiences) != 1 || tr.Spec.Audiences[0] != "mlflow" {
		t.Fatalf("TokenRequest for %s: %+v", got.Name, tr.Spec)
	}
	if token, ok := e.projectedFile(0, executorCtn, "mlflow-token", "jwt"); !ok || token != "minted-for-default" {
		t.Errorf("token %q (found %v)", token, ok)
	}
}

func TestNoTokenWithoutProjection(t *testing.T) {
	e := newExecutorEnv(t)
	seen := tokenRequests(e)
	req := testRequest(1)
	req.Spec.PodSets[0].Template.Spec.AutomountServiceAccountToken = ptrBool(true)
	e.submit(req)
	for _, a := range e.kube.Actions() {
		if a.GetResource().Resource == "serviceaccounts" {
			t.Fatalf("a template without a token projection made %s %s/%s", a.GetVerb(), a.GetResource().Resource, a.GetSubresource())
		}
	}
	if len(*seen) != 0 {
		t.Fatalf("%d TokenRequests", len(*seen))
	}
}

func TestTokenRefusedIsPermanent(t *testing.T) {
	e := newExecutorEnv(t)
	e.kube.PrependReactor("create", "serviceaccounts", func(a k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(v1.Resource("serviceaccounts/token"), "default", nil)
	})
	req := testRequest(1)
	req.Spec.PodSets[0].Template.Spec.Volumes = []v1.Volume{{Name: "tok", VolumeSource: v1.VolumeSource{Projected: &v1.ProjectedVolumeSource{Sources: []v1.VolumeProjection{
		{ServiceAccountToken: &v1.ServiceAccountTokenProjection{Path: "token"}},
	}}}}}
	e.refusedWith(e.take(req), `ServiceAccount "default"`)
}

// A plain pod's step (Argo's executor) reaches the API server through its tunnel and
// addresses its own pod: the downward API names the real pod, and the kubelet's
// service variables point at the tunnel.
func TestSinglePodSeesItself(t *testing.T) {
	e := newExecutorEnv(t)
	tokenRequests(e)
	e.d.Config.Executor.KubernetesAPI = ExecutorKubernetesAPI{Host: "kubernetes.default.svc.cluster.local", Port: "6443"}
	req := testRequest(1)
	req.Spec.Owner = v1alpha1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "step-7f2k", UID: "real-pod-uid"}
	ps := &req.Spec.PodSets[0]
	ps.Template.Annotations = map[string]string{executorapi.ServiceAccountTokenAnnotation: "true"}
	ps.ConfigMapRefs = []string{"kube-root-ca.crt"}
	ps.Template.Spec.Volumes = []v1.Volume{kubeAPIAccessVolume()}
	ps.Template.Spec.Containers[0].Env = []v1.EnvVar{
		{Name: "ARGO_POD_NAME", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "ARGO_POD_UID", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
	}
	e.submit(req)
	pod := e.plugin.createdPods()[0]
	env := envOf(pod.Spec.Containers[0])
	for k, want := range map[string]string{"ARGO_POD_NAME": "step-7f2k", "ARGO_POD_UID": "real-pod-uid", "KUBERNETES_SERVICE_HOST": "kubernetes.default.svc.cluster.local", "KUBERNETES_SERVICE_PORT": "6443"} {
		if env[k] != want {
			t.Errorf("env %s = %q, want %q", k, env[k], want)
		}
	}
	if pod.Name != "step-7f2k" {
		t.Errorf("member name %s, want the pod's", pod.Name)
	}

	// Without a token the job gets no API address.
	e2 := newExecutorEnv(t)
	e2.d.Config.Executor.KubernetesAPI = ExecutorKubernetesAPI{Host: "kubernetes.default.svc.cluster.local", Port: "6443"}
	e2.submit(testRequest(1))
	if _, ok := envOf(e2.plugin.createdPods()[0].Spec.Containers[0])["KUBERNETES_SERVICE_HOST"]; ok {
		t.Errorf("a job without a token got the API address")
	}
}

// A Job's template has no ServiceAccount directory (the API server adds it to pods, and
// the workload's pods never exist): the executor adds it, with the namespace and the CA, and
// with a token only when the site allows cluster tokens.
func TestServiceAccountDirectory(t *testing.T) {
	const volume = "kube-api-access-interlink"
	for _, tc := range []struct {
		name       string
		automount  *bool
		siteTokens bool
		wantDir    bool
		wantToken  bool
	}{
		{"default, no cluster tokens", nil, false, true, false},
		{"default, cluster tokens", nil, true, true, true},
		{"opted out", ptrBool(false), true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			seen := tokenRequests(e)
			e.d.Config.Executor.AutomountServiceAccountToken = tc.siteTokens
			req := testRequest(1)
			req.Spec.PodSets[0].Template.Spec.AutomountServiceAccountToken = tc.automount
			e.submit(req)
			ns, hasNS := e.projectedFile(0, executorCtn, volume, "namespace")
			_, hasCA := e.projectedFile(0, executorCtn, volume, "ca.crt")
			token, hasToken := e.projectedFile(0, executorCtn, volume, "token")
			if hasNS != tc.wantDir || hasCA != tc.wantDir || (hasNS && ns != executorNS) {
				t.Errorf("namespace %q (%v), ca.crt %v; want the directory: %v", ns, hasNS, hasCA, tc.wantDir)
			}
			if hasToken != tc.wantToken || len(*seen) != map[bool]int{true: 1, false: 0}[tc.wantToken] || (hasToken && token != "minted-for-default") {
				t.Errorf("token %q (%v), %d TokenRequests; want a token: %v", token, hasToken, len(*seen), tc.wantToken)
			}
			mounted := false
			for _, m := range e.plugin.createdPods()[0].Spec.Containers[0].VolumeMounts {
				mounted = mounted || (m.Name == volume && m.MountPath == serviceAccountDir)
			}
			if mounted != tc.wantDir {
				t.Errorf("directory mounted: %v, want %v", mounted, tc.wantDir)
			}
		})
	}
}
