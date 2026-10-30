package executor

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/shadow"
)

func shadowBlock() ShadowConfig {
	return ShadowConfig{
		Enabled:       true,
		ExecutorImage: "registry.example.org/kueue-offload:test",
		ExecutorURL:   "http://kueue-offload-executor:8000",
		LoginHost:     "login.hpc-a.example.org",
		User:          "alice",
		KeySecret:     "hpc-a-tunnel-key",
	}
}

func TestValidateConfigChecksShadow(t *testing.T) {
	t.Setenv(shadow.ExecutorImageEnv, "")
	base := func() Config {
		c := testConfig("http://localhost:3000")
		c.Shadow = shadowBlock()
		return c
	}
	c := base()
	if err := c.Validate(); err != nil {
		t.Fatalf("valid Shadow block refused: %v", err)
	}

	bad := map[string]func(*Config){
		"no executor URL":   func(c *Config) { c.Shadow.ExecutorURL = "" },
		"no executor image": func(c *Config) { c.Shadow.ExecutorImage = "" },
		"no key Secret":     func(c *Config) { c.Shadow.KeySecret = "" },
		"no user":           func(c *Config) { c.Shadow.User = "" },
		"reverse without login": func(c *Config) {
			c.Shadow.LoginHost, c.Shadow.Reverse.Enabled = "", true
		},
		"bad forward mode":     func(c *Config) { c.Shadow.ForwardMode = "socks" },
		"bad node wait":        func(c *Config) { c.Shadow.NodeWaitTimeout = "soon" },
		"user unsafe in shell": func(c *Config) { c.Shadow.User = "alice;rm" },
	}
	for name, mutate := range bad {
		c := base()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	// No login node: logs-only shadows, with neither user nor key.
	c = base()
	c.Shadow.LoginHost, c.Shadow.User, c.Shadow.KeySecret = "", "", ""
	if err := c.Validate(); err != nil {
		t.Fatalf("logs-only Shadow block refused: %v", err)
	}

	t.Setenv(shadow.ExecutorImageEnv, "registry.example.org/kueue-offload:env")
	c = base()
	c.Shadow.ExecutorImage = ""
	if err := c.Validate(); err != nil {
		t.Fatalf("ExecutorImage from %s refused: %v", shadow.ExecutorImageEnv, err)
	}
}

// TestWithShadowManager runs a request with the real shadow manager on a fake
// cluster: the shadow appears once the job runs, loses its tunnel when the job
// ends, and goes with the request.
func TestWithShadowManager(t *testing.T) {
	e := newExecutorEnv(t)
	e.d.Config.Shadow = shadowBlock()
	m, err := NewShadowManager(e.kube, e.d.Config)
	if err != nil {
		t.Fatal(err)
	}
	e.d.Shadow = m
	ctx := context.Background()
	deployments := e.kube.AppsV1().Deployments(executorNS)
	services := e.kube.CoreV1().Services(executorNS)
	configMaps := e.kube.CoreV1().ConfigMaps(executorNS)
	selector := metav1.ListOptions{LabelSelector: shadow.LabelShadowOf + "=" + executorWorkload}

	req := testRequest(1)
	req.Spec.PodSets[0].Template.Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 8888}}
	e.submit(req)
	e.setStatus(0, "", waitingContainer(executorCtn, "Priority", ""))
	if q := e.poll(); q.Status.Shadow != nil {
		t.Fatalf("queued job has a shadow: %+v", q.Status.Shadow)
	}
	if list, _ := deployments.List(ctx, selector); len(list.Items) != 0 {
		t.Fatalf("queued job got %d shadow Deployments", len(list.Items))
	}

	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	q := e.poll()
	st := q.Status
	if state(q) != "running" || st.Shadow == nil || st.Shadow.Deployment == "" || st.Shadow.Service != st.Shadow.Deployment {
		t.Fatalf("running: state %s, shadow %+v", state(q), st.Shadow)
	}
	dep, err := deployments.Get(ctx, st.Shadow.Deployment, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if dep.Labels[shadow.LabelShadowOf] != executorWorkload {
		t.Errorf("Deployment labels: %v", dep.Labels)
	}
	forward, logs := shadowContainers(t, dep)
	if !strings.Contains(strings.Join(forward.Args, " ")+strings.Join(forward.Command, " "), "alice@login.hpc-a.example.org") {
		t.Errorf("ssh-forward does not reach the site's login node as its user:\n%v %v", forward.Command, forward.Args)
	}
	if logs.Image != "registry.example.org/kueue-offload:test" || strings.Join(logs.Command, " ") != "/executor logs --executor http://kueue-offload-executor:8000 --workload "+executorWorkload+" --follow" {
		t.Errorf("job-logs: image %s, command %v", logs.Image, logs.Command)
	}
	if _, err := services.Get(ctx, st.Shadow.Service, metav1.GetOptions{}); err != nil {
		t.Fatalf("Service of a running job with a port: %v", err)
	}
	if cm := e.nodeConfigMap(); cm.Data["compute-node"] != "node07" {
		t.Fatalf("node ConfigMap: %v", cm.Data)
	}

	e.setStatus(0, "node07", terminatedContainer(executorCtn, 0, "Completed", time.Now(), time.Now()))
	q = e.poll()
	st = q.Status
	if state(q) != "succeeded" || st.Shadow == nil || st.Shadow.Service != "" {
		t.Fatalf("completed: state %s, shadow %+v", state(q), st.Shadow)
	}
	if _, err := services.Get(ctx, st.Shadow.Deployment, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the Service outlived the job: %v", err)
	}
	dep, err = deployments.Get(ctx, st.Shadow.Deployment, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the logs-only shadow is gone: %v", err)
	}
	if names := containerNames(dep); names != "job-logs" {
		t.Fatalf("logs-only shadow runs %s", names)
	}
	before := len(e.kube.Actions())
	e.poll()
	for _, a := range e.kube.Actions()[before:] {
		if a.GetVerb() != "get" && a.GetVerb() != "list" {
			t.Errorf("an ended job's shadow was changed again: %s %s", a.GetVerb(), a.GetResource().Resource)
		}
	}
	// A restarted executor renders it for the end once more, unchanged.
	e.restart()
	e.poll()
	if dep, err = deployments.Get(ctx, st.Shadow.Deployment, metav1.GetOptions{}); err != nil || containerNames(dep) != "job-logs" {
		t.Fatalf("the logs-only shadow after a restart: %v", err)
	}

	if err := e.remove(); err != nil {
		t.Fatal(err)
	}
	if list, _ := deployments.List(ctx, selector); len(list.Items) != 0 {
		t.Errorf("%d shadow Deployments left after the delete", len(list.Items))
	}
	if list, _ := configMaps.List(ctx, selector); len(list.Items) != 0 {
		t.Errorf("%d shadow ConfigMaps left after the delete", len(list.Items))
	}
}

// TestShadowOfSinglePod: the shadow of a Workload of one plain pod carries the
// labels the Service in front of the pod (a Kubeflow Notebook's) selects it by,
// so the Service reaches the job through the shadow; its pod's IP and readiness
// go to the request's status.
func TestShadowOfSinglePod(t *testing.T) {
	e := newExecutorEnv(t, &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "nb", Namespace: executorNS},
		Spec:       v1.ServiceSpec{Selector: map[string]string{"statefulset": "nb"}},
	})
	e.d.Config.Shadow = shadowBlock()
	m, err := NewShadowManager(e.kube, e.d.Config)
	if err != nil {
		t.Fatal(err)
	}
	e.d.Shadow = m
	ctx := context.Background()

	req := testRequest(1)
	req.Spec.Owner = v1alpha1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "nb-0", UID: "0b6c-pod"}
	req.Spec.OwnerLabels = map[string]string{
		"statefulset": "nb", "notebook-name": "nb", "hpc-a-proj42": "true", "kueue.x-k8s.io/queue-name": "hpc-a-proj42",
	}
	req.Spec.PodSets[0].Template.Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 8888}}
	e.submit(req)
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	q := e.poll()
	if q.Status.Shadow == nil || q.Status.Shadow.PodIP != "" || q.Status.Shadow.Ready {
		t.Fatalf("shadow without a pod yet: %+v", q.Status.Shadow)
	}
	dep, err := e.kube.AppsV1().Deployments(executorNS).Get(ctx, q.Status.Shadow.Deployment, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	labels := dep.Spec.Template.Labels
	if labels["statefulset"] != "nb" || labels[shadow.LabelShadowOf] != executorWorkload {
		t.Errorf("shadow pod labels %v, want the Service's and the shadow's", labels)
	}
	for _, k := range []string{"notebook-name", "hpc-a-proj42", "kueue.x-k8s.io/queue-name"} {
		if _, ok := labels[k]; ok {
			t.Errorf("shadow pod carries %s: %v", k, labels)
		}
	}

	// The Deployment's pod, as the cluster would run it.
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: dep.Name + "-7c9f-x2x4q", Namespace: executorNS, Labels: labels},
		Status:     v1.PodStatus{PodIP: "10.244.1.7", Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionFalse}}},
	}
	pods := e.kube.CoreV1().Pods(executorNS)
	if _, err := pods.Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if sh := e.poll().Status.Shadow; sh.PodIP != "10.244.1.7" || sh.Ready {
		t.Fatalf("shadow %+v, want its pod's IP, not ready", sh)
	}
	pod.Status.Conditions[0].Status = v1.ConditionTrue
	if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if sh := e.poll().Status.Shadow; sh.PodIP != "10.244.1.7" || !sh.Ready {
		t.Fatalf("shadow %+v, want its pod's IP, ready", sh)
	}
}

// TestShadowOfJobInheritsNoLabels: only a Workload of one plain pod has owner
// labels; a Job's shadow keeps its own.
func TestShadowOfJobInheritsNoLabels(t *testing.T) {
	e := newExecutorEnv(t)
	req := testRequest(1)
	req.Spec.PodSets[0].Template.Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 8888}}
	e.submit(req)
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	e.poll()
	if len(e.shadow.ensured) != 1 || e.shadow.ensured[0].Labels != nil {
		t.Fatalf("shadows %+v, want one without owner labels", e.shadow.ensured)
	}
}

// nodeConfigMap is the shadow's ConfigMap with the compute node.
func (e *executorEnv) nodeConfigMap() *v1.ConfigMap {
	e.t.Helper()
	list, err := e.kube.CoreV1().ConfigMaps(executorNS).List(context.Background(), metav1.ListOptions{LabelSelector: shadow.LabelShadowOf + "=" + executorWorkload})
	if err != nil {
		e.t.Fatal(err)
	}
	for i := range list.Items {
		if _, ok := list.Items[i].Data["compute-node"]; ok {
			return &list.Items[i]
		}
	}
	e.t.Fatalf("no node ConfigMap among %d shadow ConfigMaps", len(list.Items))
	return nil
}

func shadowContainers(t *testing.T, dep *appsv1.Deployment) (forward, logs v1.Container) {
	t.Helper()
	var haveForward, haveLogs bool
	for _, c := range dep.Spec.Template.Spec.Containers {
		switch c.Name {
		case "ssh-forward":
			forward, haveForward = c, true
		case "job-logs":
			logs, haveLogs = c, true
		}
	}
	if !haveForward || !haveLogs {
		t.Fatalf("shadow containers: %s", containerNames(dep))
	}
	return forward, logs
}

func containerNames(dep *appsv1.Deployment) string {
	var names []string
	for _, c := range dep.Spec.Template.Spec.Containers {
		names = append(names, c.Name)
	}
	return strings.Join(names, ",")
}
