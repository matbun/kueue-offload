package executor

import (
	"context"
	"os"
	"path/filepath"
	"slices"
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
		Enabled:   true,
		LoginHost: "login.hpc-a.example.org",
		User:      "alice",
		KeySecret: "hpc-a-tunnel-key",
	}
}

func TestValidateConfigChecksShadow(t *testing.T) {
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
		"no key Secret": func(c *Config) { c.Shadow.KeySecret = "" },
		"no user":       func(c *Config) { c.Shadow.User = "" },
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

	// No login node: no tunnels, so neither user nor key.
	c = base()
	c.Shadow.LoginHost, c.Shadow.User, c.Shadow.KeySecret = "", "", ""
	if err := c.Validate(); err != nil {
		t.Fatalf("Shadow block without a login node refused: %v", err)
	}
	// The logs-only shadow's keys are gone: the executor refuses unknown keys.
	path := filepath.Join(t.TempDir(), "executor.yaml")
	for _, key := range []string{"AlwaysShadow: true", "ExecutorURL: http://kueue-offload-executor:8000", "ExecutorImage: executor:1"} {
		raw := "InterLink: {URL: http://localhost:3000}\nExecutor: {Namespace: " + executorNS + "}\n" +
			"Sites: {" + executorSite + ": {AllowedAccounts: [" + executorAccount + "]}}\nShadow:\n  Enabled: true\n  " + key + "\n"
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), strings.SplitN(key, ":", 2)[0]) {
			t.Errorf("Shadow.%s: %v", key, err)
		}
	}
}

// TestWithShadowManager runs a request with the real shadow manager on a fake
// cluster: the shadow appears once the job runs and is deleted when the job
// ends, once per executor run.
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
	if names := containerNames(dep); names != "ssh-forward" {
		t.Fatalf("shadow containers %s", names)
	}
	forward := dep.Spec.Template.Spec.Containers[0]
	if !strings.Contains(strings.Join(forward.Args, " ")+strings.Join(forward.Command, " "), "alice@login.hpc-a.example.org") {
		t.Errorf("ssh-forward does not reach the site's login node as its user:\n%v %v", forward.Command, forward.Args)
	}
	if _, err := services.Get(ctx, st.Shadow.Service, metav1.GetOptions{}); err != nil {
		t.Fatalf("Service of a running job with a port: %v", err)
	}
	if cm := e.nodeConfigMap(); cm.Data["compute-node"] != "node07" {
		t.Fatalf("node ConfigMap: %v", cm.Data)
	}

	e.setStatus(0, "node07", terminatedContainer(executorCtn, 0, "Completed", time.Now(), time.Now()))
	name := st.Shadow.Deployment
	q = e.poll()
	if state(q) != "succeeded" || q.Status.Shadow != nil {
		t.Fatalf("completed: state %s, shadow %+v", state(q), q.Status.Shadow)
	}
	if _, err := deployments.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the shadow outlived the job: %v", err)
	}
	if _, err := services.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the Service outlived the job: %v", err)
	}
	if list, _ := configMaps.List(ctx, selector); len(list.Items) != 0 {
		t.Fatalf("%d shadow ConfigMaps outlived the job", len(list.Items))
	}
	deletes := func() int {
		n := 0
		for _, a := range e.kube.Actions() {
			if a.GetVerb() == "list" && a.GetResource().Resource == "deployments" {
				n++
			}
		}
		return n
	}
	before := deletes()
	e.poll()
	if deletes() != before {
		t.Errorf("an ended job's shadow was deleted again")
	}
	// A restarted executor deletes it once more, finding nothing.
	e.restart()
	e.poll()
	if deletes() != before+1 {
		t.Errorf("a restarted executor deleted the shadow %d times", deletes()-before)
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

// TestShadowOfSinglePod: the shadow carries only its own labels, so no Service
// of the namespace selects it (the owner pod carries its IP); its pod's IP and
// readiness go to the request's status.
func TestShadowOfSinglePod(t *testing.T) {
	e := newExecutorEnv(t)
	e.d.Config.Shadow = shadowBlock()
	m, err := NewShadowManager(e.kube, e.d.Config)
	if err != nil {
		t.Fatal(err)
	}
	e.d.Shadow = m
	ctx := context.Background()

	req := testRequest(1)
	req.Spec.Owner = v1alpha1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "nb-0", UID: "0b6c-pod"}
	req.Spec.PodSets[0].Template.Labels = map[string]string{"statefulset": "nb", "kueue.x-k8s.io/queue-name": "hpc-a-proj42"}
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
	if labels[shadow.LabelShadowOf] != executorWorkload {
		t.Errorf("shadow pod labels %v, want the shadow's", labels)
	}
	for _, k := range []string{"statefulset", "kueue.x-k8s.io/queue-name"} {
		if _, ok := labels[k]; ok {
			t.Errorf("shadow pod carries %s: %v", k, labels)
		}
	}
	if list, _ := e.kube.DiscoveryV1().EndpointSlices(executorNS).List(ctx, metav1.ListOptions{}); len(list.Items) != 0 {
		t.Errorf("the shadow published %d EndpointSlices", len(list.Items))
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

// TestBatchJobHasNoShadow: a job that forwards no port and opens no reverse
// tunnel gets no shadow while it runs, and none is deleted when it ends.
func TestBatchJobHasNoShadow(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(testRequest(1))
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	if q := e.poll(); state(q) != "running" || q.Status.Shadow != nil {
		t.Fatalf("running: state %s, shadow %+v", state(q), q.Status.Shadow)
	}
	e.setStatus(0, "node07", terminatedContainer(executorCtn, 0, "Completed", time.Now(), time.Now()))
	if q := e.poll(); state(q) != "succeeded" || q.Status.Shadow != nil {
		t.Fatalf("completed: state %s, shadow %+v", state(q), q.Status.Shadow)
	}
	if len(e.shadow.ensured) != 0 || len(e.shadow.deleted) != 0 {
		t.Fatalf("shadows ensured %+v, deleted %v", e.shadow.ensured, e.shadow.deleted)
	}
}

// TestShadowDeletedOnceAtJobEnd: the shadow of a job that forwards a port goes
// when the job ends, also when the executor first sees the request ended.
func TestShadowDeletedOnceAtJobEnd(t *testing.T) {
	e := newExecutorEnv(t)
	req := testRequest(1)
	req.Spec.PodSets[0].Template.Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 8888}}
	e.submit(req)
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	if q := e.poll(); q.Status.Shadow == nil || len(e.shadow.ensured) != 1 {
		t.Fatalf("running: shadow %+v, ensured %d", q.Status.Shadow, len(e.shadow.ensured))
	}
	e.setStatus(0, "node07", terminatedContainer(executorCtn, 0, "Completed", time.Now(), time.Now()))
	if q := e.poll(); q.Status.Shadow != nil || !slices.Equal(e.shadow.deleted, []string{executorWorkload}) {
		t.Fatalf("completed: shadow %+v, deleted %v", q.Status.Shadow, e.shadow.deleted)
	}
	if e.poll(); len(e.shadow.ensured) != 1 || len(e.shadow.deleted) != 1 {
		t.Fatalf("ended job polled again: ensured %d, deleted %v", len(e.shadow.ensured), e.shadow.deleted)
	}
	e.restart()
	if e.poll(); len(e.shadow.ensured) != 1 || len(e.shadow.deleted) != 2 {
		t.Fatalf("after a restart: ensured %d, deleted %v", len(e.shadow.ensured), e.shadow.deleted)
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

func containerNames(dep *appsv1.Deployment) string {
	var names []string
	for _, c := range dep.Spec.Template.Spec.Containers {
		names = append(names, c.Name)
	}
	return strings.Join(names, ",")
}
