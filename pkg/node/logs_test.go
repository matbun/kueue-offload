package node

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/executorapi"
)

// anyone and allowAll let every request through to the kubelet API's routes.
var (
	anyone = authenticator.RequestFunc(func(*http.Request) (*authenticator.Response, bool, error) {
		return &authenticator.Response{User: &user.DefaultInfo{Name: "kubernetes"}}, true, nil
	})
	allowAll = authorizer.AuthorizerFunc(func(context.Context, authorizer.Attributes) (authorizer.Decision, string, error) {
		return authorizer.DecisionAllow, "", nil
	})
)

func testConfig() *v1alpha1.OffloadConfig {
	return &v1alpha1.OffloadConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "hpc-a", UID: "uid-cfg"},
		Spec:       v1alpha1.OffloadConfigSpec{Workers: []string{"hpc-a"}, ExecutorService: "kueue-offload-executor", ExecutorPort: 8000},
	}
}

// logsEnv is a provider whose executor is executor, with the pod of rank 1 of
// the test Workload on the virtual node.
func logsEnv(t *testing.T, executor http.HandlerFunc) (*Provider, *[]url.Values) {
	t.Helper()
	var queries []url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != executorapi.GetLogsPath {
			http.NotFound(w, r)
			return
		}
		queries = append(queries, r.URL.Query())
		executor(w, r)
	}))
	t.Cleanup(srv.Close)
	var calls []apiCall
	c := fakeClient(t, &calls, testPod("train-1", "1"), testWorkload(), testConfig())
	p := &Provider{Reader: c, executorURL: func(cfg *v1alpha1.OffloadConfig, namespace string) string {
		if cfg.Name != "hpc-a" || namespace != testNS {
			t.Errorf("executor of OffloadConfig %s in namespace %s", cfg.Name, namespace)
		}
		return srv.URL
	}}
	return p, &queries
}

// The executor gets the pod's Workload UID and rank, the container and every
// option kubectl logs passes, and its answer is streamed back.
func TestLogsParams(t *testing.T) {
	p, queries := logsEnv(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "epoch 1\n") })
	since := time.Date(2026, 11, 6, 9, 0, 0, 0, time.UTC)
	body, err := p.GetContainerLogs(context.Background(), testNS, "train-1", "main",
		vkapi.ContainerLogOpts{Follow: true, Tail: 20, SinceTime: since, Timestamps: true})
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(body)
	body.Close()
	if string(out) != "epoch 1\n" {
		t.Errorf("body %q", out)
	}
	want := url.Values{
		executorapi.LogsWorkloadUID: {"uid-wl"}, executorapi.LogsRank: {"1"}, executorapi.LogsContainer: {"main"},
		executorapi.LogsFollow: {"true"}, executorapi.LogsTail: {"20"}, executorapi.LogsSinceTime: {"2026-11-06T09:00:00Z"},
		executorapi.LogsTimestamps: {"true"},
	}
	if len(*queries) != 1 || (*queries)[0].Encode() != want.Encode() {
		t.Fatalf("queries %v, want %v", *queries, want)
	}

	if _, err := p.GetContainerLogs(context.Background(), testNS, "train-1", "main", vkapi.ContainerLogOpts{SinceSeconds: 30, LimitBytes: 512}); err != nil {
		t.Fatal(err)
	}
	want = url.Values{
		executorapi.LogsWorkloadUID: {"uid-wl"}, executorapi.LogsRank: {"1"}, executorapi.LogsContainer: {"main"},
		executorapi.LogsSinceSeconds: {"30"}, executorapi.LogsLimitBytes: {"512"},
	}
	if (*queries)[1].Encode() != want.Encode() {
		t.Errorf("query %v, want %v", (*queries)[1], want)
	}
}

// The executor's 404 is NotFound and its other 4xx InvalidInput, with its
// message; previous is refused without asking it.
func TestLogsErrors(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		check   func(error) bool
		message string
	}{
		{"not found", http.StatusNotFound, "no job for workload uid-wl", errdefs.IsNotFound, "no job for workload uid-wl"},
		{"waiting", http.StatusBadRequest, "container main of rank 1 is waiting to start", errdefs.IsInvalidInput, "waiting to start"},
		{"executor broken", http.StatusBadGateway, "the interLink API server cannot be reached",
			func(err error) bool { return !errdefs.IsInvalidInput(err) && !errdefs.IsNotFound(err) }, "answered 502"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, _ := logsEnv(t, func(w http.ResponseWriter, _ *http.Request) { http.Error(w, c.body, c.status) })
			_, err := p.GetContainerLogs(context.Background(), testNS, "train-1", "main", vkapi.ContainerLogOpts{})
			if err == nil || !c.check(err) || !strings.Contains(err.Error(), c.message) {
				t.Errorf("error %v, want one containing %q of the right kind", err, c.message)
			}
		})
	}
	p, queries := logsEnv(t, func(http.ResponseWriter, *http.Request) {})
	_, err := p.GetContainerLogs(context.Background(), testNS, "train-1", "main", vkapi.ContainerLogOpts{Previous: true})
	if !errdefs.IsInvalidInput(err) || len(*queries) != 0 {
		t.Errorf("previous: error %v, %d executor calls", err, len(*queries))
	}
	if _, err := p.GetContainerLogs(context.Background(), testNS, "gone", "main", vkapi.ContainerLogOpts{}); !errdefs.IsNotFound(err) {
		t.Errorf("pod not on the node: error %v, want NotFound", err)
	}
}

// The logs of a pod come from the executor of the pod's own namespace, by the
// OffloadConfig's Service and port or their defaults. A pod of another
// namespace that names this namespace's Workload is not that Workload's.
func TestLogsFromTheNamespaceExecutor(t *testing.T) {
	p := &Provider{}
	if got := p.baseURL(testConfig(), "bob"); got != "http://kueue-offload-executor.bob.svc:8000" {
		t.Errorf("executor URL %s", got)
	}
	cfg := testConfig()
	cfg.Spec.ExecutorService, cfg.Spec.ExecutorPort = "", 0
	if got := p.baseURL(cfg, testNS); got != "http://kueue-offload-executor."+testNS+".svc:8000" {
		t.Errorf("executor URL by default %s", got)
	}
	cfg.Spec.ExecutorService, cfg.Spec.ExecutorPort = "executor-b", 9000
	if got := p.baseURL(cfg, testNS); got != "http://executor-b."+testNS+".svc:9000" {
		t.Errorf("executor URL %s", got)
	}

	intruder := testPod("train-1", "1")
	intruder.Namespace = "bob"
	elsewhere := testPod("train-2", "0")
	elsewhere.Labels[v1alpha1.NodeLabel] = "kueue-offload-hpc-b"
	var calls []apiCall
	called := false
	p = &Provider{Reader: fakeClient(t, &calls, intruder, elsewhere, testWorkload(), testConfig()),
		executorURL: func(*v1alpha1.OffloadConfig, string) string { called = true; return "http://127.0.0.1:1" }}
	if _, err := p.GetContainerLogs(context.Background(), "bob", "train-1", "main", vkapi.ContainerLogOpts{}); !errdefs.IsNotFound(err) || called {
		t.Errorf("pod of namespace bob: error %v, executor called %v, want NotFound without a call", err, called)
	}
	if _, err := p.GetContainerLogs(context.Background(), testNS, "train-2", "main", vkapi.ContainerLogOpts{}); !errdefs.IsNotFound(err) || called {
		t.Errorf("pod labelled for another node: error %v, executor called %v, want NotFound without a call", err, called)
	}
}

// A followed log reaches kubectl line by line while the executor streams it,
// through the kubelet API's handler.
func TestLogsFollowStreams(t *testing.T) {
	release := make(chan struct{})
	p, _ := logsEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "line 1\n")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "line 2\n")
	})
	defer close(release)
	srv := httptest.NewServer((&Server{Provider: p}).handler(anyone, allowAll))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/containerLogs/"+testNS+"/train-1/main?follow=true", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	lines := make(chan string)
	go func() {
		sc := bufio.NewScanner(res.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	select {
	case l := <-lines:
		if l != "line 1" {
			t.Errorf("first line %q", l)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first line did not arrive while the executor streams")
	}
}

// kubectl logs --previous is a 400 of the kubelet API, without asking the
// executor.
func TestLogsPreviousThroughHandler(t *testing.T) {
	p, queries := logsEnv(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "epoch 1\n") })
	srv := httptest.NewServer((&Server{Provider: p}).handler(anyone, allowAll))
	defer srv.Close()
	res, err := http.Get(srv.URL + "/containerLogs/" + testNS + "/train-1/main?previous=true")
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusBadRequest || !strings.Contains(string(msg), "no previous instance") || len(*queries) != 0 {
		t.Errorf("previous: %d %q, %d executor calls, want 400 and none", res.StatusCode, msg, len(*queries))
	}
}

// exec answers that the plugin API has no exec call; the summary names the
// only node.
func TestExecAndStats(t *testing.T) {
	var calls []apiCall
	p := &Provider{Reader: fakeClient(t, &calls, testConfig())}
	if err := p.RunInContainer(context.Background(), testNS, "train-1", "main", []string{"sh"}, nil); err == nil || err.Error() != execUnsupported {
		t.Errorf("exec error %v", err)
	}
	s, err := p.GetStatsSummary(context.Background())
	if err != nil || s.Node.NodeName != testNode || s.Pods != nil {
		t.Errorf("summary %+v, %v", s, err)
	}
}
