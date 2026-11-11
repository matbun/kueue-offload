package executor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/matbun/kueue-offload/pkg/executorapi"
)

// /exec is refused without a plugin address and for a member that does not
// run; a running member's exec reaches the plugin's /exec, protocol upgrade
// and all, for the member's pod at the plugin and its compute node.
func TestExecToThePlugin(t *testing.T) {
	e := newExecutorEnv(t)
	query := url.Values{executorapi.ExecWorkloadUID: {executorWorkload}, executorapi.ExecRank: {"0"},
		"command": {"hostname", "-f"}, "output": {"1"}}
	target := executorapi.ExecPath + "?" + query.Encode()
	if rec := e.do(http.MethodPost, target); rec.Code != http.StatusNotImplemented {
		t.Errorf("without PluginURL = %d %q", rec.Code, rec.Body.String())
	}

	var got url.Values
	plugin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/exec" {
			http.NotFound(w, r)
			return
		}
		got = r.URL.Query()
		r = mux.SetURLVars(r, map[string]string{"namespace": got.Get("namespace"), "pod": got.Get("pod"), "container": got.Get("container")})
		vkapi.HandleContainerExec(func(_ context.Context, _, _, _ string, cmd []string, a vkapi.AttachIO) error {
			fmt.Fprintf(a.Stdout(), "ran %s\n", strings.Join(cmd, " "))
			return nil
		})(w, r)
	}))
	t.Cleanup(plugin.Close)
	e.d.Config.InterLink.PluginURL = plugin.URL

	e.submit(testRequest(1))
	if rec := e.do(http.MethodPost, target); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "is not running") {
		t.Errorf("before the start = %d %q", rec.Code, rec.Body.String())
	}
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	e.poll()

	srv := httptest.NewServer(e.mux)
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL + target)
	exec, err := remotecommand.NewSPDYExecutor(&rest.Config{Host: srv.URL}, http.MethodPost, u)
	if err != nil {
		t.Fatal(err)
	}
	out := &bytes.Buffer{}
	if err := exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{Stdout: out}); err != nil {
		t.Fatal(err)
	}
	if out.String() != "ran hostname -f\n" {
		t.Errorf("output %q", out.String())
	}
	want := url.Values{"namespace": {testRequest(1).Namespace}, "pod": {memberUID(executorWorkload, 0)}, "container": {executorCtn},
		"node": {"node07"}, "command": {"hostname", "-f"}, "output": {"1"}}
	if got.Encode() != want.Encode() {
		t.Errorf("plugin got %v, want %v", got, want)
	}
}
