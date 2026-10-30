package executor

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/containerd/containerd/log"
)

// The tests drive the executor through a real interLink API server, built once
// from INTERLINK_API_SERVER (a binary) or INTERLINK_SRC (a checkout of interLink
// with PodStatus.SocketDir, default ../../../interLink-hardening). Without either
// the tests that submit are skipped.
var (
	apiServerBin     string
	apiServerMissing string
)

func TestMain(m *testing.M) {
	_ = log.SetLevel("warn")
	dir, err := os.MkdirTemp("", "kueue-offload-interlink-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	apiServerBin, apiServerMissing = buildAPIServer(dir)
	if apiServerBin == "" && apiServerMissing == "" {
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// buildAPIServer returns the API server binary, or why there is none. A source
// that does not build is an error: it returns two empty strings.
func buildAPIServer(dir string) (bin, missing string) {
	if p := os.Getenv("INTERLINK_API_SERVER"); p != "" {
		return p, ""
	}
	src := os.Getenv("INTERLINK_SRC")
	if src == "" {
		src = filepath.Join("..", "..", "..", "interLink-hardening")
	}
	if _, err := os.Stat(filepath.Join(src, "cmd", "interlink")); err != nil {
		return "", "no interLink source at " + src + ": set INTERLINK_SRC or INTERLINK_API_SERVER"
	}
	bin = filepath.Join(dir, "interlink")
	cmd := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", bin, "./cmd/interlink")
	cmd.Dir = src
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building the interLink API server in %s: %v\n%s", src, err, out)
		return "", ""
	}
	return bin, ""
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startAPIServer runs an interLink API server in front of the plugin at pluginURL
// and returns its URL.
func startAPIServer(t *testing.T, pluginURL string) string {
	t.Helper()
	if apiServerBin == "" {
		t.Skip(apiServerMissing)
	}
	plugin, err := url.Parse(pluginURL)
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(freePort(t))
	cfg := fmt.Sprintf("InterlinkAddress: http://127.0.0.1\nInterlinkPort: %q\nSidecarURL: http://%s\nSidecarPort: %q\nErrorsOnlyLogging: true\n",
		port, plugin.Hostname(), plugin.Port())
	path := filepath.Join(t.TempDir(), "InterLinkConfig.yaml")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	var out syncBuffer
	cmd := exec.Command(apiServerBin)
	cmd.Env = append(os.Environ(), "INTERLINKCONFIGPATH="+path)
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-exited
		if t.Failed() {
			t.Logf("interLink API server output:\n%s", out.String())
		}
	})
	base := "http://127.0.0.1:" + port
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := http.Get(base + "/version")
		if err == nil {
			resp.Body.Close()
			return base
		}
		select {
		case <-exited:
			t.Fatalf("the interLink API server exited:\n%s", out.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the interLink API server did not answer: %v\n%s", err, out.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
