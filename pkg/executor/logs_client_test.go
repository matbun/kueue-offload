package executor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matbun/kueue-offload/pkg/shadow"
)

// syncBuffer is a bytes.Buffer safe to read while the command writes it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunLogsCommandReconnectsThenIdles(t *testing.T) {
	old := logsRetryDelay
	logsRetryDelay = 10 * time.Millisecond
	t.Cleanup(func() { logsRetryDelay = old })

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("workloadUID") != executorWorkload || q.Get("follow") != "true" || q.Get("container") != executorCtn {
			http.Error(w, "bad query "+r.URL.RawQuery, http.StatusBadRequest)
			return
		}
		switch calls.Add(1) {
		case 1:
			// Breaks off: the client sees less than the declared length.
			w.Header().Set("Content-Length", strconv.Itoa(100))
			_, _ = w.Write([]byte("line1\n"))
		default:
			_, _ = w.Write([]byte("line1\nline2\n"))
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var stdout, stderr syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- RunLogsCommand(ctx, []string{"--executor", srv.URL + "/", "--workload", executorWorkload, "--container", executorCtn, "--follow"}, &stdout, &stderr)
	}()

	want := "line1\n" + LogsReconnectBanner + "\nline1\nline2\n"
	deadline := time.Now().Add(5 * time.Second)
	for stdout.String() != want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := stdout.String(); got != want {
		t.Fatalf("stdout = %q, want %q (stderr %q)", got, want, stderr.String())
	}
	select {
	case err := <-done:
		t.Fatalf("returned after a clean end of the stream: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("%d requests, want 2: a clean end must not reconnect", n)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("did not return when its context ended")
	}
}

func TestRunLogsCommandWithoutFollowReturns(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("all\n"))
	}))
	defer srv.Close()
	var stdout, stderr syncBuffer
	if err := RunLogsCommand(context.Background(), []string{"--executor", srv.URL, "--workload", executorWorkload}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "all\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if err := RunLogsCommand(context.Background(), []string{"--executor", srv.URL}, &stdout, &stderr); err == nil {
		t.Fatal("accepted a call without --workload")
	}

	// As kubectl exec runs it: an error ends the command instead of a retry.
	gone := httptest.NewServer(http.NotFoundHandler())
	defer gone.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := RunLogsCommand(ctx, []string{"--executor", gone.URL, "--workload", executorWorkload}, &stdout, &stderr); err == nil || ctx.Err() != nil {
		t.Fatalf("answered 404: err %v after %v", err, ctx.Err())
	}
}

// A log larger than the kubelet keeps in a file is followed by its end once more,
// after the kubelet's rotation check: the newest file, all kubectl logs shows,
// can be empty after the stream. The end starts at a line and fits in
// logsTailSize as the kubelet stores it.
func TestRunLogsCommandPrintsEndOfLargeLog(t *testing.T) {
	old := logsTailDelay
	logsTailDelay = 10 * time.Millisecond
	t.Cleanup(func() { logsTailDelay = old })

	var log strings.Builder
	for i := 0; i < 100000; i++ {
		fmt.Fprintf(&log, "line %06d %s\n", i, strings.Repeat("x", 60))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(log.String()))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var stdout, stderr syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- RunLogsCommand(ctx, []string{"--executor", srv.URL, "--workload", executorWorkload, "--follow"}, &stdout, &stderr)
	}()
	marker := log.String() + LogsTailBanner + "\n"
	deadline := time.Now().Add(5 * time.Second)
	for o := stdout.String(); (len(o) <= len(marker) || !strings.HasSuffix(o, "line 099999 "+strings.Repeat("x", 60)+"\n")) && time.Now().Before(deadline); o = stdout.String() {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	out := stdout.String()
	end, ok := strings.CutPrefix(out, marker)
	if !ok {
		t.Fatalf("output is not the log then %q: %d bytes, ends %q (stderr %q)", LogsTailBanner, len(out), out[max(0, len(out)-200):], stderr.String())
	}
	lines := strings.Count(end, "\n")
	if !strings.HasPrefix(end, "line ") || !strings.HasSuffix(log.String(), end) || lines < 1000 || len(end)+criLineOverhead*lines > logsTailSize {
		t.Fatalf("end of the log: %d lines, %d bytes, starts %q", lines, len(end), end[:min(len(end), 40)])
	}
}

// A small log is not printed twice, and without flags the command reads the
// executor and the workload from the job-logs container's environment, as
// "kubectl exec ... -- /executor logs" runs it.
func TestRunLogsCommandFromEnvironment(t *testing.T) {
	old := logsTailDelay
	logsTailDelay = 10 * time.Millisecond
	t.Cleanup(func() { logsTailDelay = old })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("workloadUID") != executorWorkload {
			http.Error(w, "bad query "+r.URL.RawQuery, http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte("all\n"))
	}))
	defer srv.Close()
	t.Setenv(shadow.JobLogsExecutorEnv, srv.URL)
	t.Setenv(shadow.JobLogsWorkloadEnv, executorWorkload)

	var stdout, stderr syncBuffer
	if err := RunLogsCommand(context.Background(), nil, &stdout, &stderr); err != nil || stdout.String() != "all\n" {
		t.Fatalf("stdout %q, err %v, stderr %q", stdout.String(), err, stderr.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	stdout = syncBuffer{}
	_ = RunLogsCommand(ctx, []string{"--follow"}, &stdout, &stderr)
	if stdout.String() != "all\n" {
		t.Fatalf("followed small log: stdout %q", stdout.String())
	}
}
