package executor

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/matbun/kueue-offload/pkg/shadow"
)

// LogsReconnectBanner is printed before the log is copied again after a lost
// stream.
const LogsReconnectBanner = "--- log stream reconnected, full log follows ---"

// LogsTailBanner is printed before the end of a large log is printed again.
const LogsTailBanner = "--- the end of the log again, the kubelet keeps only part of it; the whole log: kubectl exec <shadow pod> -c " +
	shadow.JobLogsContainer + " -- " + shadow.JobLogsCommand + " ---"

// logsRetryDelay is how long the logs command waits before it reconnects.
var logsRetryDelay = 5 * time.Second

// kubectl logs shows a container's newest log file only, which the kubelet
// rotates once it holds containerLogMaxSize (10Mi by default), with
// criLineOverhead bytes of timestamp and stream tags per line. A log that filled
// it can leave the newest file empty when its stream ends, so its end, at most
// logsTailSize bytes as stored, is printed again logsTailDelay later: after the
// kubelet's next rotation check (every 10 s by default).
const (
	kubeletLogMaxSize = 10 << 20
	criLineOverhead   = 40
	logsTailSize      = 256 << 10
)

var logsTailDelay = 30 * time.Second

// RunLogsCommand is "logs [--executor <url>] [--workload <uid>] [--container <c>]
// [--follow]": it copies the workload's /getLogs to stdout. The executor and the
// workload default to the environment of the shadow's job-logs container.
// Without --follow it copies the log once, and fails on an error. With --follow,
// when the stream breaks before its end it waits, prints LogsReconnectBanner and
// copies the whole log again, and a stream that ended cleanly leaves the command
// idle until ctx is done, so the container keeps its output and is not
// restarted; when the output did not fit in the kubelet's log file, the command
// first prints LogsTailBanner and the log's end again.
func RunLogsCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	executor := fs.String("executor", os.Getenv(shadow.JobLogsExecutorEnv), "URL of the executor")
	workload := fs.String("workload", os.Getenv(shadow.JobLogsWorkloadEnv), "UID of the workload")
	container := fs.String("container", "", "container (default: the first one)")
	follow := fs.Bool("follow", false, "follow the log until the job ends")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *executor == "" || *workload == "" {
		return errors.New("logs: --executor and --workload are required")
	}
	q := url.Values{"workloadUID": {*workload}}
	if *container != "" {
		q.Set("container", *container)
	}
	if *follow {
		q.Set("follow", "true")
	}
	target := strings.TrimRight(*executor, "/") + "/getLogs?" + q.Encode()

	out := &tailWriter{out: stdout}
	connected := false
	for {
		out.restart()
		ok, err := copyLogsOnce(ctx, target, out)
		if err == nil {
			if !*follow {
				return nil
			}
			if out.stored > kubeletLogMaxSize {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(logsTailDelay):
				}
				out.printEnd()
			}
			<-ctx.Done()
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		} else if !*follow {
			return err
		}
		fmt.Fprintln(stderr, "logs:", err)
		connected = connected || ok
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(logsRetryDelay):
		}
		if connected {
			fmt.Fprintln(out, LogsReconnectBanner)
		}
	}
}

// copyLogsOnce copies one /getLogs answer to out. connected is set once the
// executor answered 200; err is nil only for a stream that reached its end.
func copyLogsOnce(ctx context.Context, target string, out io.Writer) (connected bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return false, fmt.Errorf("executor answered %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		return true, err
	}
	return true, nil
}

// tailWriter writes to out, counts the bytes the kubelet stores of what it
// wrote, and keeps the end of the copy since restart.
type tailWriter struct {
	out    io.Writer
	stored int
	tail   []byte
	// cut: tail lost its start, so its first line is partial.
	cut bool
}

func (w *tailWriter) Write(p []byte) (int, error) {
	n, err := w.out.Write(p)
	w.stored += n + criLineOverhead*bytes.Count(p[:n], []byte{'\n'})
	w.tail = append(w.tail, p[:n]...)
	if len(w.tail) > 2*logsTailSize {
		w.tail = append(w.tail[:0], w.tail[len(w.tail)-logsTailSize:]...)
		w.cut = true
	}
	return n, err
}

func (w *tailWriter) restart() {
	w.tail, w.cut = w.tail[:0], false
}

// printEnd prints LogsTailBanner and the last whole lines of the copy that the
// kubelet stores in at most logsTailSize bytes.
func (w *tailWriter) printEnd() {
	t := w.tail
	start, size := len(t), 0
	for start > 0 {
		line := bytes.LastIndexByte(t[:start-1], '\n') + 1
		if line == 0 && w.cut {
			break
		}
		if size += start - line + criLineOverhead; size > logsTailSize {
			break
		}
		start = line
	}
	if len(t) > 0 && t[len(t)-1] != '\n' {
		fmt.Fprintln(w.out)
	}
	fmt.Fprintln(w.out, LogsTailBanner)
	_, _ = w.out.Write(t[start:])
}
