package executor

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/log"
	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
	v1 "k8s.io/api/core/v1"
)

// logsPollInterval is how often a followed workload log checks whether its job
// started.
var logsPollInterval = time.Second

// logsReopenDelay is how long a followed log waits before it opens a member's
// stream again after the stream ended while the container still runs.
var logsReopenDelay = 5 * time.Second

// GetLogsHandler is /getLogs: the workload's logs, each member's read through the
// interLink API server's /getLogs.
//
// Query: workloadUID, container (default: each member's first container), rank,
// follow, tail, sinceSeconds. With rank, that member's log as the plugin returns it.
// Without rank, the lines of every member that has the container, prefixed
// "[rank N] " when there are more than one member: rank by rank, or with follow as
// they arrive, from the time the job started until every member's stream ended.
// With follow, tail applies to each member.
func (d *Executor) GetLogsHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	uid := q.Get("workloadUID")
	if uid == "" {
		http.Error(w, "workloadUID is required", http.StatusBadRequest)
		return
	}
	req := d.current(r.Context(), uid)
	if req == nil {
		http.Error(w, "no job for workload "+uid, http.StatusNotFound)
		return
	}
	container := q.Get("container")
	intParam := func(name string) (int, bool) {
		raw := q.Get(name)
		if raw == "" {
			return 0, true
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			http.Error(w, "invalid "+name+" "+strconv.Quote(raw), http.StatusBadRequest)
			return 0, false
		}
		return n, true
	}
	tail, ok := intParam("tail")
	if !ok {
		return
	}
	since, ok := intParam("sinceSeconds")
	if !ok {
		return
	}
	follow := q.Get("follow") == "true"
	members := memberPods(jobRequestOf(req))
	logs := memberLogs(members, container)
	if len(logs) == 0 {
		http.Error(w, "no container "+container+" in workload "+uid, http.StatusBadRequest)
		return
	}
	opts := commonIL.ContainerLogOpts{Tail: tail, Follow: follow, SinceSeconds: since}

	if q.Has("rank") {
		rank, ok := intParam("rank")
		if !ok {
			return
		}
		if rank >= len(members) {
			http.Error(w, fmt.Sprintf("rank %d: workload %s has %d member(s)", rank, uid, len(members)), http.StatusBadRequest)
			return
		}
		i := slices.IndexFunc(logs, func(l memberLog) bool { return l.rank == rank })
		if i < 0 {
			http.Error(w, fmt.Sprintf("rank %d of workload %s has no container %s", rank, uid, container), http.StatusBadRequest)
			return
		}
		if follow && !d.waitForStart(r.Context(), uid) {
			return
		}
		if follow {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			flushWriter{w}.Flush()
			if err := d.followRank(r.Context(), uid, logs[i], opts, flushWriter{w}, ""); err != nil && r.Context().Err() == nil {
				log.L.Warning("executor: logs of workload ", uid, " rank ", rank, ": ", err)
				panic(http.ErrAbortHandler)
			}
			return
		}
		body, err := d.InterLink.Logs(r.Context(), uid, logs[i].request(opts))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer body.Close()
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(flushWriter{w}, body)
		return
	}

	prefix := len(members) > 1
	if !follow {
		// The tail is of the whole text, rank by rank.
		whole := opts
		whole.Tail = 0
		var all bytes.Buffer
		for _, l := range logs {
			if rankPending(req, l.rank) {
				continue
			}
			body, err := d.InterLink.Logs(r.Context(), uid, l.request(whole))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			err = copyLines(&all, body, l.prefix(prefix))
			body.Close()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(lastLines(all.Bytes(), tail))
		return
	}

	if !d.waitForStart(r.Context(), uid) {
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	flushWriter{w}.Flush()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	out := &lockedWriter{w: flushWriter{w}}
	errs := make(chan error, len(logs))
	var wg sync.WaitGroup
	for _, l := range logs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.followRank(ctx, uid, l, opts, out, l.prefix(prefix)); err != nil {
				errs <- fmt.Errorf("rank %d: %w", l.rank, err)
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errs)
	err := <-errs
	if err != nil && r.Context().Err() == nil {
		log.L.Warning("executor: logs of workload ", uid, ": ", err)
		// A broken stream is not an end: the client sees the connection fail and
		// asks again.
		panic(http.ErrAbortHandler)
	}
}

// errStreamEnded is a followed stream that ended while its container still runs
// and cannot be resumed, because the request asked for a tail or a time window.
var errStreamEnded = errors.New("a stream ended before the job")

// followRank copies one member's followed log to out until its container ended.
// A stream that ends before that (a plugin without Follow, or the API server
// losing the plugin's stream, which it ends as if it were done) is opened again
// after logsReopenDelay with the lines already copied skipped, so each line
// reaches the caller once. A last line without a newline is held back until the
// container ended, since it may still grow.
func (d *Executor) followRank(ctx context.Context, uid string, l memberLog, opts commonIL.ContainerLogOpts, out io.Writer, prefix string) error {
	for q := d.current(ctx, uid); q != nil && rankPending(q, l.rank); q = d.current(ctx, uid) {
		// Independent members wait for the site before they have a log.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(logsReopenDelay):
		}
	}
	resumable := opts.Tail == 0 && opts.SinceSeconds == 0
	copied, reopened := 0, false
	var partial []byte
	for {
		body, err := d.InterLink.Logs(ctx, uid, l.request(opts))
		if err != nil {
			return err
		}
		var n int
		n, partial, err = copyNewLines(out, body, prefix, copied)
		body.Close()
		if err != nil {
			return err
		}
		copied = max(copied, n)
		if d.logsEnded(ctx, uid, []memberLog{l}) {
			if reopened && resumable {
				// The last stream may predate the container's last writes.
				final := opts
				final.Follow = false
				if body, err := d.InterLink.Logs(ctx, uid, l.request(final)); err == nil {
					_, partial, err = copyNewLines(out, body, prefix, copied)
					body.Close()
					if err != nil {
						return err
					}
				}
			}
			if len(partial) > 0 {
				_, err = out.Write(append(append([]byte(prefix), partial...), '\n'))
			}
			return err
		}
		if !resumable {
			return errStreamEnded
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(logsReopenDelay):
		}
		reopened = true
	}
}

// copyNewLines copies the complete lines of r after the first skip to w, each
// with prefix and as one write, so the lines of several members interleave whole.
// It returns how many complete lines r had and the last line if it has no newline.
func copyNewLines(w io.Writer, r io.Reader, prefix string, skip int) (int, []byte, error) {
	br := bufio.NewReaderSize(r, 64<<10)
	n := 0
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] == '\n' {
			if n >= skip {
				if _, werr := w.Write(append([]byte(prefix), line...)); werr != nil {
					return n, nil, werr
				}
			}
			n++
		} else if len(line) > 0 && errors.Is(err, io.EOF) {
			return n, line, nil
		}
		if errors.Is(err, io.EOF) {
			return n, nil, nil
		}
		if err != nil {
			return n, nil, err
		}
	}
}

// logsEnded reports whether the followed containers have nothing more to write:
// the workload's request is gone, its job ended, or the plugin reports every
// one of them terminated.
func (d *Executor) logsEnded(ctx context.Context, uid string, logs []memberLog) bool {
	if q := d.current(ctx, uid); q == nil || jobEnded(q) {
		return true
	}
	pods := make([]*v1.Pod, 0, len(logs))
	for _, l := range logs {
		pod := l.pod.DeepCopy()
		pod.Status.Phase = v1.PodRunning
		pods = append(pods, pod)
	}
	statuses, err := d.InterLink.Status(ctx, uid, pods)
	if err != nil {
		return false
	}
	terminated := map[string]bool{}
	for _, s := range statuses {
		for _, c := range append(append([]v1.ContainerStatus{}, s.InitContainers...), s.Containers...) {
			if c.State.Terminated != nil {
				terminated[s.PodUID+"/"+c.Name] = true
			}
		}
	}
	for _, l := range logs {
		if !terminated[string(l.pod.UID)+"/"+l.container] {
			return false
		}
	}
	return true
}

// waitForStart holds a followed log until the job ran, so the plugin is not asked
// to follow a log that does not exist yet. It is false when the caller went away
// or the workload's request is gone.
func (d *Executor) waitForStart(ctx context.Context, uid string) bool {
	for {
		q := d.current(ctx, uid)
		switch {
		case q == nil:
			return false
		case started(q):
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(logsPollInterval):
		}
	}
}

// memberLog is the log of one container of one member.
type memberLog struct {
	rank      int
	pod       *v1.Pod
	container string
}

func (l memberLog) request(opts commonIL.ContainerLogOpts) commonIL.LogStruct {
	return commonIL.LogStruct{
		Namespace:     l.pod.Namespace,
		PodUID:        string(l.pod.UID),
		PodName:       l.pod.Name,
		ContainerName: l.container,
		Opts:          opts,
	}
}

func (l memberLog) prefix(on bool) string {
	if !on {
		return ""
	}
	return "[rank " + strconv.Itoa(l.rank) + "] "
}

func hasContainer(list []v1.Container, name string) bool {
	return slices.ContainsFunc(list, func(c v1.Container) bool { return c.Name == name })
}

// memberLogs lists the members that have the container, or every member's first
// container when none is named: members of different PodSets can have different
// containers.
func memberLogs(members []*v1.Pod, container string) []memberLog {
	var logs []memberLog
	for i, m := range members {
		l := memberLog{rank: i, pod: m, container: container}
		switch {
		case container == "":
			l.container = m.Spec.Containers[0].Name
		case hasContainer(m.Spec.Containers, container), hasContainer(m.Spec.InitContainers, container):
		default:
			continue
		}
		logs = append(logs, l)
	}
	return logs
}

// copyLines copies r to w line by line, each line with prefix. A last line without
// a newline gets one. Each line is one write, so the lines of several members
// interleave whole.
func copyLines(w io.Writer, r io.Reader, prefix string) error {
	br := bufio.NewReaderSize(r, 64<<10)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				line = append(line, '\n')
			}
			if _, werr := w.Write(append([]byte(prefix), line...)); werr != nil {
				return werr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// lastLines keeps the last n lines of text, all of it when n is 0.
func lastLines(text []byte, n int) []byte {
	if n <= 0 {
		return text
	}
	lines := strings.SplitAfter(string(text), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return []byte(strings.Join(lines, ""))
}

// flushWriter flushes after every write, so a followed line reaches the client at
// once.
type flushWriter struct{ w http.ResponseWriter }

func (f flushWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	f.Flush()
	return n, err
}

func (f flushWriter) Flush() {
	if fl, ok := f.w.(http.Flusher); ok {
		fl.Flush()
	}
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
