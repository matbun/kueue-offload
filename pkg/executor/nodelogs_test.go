package executor

import (
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
	v1 "k8s.io/api/core/v1"

	"github.com/matbun/kueue-offload/pkg/executorapi"
)

// The options kubectl logs passes through the virtual node reach the plugin;
// those an offloaded container cannot honour, or the interLink API server
// refuses together, are refused.
func TestGetLogsOptions(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(testRequest(1))
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	e.poll()
	e.setLog(0, executorCtn, "hello\n")

	since := time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	query := func(params ...string) string {
		v := url.Values{executorapi.LogsWorkloadUID: {executorWorkload}, executorapi.LogsRank: {"0"}}
		for i := 0; i+1 < len(params); i += 2 {
			v.Set(params[i], params[i+1])
		}
		return executorapi.GetLogsPath + "?" + v.Encode()
	}
	rec := e.do(http.MethodGet, query(executorapi.LogsSinceTime, since.Format(time.RFC3339), executorapi.LogsLimitBytes, "100", executorapi.LogsTimestamps, "true"))
	if rec.Code != http.StatusOK || rec.Body.String() != "hello\n" {
		t.Fatalf("with options = %d %q", rec.Code, rec.Body.String())
	}
	var got []commonIL.ContainerLogOpts
	e.plugin.with(func(p *fakePlugin) { got = slices.Clone(p.logOpts) })
	if len(got) != 1 || !got[0].SinceTime.Equal(since) || got[0].LimitBytes != 100 || !got[0].Timestamps || got[0].Follow || got[0].Previous {
		t.Fatalf("plugin got %+v", got)
	}

	for name, tc := range map[string]struct {
		target, body string
	}{
		"previous":              {query(executorapi.LogsPrevious, "true"), "an offloaded container has no previous instance\n"},
		"sinceTime not RFC3339": {query(executorapi.LogsSinceTime, "yesterday"), "invalid sinceTime \"yesterday\": RFC 3339 is required\n"},
		"bad limitBytes":        {query(executorapi.LogsLimitBytes, "-1"), "invalid limitBytes \"-1\"\n"},
		"tail and limitBytes":   {query(executorapi.LogsTail, "1", executorapi.LogsLimitBytes, "5"), "tail and limitBytes cannot both be set\n"},
		"two windows": {query(executorapi.LogsSinceSeconds, "5", executorapi.LogsSinceTime, since.Format(time.RFC3339)),
			"sinceSeconds and sinceTime cannot both be set\n"},
	} {
		if rec := e.do(http.MethodGet, tc.target); rec.Code != http.StatusBadRequest || rec.Body.String() != tc.body {
			t.Errorf("%s = %d %q, want 400 %q", name, rec.Code, rec.Body.String(), tc.body)
		}
	}
	e.plugin.with(func(p *fakePlugin) { got = slices.Clone(p.logOpts) })
	if len(got) != 1 {
		t.Errorf("refused reads reached the plugin: %+v", got[1:])
	}
}

// A rank that waits for the site has no log yet: read without follow it is
// refused, as a kubelet refuses the log of a container that waits to start.
func TestGetLogsOfPendingRank(t *testing.T) {
	e := newExecutorEnv(t)
	e.plugin.with(func(p *fakePlugin) {
		p.capabilities = nil
		p.createCodes = []int{http.StatusOK, http.StatusServiceUnavailable, http.StatusServiceUnavailable}
		p.createText = "QOSMaxSubmitJobPerUserLimit"
	})
	e.submit(markIndependent(testRequest(2)))
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	if q := e.poll(); !started(q) || !rankPending(q, 1) {
		t.Fatalf("rank 0 runs, rank 1 waits: %+v", q.Status)
	}
	e.setLog(0, executorCtn, "zero\n")
	if rec := e.do(http.MethodGet, executorapi.GetLogsPath+"?workloadUID="+executorWorkload+"&rank=0"); rec.Code != http.StatusOK || rec.Body.String() != "zero\n" {
		t.Fatalf("rank 0 = %d %q", rec.Code, rec.Body.String())
	}
	rec := e.do(http.MethodGet, executorapi.GetLogsPath+"?workloadUID="+executorWorkload+"&rank=1")
	if rec.Code != http.StatusBadRequest || rec.Body.String() != "container main of rank 1 is waiting to start\n" {
		t.Fatalf("rank 1 = %d %q", rec.Code, rec.Body.String())
	}
}

// Each rank's status carries its compute node and its init containers, which the
// virtual node shows on the member's pod.
func TestRanksCarryNodeAndInitContainers(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(testRequest(2))
	start := time.Now().Add(-time.Minute)
	fetch := terminatedContainer("fetch", 0, "Completed", start, start)
	for i, node := range []string{"node07", "node08"} {
		e.plugin.with(func(p *fakePlugin) {
			p.status[memberUID(executorWorkload, i)] = commonIL.PodStatus{JobID: executorJobID, NodeName: node,
				InitContainers: []v1.ContainerStatus{fetch}, Containers: []v1.ContainerStatus{runningContainer(executorCtn, start)}}
		})
	}
	q := e.poll()
	if len(q.Status.Ranks) != 2 {
		t.Fatalf("ranks %+v", q.Status.Ranks)
	}
	for i, node := range []string{"node07", "node08"} {
		r := q.Status.Ranks[i]
		if r.NodeName != node || len(r.InitContainers) != 1 || r.InitContainers[0].Name != "fetch" || r.InitContainers[0].State.Terminated == nil ||
			len(r.Containers) != 1 || r.Containers[0].State.Running == nil {
			t.Errorf("rank %d: node %q, init %+v, containers %+v", i, r.NodeName, r.InitContainers, r.Containers)
		}
	}
}
