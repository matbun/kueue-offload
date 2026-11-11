package executor

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/executorapi"
)

// execStreamParams are the kubelet's exec parameters, passed on as they are.
var execStreamParams = []string{"command", "tty", "input", "output", "error"}

// ExecHandler is /exec: a command in a container of a running member, with the
// kubelet's exec protocol, passed to the plugin's /exec (interLink's API
// server has no exec call) for the member's pod at the plugin and the compute
// node it runs on.
//
// Query (executorapi): workloadUID, rank, container (default: the member's
// first), then the kubelet's command, tty, input, output and error.
func (d *Executor) ExecHandler(w http.ResponseWriter, r *http.Request) {
	if d.Config.InterLink.PluginURL == "" {
		http.Error(w, "kubectl exec is not set up for this site (the executor has no InterLink.PluginURL)", http.StatusNotImplemented)
		return
	}
	q := r.URL.Query()
	uid := q.Get(executorapi.ExecWorkloadUID)
	if uid == "" {
		http.Error(w, executorapi.ExecWorkloadUID+" is required", http.StatusBadRequest)
		return
	}
	req := d.current(r.Context(), uid)
	if req == nil {
		http.Error(w, "no job for workload "+uid, http.StatusNotFound)
		return
	}
	rank, err := strconv.Atoi(q.Get(executorapi.ExecRank))
	members := memberPods(jobRequestOf(req))
	if err != nil || rank < 0 || rank >= len(members) {
		http.Error(w, fmt.Sprintf("rank %q: workload %s has %d member(s)", q.Get(executorapi.ExecRank), uid, len(members)), http.StatusBadRequest)
		return
	}
	member := members[rank]
	container := q.Get(executorapi.ExecContainer)
	if container == "" {
		container = member.Spec.Containers[0].Name
	}
	if !hasContainer(member.Spec.Containers, container) {
		http.Error(w, fmt.Sprintf("rank %d of workload %s has no container %s", rank, uid, container), http.StatusBadRequest)
		return
	}
	st := rankStatus(req, rank)
	if !started(req) || st == nil || st.State != v1alpha1.RankRunning || st.NodeName == "" {
		http.Error(w, fmt.Sprintf("container %s of rank %d is not running", container, rank), http.StatusBadRequest)
		return
	}
	podUID := st.PodUID
	if podUID == "" {
		podUID = string(member.UID)
	}
	out := url.Values{}
	out.Set("namespace", member.Namespace)
	out.Set("pod", podUID)
	out.Set("container", container)
	out.Set("node", st.NodeName)
	for _, k := range execStreamParams {
		for _, v := range q[k] {
			out.Add(k, v)
		}
	}
	target, err := url.Parse(d.Config.InterLink.PluginURL + "/exec")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	target.RawQuery = out.Encode()
	// the protocol upgrade (SPDY or WebSocket) passes through to the plugin
	proxy := &httputil.ReverseProxy{Rewrite: func(pr *httputil.ProxyRequest) {
		pr.Out.URL = target
		pr.Out.Host = target.Host
	}}
	proxy.ServeHTTP(w, r)
}

// rankStatus is rank's entry in q's status, or nil.
func rankStatus(q *v1alpha1.OffloadRequest, rank int) *v1alpha1.RankStatus {
	for i := range q.Status.Ranks {
		if int(q.Status.Ranks[i].Rank) == rank {
			return &q.Status.Ranks[i]
		}
	}
	return nil
}
