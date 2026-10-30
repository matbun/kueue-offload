package executor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
	v1 "k8s.io/api/core/v1"
)

// fakePlugin is an interLink plugin behind the API server: /create, /status (with
// an empty list, the ping), /getLogs, /delete and /createPodSets. Every answer is
// scripted by the test. With capability pod-group, which it lists by default,
// members of a pod group are buffered until the last one, as the Slurm plugin's
// gang path does; without it every pod is its own job, numbered from fakeJobID by
// completion index. /createPodSets gives every pod of the group fakeJobID, as the
// Slurm plugin does; the executor calls it only when a test lists podset-create.
type fakePlugin struct {
	t   *testing.T
	srv *httptest.Server

	mu sync.Mutex
	// created holds every /create body, in arrival order.
	created []commonIL.RetrievedPodData
	// createCodes answer the next /create calls, one each; 200 once used up.
	createCodes []int
	createText  string
	// podSets holds every /createPodSets body, in arrival order. podSetsCodes and
	// podSetsText answer the next calls as createCodes and createText do;
	// podSetsAnswer, when set, rewrites an answer of 200.
	podSets       []fakePodSets
	podSetsCodes  []int
	podSetsText   string
	podSetsAnswer func([]commonIL.CreateStruct) []commonIL.CreateStruct
	// status is what /status reports per pod UID; a pod not in it is left out.
	status map[string]commonIL.PodStatus
	// phases holds the phases of the last /status request, by pod UID.
	phases   map[string]v1.PodPhase
	statuses int
	// logs is each container's log, by "<pod UID>/<container>". A followed log
	// ends once its pod is in ended.
	logs    map[string]string
	ended   map[string]bool
	deleted []string
	pings   int
	// groups holds the buffered members of each pod group.
	groups map[string][]string
	// holdGroups keeps every pod group buffered: no member gets a job ID.
	holdGroups bool
	// capabilities are listed in the ping answer; pingBody, when set, replaces
	// that answer.
	capabilities []string
	pingBody     string
}

const fakeJobID = executorJobID

// fakePodSets is a /createPodSets body as the fake reads it: only the pods, which
// the API server's body and the plugin's both carry under "pod".
type fakePodSets struct {
	GroupUID string `json:"groupUID"`
	PodSets  []struct {
		Name string `json:"name"`
		Pods []struct {
			Pod v1.Pod `json:"pod"`
		} `json:"pods"`
	} `json:"podSets"`
}

func newFakePlugin(t *testing.T) *fakePlugin {
	p := &fakePlugin{t: t, status: map[string]commonIL.PodStatus{}, logs: map[string]string{}, ended: map[string]bool{}, groups: map[string][]string{},
		capabilities: []string{CapabilityPodGroup}}
	mux := http.NewServeMux()
	mux.HandleFunc("/create", p.create)
	mux.HandleFunc("/status", p.statusHandler)
	mux.HandleFunc("/getLogs", p.getLogs)
	mux.HandleFunc("/delete", p.delete)
	mux.HandleFunc("/createPodSets", p.createPodSets)
	p.srv = httptest.NewServer(mux)
	t.Cleanup(p.srv.Close)
	return p
}

func (p *fakePlugin) with(f func(p *fakePlugin)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f(p)
}

func (p *fakePlugin) create(w http.ResponseWriter, r *http.Request) {
	var data commonIL.RetrievedPodData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		p.t.Errorf("fake plugin: decoding /create: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.created = append(p.created, data)
	if len(p.createCodes) > 0 {
		code := p.createCodes[0]
		p.createCodes = p.createCodes[1:]
		if code != http.StatusOK {
			http.Error(w, p.createText, code)
			return
		}
	}
	pod := data.Pod
	jid := fakeJobID
	if group := pod.Labels[PodGroupNameLabel]; group == "" || !slices.Contains(p.capabilities, CapabilityPodGroup) {
		base, _ := strconv.Atoi(fakeJobID)
		index, _ := strconv.Atoi(pod.Labels[JobCompletionIndexLabel])
		jid = strconv.Itoa(base + index)
	} else {
		n, _ := strconv.Atoi(pod.Annotations[PodGroupTotalCountAnnotation])
		p.groups[group] = append(p.groups[group], string(pod.UID))
		if len(p.groups[group]) < n || p.holdGroups {
			jid = ""
		} else {
			delete(p.groups, group)
		}
	}
	writeTestJSON(w, commonIL.CreateStruct{PodUID: string(pod.UID), PodJID: jid})
}

func (p *fakePlugin) createPodSets(w http.ResponseWriter, r *http.Request) {
	var body fakePodSets
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		p.t.Errorf("fake plugin: decoding /createPodSets: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.podSets = append(p.podSets, body)
	if len(p.podSetsCodes) > 0 {
		code := p.podSetsCodes[0]
		p.podSetsCodes = p.podSetsCodes[1:]
		if code != http.StatusOK {
			http.Error(w, p.podSetsText, code)
			return
		}
	}
	out := []commonIL.CreateStruct{}
	for _, ps := range body.PodSets {
		for _, data := range ps.Pods {
			out = append(out, commonIL.CreateStruct{PodUID: string(data.Pod.UID), PodJID: fakeJobID})
		}
	}
	if p.podSetsAnswer != nil {
		out = p.podSetsAnswer(out)
	}
	writeTestJSON(w, out)
}

func (p *fakePlugin) statusHandler(w http.ResponseWriter, r *http.Request) {
	var pods []*v1.Pod
	if err := json.NewDecoder(r.Body).Decode(&pods); err != nil {
		p.t.Errorf("fake plugin: decoding /status: %v", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(pods) == 0 {
		p.pings++
		if p.pingBody != "" {
			_, _ = w.Write([]byte(p.pingBody))
			return
		}
		writeTestJSON(w, pingAnswer{PingResponse: commonIL.PingResponse{Status: "ok"}, Capabilities: p.capabilities})
		return
	}
	p.statuses++
	p.phases = map[string]v1.PodPhase{}
	out := []commonIL.PodStatus{}
	for _, pod := range pods {
		p.phases[string(pod.UID)] = pod.Status.Phase
		if st, ok := p.status[string(pod.UID)]; ok {
			st.PodUID, st.PodName, st.PodNamespace = string(pod.UID), pod.Name, pod.Namespace
			out = append(out, st)
		}
	}
	writeTestJSON(w, out)
}

func (p *fakePlugin) getLogs(w http.ResponseWriter, r *http.Request) {
	var req commonIL.LogStruct
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		p.t.Errorf("fake plugin: decoding /getLogs: %v", err)
	}
	key := req.PodUID + "/" + req.ContainerName
	sent := 0
	next := func() (string, bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		text := p.logs[key][sent:]
		sent += len(text)
		return text, p.ended[req.PodUID]
	}
	text, _ := next()
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(text))
	w.(http.Flusher).Flush()
	if !req.Opts.Follow {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(5 * time.Millisecond):
		}
		text, ended := next()
		if text != "" {
			_, _ = w.Write([]byte(text))
			w.(http.Flusher).Flush()
		}
		if ended {
			return
		}
	}
}

func (p *fakePlugin) delete(w http.ResponseWriter, r *http.Request) {
	var pod v1.Pod
	if err := json.NewDecoder(r.Body).Decode(&pod); err != nil {
		p.t.Errorf("fake plugin: decoding /delete: %v", err)
	}
	p.mu.Lock()
	p.deleted = append(p.deleted, string(pod.UID))
	for group, uids := range p.groups {
		p.groups[group] = slices.DeleteFunc(uids, func(u string) bool { return u == string(pod.UID) })
	}
	p.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func writeTestJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

// createdPods returns the pods /create received, in arrival order.
func (p *fakePlugin) createdPods() []v1.Pod {
	p.mu.Lock()
	defer p.mu.Unlock()
	pods := make([]v1.Pod, 0, len(p.created))
	for _, d := range p.created {
		pods = append(pods, d.Pod)
	}
	return pods
}

// podSetCalls returns the /createPodSets bodies, in arrival order.
func (p *fakePlugin) podSetCalls() []fakePodSets {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]fakePodSets(nil), p.podSets...)
}

// container returns the data /create received for a container of member i, the
// last time that member was created.
func (p *fakePlugin) container(t *testing.T, i int, name string) commonIL.RetrievedContainer {
	t.Helper()
	uid := memberUID(executorWorkload, i)
	p.mu.Lock()
	defer p.mu.Unlock()
	for j := len(p.created) - 1; j >= 0; j-- {
		if string(p.created[j].Pod.UID) != uid {
			continue
		}
		for _, c := range p.created[j].Containers {
			if c.Name == name {
				return c
			}
		}
	}
	t.Fatalf("no container %s of member %d reached the plugin", name, i)
	return commonIL.RetrievedContainer{}
}

// phase is the phase of member i in the last /status request.
func (p *fakePlugin) phase(i int) v1.PodPhase {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.phases[memberUID(executorWorkload, i)]
}

func (p *fakePlugin) deletedUIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.deleted...)
}

// Container states the tests script.

func waitingContainer(name, reason, message string) v1.ContainerStatus {
	return v1.ContainerStatus{Name: name, State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: reason, Message: message}}}
}

func runningContainer(name string, since time.Time) v1.ContainerStatus {
	return v1.ContainerStatus{Name: name, State: v1.ContainerState{Running: &v1.ContainerStateRunning{StartedAt: metaTime(since)}}}
}

func terminatedContainer(name string, code int32, reason string, start, end time.Time) v1.ContainerStatus {
	return v1.ContainerStatus{Name: name, State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
		ExitCode: code, Reason: reason, StartedAt: metaTime(start), FinishedAt: metaTime(end),
	}}}
}
