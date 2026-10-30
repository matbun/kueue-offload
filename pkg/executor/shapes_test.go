package executor

import (
	"context"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/executorapi"
	"github.com/matbun/kueue-offload/pkg/shadow"
)

// shapeRequest is a workload of PodSets with these names and counts, owned by
// kind; every PodSet runs the same container on the same flavor.
func shapeRequest(kind, owner string, podSets map[string]int32, order ...string) *v1alpha1.OffloadRequest {
	req := testRequest(1)
	req.Spec.Owner = v1alpha1.OwnerReference{APIVersion: "v1", Kind: kind, Name: owner, UID: "owner-uid"}
	tmpl := req.Spec.PodSets[0]
	req.Spec.PodSets = nil
	for _, name := range order {
		ps := *tmpl.DeepCopy()
		ps.Name, ps.Count = kueue.PodSetReference(name), podSets[name]
		req.Spec.PodSets = append(req.Spec.PodSets, ps)
	}
	return req
}

// markIndependent returns req with offload.interlink.eu/independent-pods on the
// owner, which the offload controller forwards in ownerAnnotations.
func markIndependent(req *v1alpha1.OffloadRequest) *v1alpha1.OffloadRequest {
	req = req.DeepCopy()
	req.Spec.OwnerAnnotations = map[string]string{executorapi.IndependentPodsAnnotation: "true"}
	return req
}

// The shapes of backends.md 3.6, each with a plugin that lists pod-group and with
// one that does not: a single pod always runs as one job, a gang runs as one
// allocation or is refused naming the capability, and independent pods run as one
// job each on either plugin.
func TestWorkloadShapes(t *testing.T) {
	shapes := []struct {
		name string
		req  *v1alpha1.OffloadRequest
	}{
		{"single pod", shapeRequest("Pod", "train-x7k2p", map[string]int32{"main": 1}, "main")},
		{"Job with parallelism 1", shapeRequest("Job", "train", map[string]int32{"main": 1}, "main")},
		{"Job with parallelism 3", shapeRequest("Job", "train", map[string]int32{"main": 3}, "main")},
		{"pod group", shapeRequest("Pod", "train-group", map[string]int32{"a1b2c3d4": 2, "e5f6a7b8": 1}, "a1b2c3d4", "e5f6a7b8")},
		{"JobSet", shapeRequest("JobSet", "train", map[string]int32{"leader": 1, "workers": 2}, "leader", "workers")},
		{"TrainJob", shapeRequest("TrainJob", "train", map[string]int32{"launcher": 1, "node": 2}, "launcher", "node")},
	}
	type outcome int
	const (
		oneJob outcome = iota
		oneAllocation
		refused
		jobPerPod
	)
	for _, shape := range shapes {
		for _, independent := range []bool{false, true} {
			for _, podGroupCap := range []bool{true, false} {
				req := shape.req
				name := shape.name
				if independent {
					req = markIndependent(req)
					name += " with independent-pods"
				}
				if !podGroupCap {
					name += ", plugin without pod-group"
				}
				n := workloadMembers(jobRequestOf(req))
				want := oneJob
				switch {
				case n == 1:
				case independent:
					want = jobPerPod
				case podGroupCap:
					want = oneAllocation
				default:
					want = refused
				}
				t.Run(name, func(t *testing.T) {
					e := newExecutorEnv(t)
					if !podGroupCap {
						e.plugin.with(func(p *fakePlugin) { p.capabilities = nil })
					}
					q := e.take(req)
					pods := e.plugin.createdPods()
					if want == refused {
						e.refusedWith(q, `capability "pod-group"`)
						e.refusedWith(q, executorapi.IndependentPodsAnnotation)
						if len(pods) != 0 {
							t.Fatalf("%d /create calls for a refused gang", len(pods))
						}
						return
					}
					if state(q) != "queued" {
						t.Fatalf("submit: %s %+v", state(q), q.Status)
					}
					if len(pods) != n {
						t.Fatalf("%d /create calls, want %d", len(pods), n)
					}
					var wantIDs []string
					for i, pod := range pods {
						group, total := pod.Labels[PodGroupNameLabel], pod.Annotations[PodGroupTotalCountAnnotation]
						if pod.Labels[JobCompletionIndexLabel] != strconv.Itoa(i) || string(pod.UID) != memberUID(executorWorkload, i) {
							t.Errorf("member %d out of order: %s %v", i, pod.UID, pod.Labels)
						}
						if env := envOf(pod.Spec.Containers[0]); env[RankEnv] != strconv.Itoa(i) || env[WorldSizeEnv] != strconv.Itoa(n) {
							t.Errorf("member %d env %v", i, env)
						}
						switch want {
						case oneAllocation:
							if group != executorWorkload || total != strconv.Itoa(n) || pod.Annotations[GangRankAnnotation] != strconv.Itoa(i) {
								t.Errorf("member %d is not in the pod group: %v %v", i, pod.Labels, pod.Annotations)
							}
						default:
							if group != "" || total != "" || pod.Annotations[GangRoleAnnotation] != "" {
								t.Errorf("member %d of a job per pod carries pod group keys: %v %v", i, pod.Labels, pod.Annotations)
							}
						}
						wantIDs = append(wantIDs, strconv.Itoa(4242+i))
					}
					wantID := executorJobID
					if want == jobPerPod {
						wantID = strings.Join(wantIDs, ",")
					}
					if got := q.Status.JobID; got != wantID {
						t.Errorf("jobID = %q, want %q", got, wantID)
					}
				})
			}
		}
	}
}

// Independent pods are one job each: every rank reports its own job, and the
// workload runs while any of them runs and ends once all ended.
func TestIndependentPodsStatus(t *testing.T) {
	e := newExecutorEnv(t)
	e.plugin.with(func(p *fakePlugin) { p.capabilities = nil })
	e.submit(markIndependent(testRequest(3)))
	start := time.Now().Add(-time.Hour)
	setJob := func(i int, containers ...v1.ContainerStatus) {
		e.setStatus(i, "", containers...)
		e.plugin.with(func(p *fakePlugin) {
			st := p.status[memberUID(executorWorkload, i)]
			st.JobID = strconv.Itoa(4242 + i)
			p.status[memberUID(executorWorkload, i)] = st
		})
	}
	setJob(0, waitingContainer(executorCtn, "Priority", ""))
	setJob(1, runningContainer(executorCtn, start))
	setJob(2, terminatedContainer(executorCtn, 0, "Completed", start, time.Now()))
	q := e.poll()
	if state(q) != "running" || q.Status.JobID != "4242,4243,4244" {
		t.Fatalf("one of three running: %s %+v", state(q), q.Status)
	}
	for i, r := range q.Status.Ranks {
		if r.JobID != strconv.Itoa(4242+i) {
			t.Errorf("rank %d reports job %q", i, r.JobID)
		}
	}
	setJob(0, terminatedContainer(executorCtn, 0, "Completed", start, time.Now()))
	setJob(1, terminatedContainer(executorCtn, 2, "Error", start, time.Now()))
	if q := e.poll(); state(q) != "failed" || q.Status.Message != "rank 1 exited with code 2 (Error)" {
		t.Fatalf("one of three failed: %s %+v", state(q), q.Status)
	}
	if err := e.remove(); err != nil || len(e.plugin.deletedUIDs()) != 3 {
		t.Fatalf("delete: %v, %d members deleted", err, len(e.plugin.deletedUIDs()))
	}
}

// Independent pods are each the Slurm plugin's single-pod job, named after its
// pod: the srun carrier finds the head's by its UID. A site launcher, which starts
// a gang's ranks, does not stop their tunnel.
func TestReverseIndependentPods(t *testing.T) {
	e := reverseEnv(t, shadow.ReverseConfig{Enabled: true})
	req := markIndependent(reverseRequest(2, "8443:"+reverseTarget+":443"))
	req.Spec.PodSets[0].Template.Annotations[LauncherAnnotation] = "torchrun"
	rt, aerr := e.d.reverseTunnel(jobRequestOf(req))
	if aerr != nil || rt.JobName != memberUID(executorWorkload, 0) {
		t.Fatalf("tunnel %+v, %v", rt, aerr)
	}
}

func TestJobIDList(t *testing.T) {
	var ids []string
	for i := range 10 {
		ids = append(ids, strconv.Itoa(100+i))
	}
	if got := jobIDList(ids[:3]); got != "100,101,102" {
		t.Errorf("three jobs: %q", got)
	}
	if got := jobIDList(ids); got != "100,...,109 (10 jobs)" {
		t.Errorf("ten jobs: %q", got)
	}
}

// The executor reads the plugin's capabilities from the ping answer at start and
// every capabilitiesInterval. An answer without the field, or not in JSON, lists
// none; a failed read keeps the last answer.
func TestCapabilities(t *testing.T) {
	oldInterval := capabilitiesInterval
	capabilitiesInterval = 20 * time.Millisecond
	t.Cleanup(func() { capabilitiesInterval = oldInterval })

	e := newExecutorEnv(t)
	e.plugin.with(func(p *fakePlugin) { p.capabilities = nil })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		e.d.WatchCapabilities(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	capabilities := func() ([]string, bool) {
		e.d.healthMu.Lock()
		defer e.d.healthMu.Unlock()
		return e.d.caps, e.d.capsKnown
	}
	waitFor := func(what string, ok func(caps []string, known bool) bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if caps, known := capabilities(); ok(caps, known) {
				return
			}
			if time.Now().After(deadline) {
				caps, known := capabilities()
				t.Fatalf("%s: capabilities %v, known %v", what, caps, known)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor("at start", func(caps []string, known bool) bool { return known && len(caps) == 0 })
	e.refusedWith(e.take(testRequest(2)), `capability "pod-group"`)

	e.plugin.with(func(p *fakePlugin) { p.capabilities = []string{"other", CapabilityPodGroup} })
	waitFor("after the plugin gained pod-group", func(caps []string, _ bool) bool { return slices.Contains(caps, CapabilityPodGroup) })
	if q := e.submit(attempt(testRequest(2), 2)); q.Status.JobID != executorJobID {
		t.Fatalf("a gang once the plugin lists pod-group: %+v", q.Status)
	}

	e.plugin.with(func(p *fakePlugin) { p.pingBody = "ok" })
	waitFor("after a plain text answer", func(caps []string, _ bool) bool { return len(caps) == 0 })
	e.plugin.with(func(p *fakePlugin) { p.pingBody = `{"status": "ok"}` })
	e.plugin.with(func(p *fakePlugin) { p.capabilities = []string{CapabilityPodGroup} })
	time.Sleep(5 * capabilitiesInterval)
	if caps, _ := capabilities(); len(caps) != 0 {
		t.Fatalf("an answer without the field lists %v", caps)
	}

	e.plugin.with(func(p *fakePlugin) { p.pingBody = "" })
	waitFor("after pod-group came back", func(caps []string, _ bool) bool { return slices.Contains(caps, CapabilityPodGroup) })
	e.plugin.srv.Close()
	time.Sleep(5 * capabilitiesInterval)
	if caps, _ := capabilities(); !slices.Contains(caps, CapabilityPodGroup) {
		t.Fatalf("a failed read dropped the capabilities: %v", caps)
	}
}

// A submit before the first ping answer asks for it; a plugin that does not answer
// is not a refusal of the gang.
func TestCapabilitiesAskedAtSubmit(t *testing.T) {
	e := newExecutorEnv(t)
	first := testRequest(2)
	e.submit(first)
	var pings int
	e.plugin.with(func(p *fakePlugin) { pings = p.pings })
	if pings != 1 {
		t.Fatalf("%d pings before the gang, want 1", pings)
	}
	if err := e.remove(); err != nil {
		t.Fatal(err)
	}
	e.submit(attempt(first, 2))
	e.plugin.with(func(p *fakePlugin) { pings = p.pings })
	if pings != 1 {
		t.Fatalf("%d pings, want the first answer reused", pings)
	}

	down := newExecutorEnv(t)
	down.plugin.srv.Close()
	waitingWith(t, down.take(testRequest(2)), "asking the plugin for its capabilities", false)
}

// A site whose Shadow block names no login node, such as a backend that reports no
// node: the shadow only streams logs and starts as soon as the job runs, and
// exposed ports and reverse forwards are refused at submit.
func TestShadowWithoutLoginHost(t *testing.T) {
	e := newExecutorEnv(t)
	e.d.Config.Shadow = shadowBlock()
	e.d.Config.Shadow.LoginHost, e.d.Config.Shadow.User, e.d.Config.Shadow.KeySecret = "", "", ""
	m, err := NewShadowManager(e.kube, e.d.Config)
	if err != nil {
		t.Fatal(err)
	}
	e.d.Shadow = m

	port := testRequest(1)
	port.Spec.PodSets[0].Template.Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 8888}}
	extraPort := attempt(testRequest(1), 2)
	extraPort.Spec.PodSets[0].Template.Annotations = map[string]string{"interlink.eu/wstunnel-extra-ports": "6006"}
	for _, req := range []*v1alpha1.OffloadRequest{port, extraPort, attempt(reverseRequest(1, reverseTarget+":5000"), 3)} {
		e.refusedWith(e.take(req), "no login node (Shadow.LoginHost)")
	}
	if n := len(e.plugin.createdPods()); n != 0 {
		t.Fatalf("%d /create calls for refused tunnels", n)
	}

	e.submit(attempt(testRequest(1), 4))
	e.setStatus(0, "", waitingContainer(executorCtn, "Idle", ""))
	if q := e.poll(); q.Status.Shadow != nil {
		t.Fatalf("queued job has a shadow: %+v", q.Status.Shadow)
	}
	e.setStatus(0, "", runningContainer(executorCtn, time.Now()))
	q := e.poll()
	st := q.Status
	if state(q) != "running" || st.NodeName != "" || st.Shadow == nil || st.Shadow.Service != "" {
		t.Fatalf("running without a node: state %s, node %q, shadow %+v", state(q), st.NodeName, st.Shadow)
	}
	dep, err := e.kube.AppsV1().Deployments(executorNS).Get(context.Background(), st.Shadow.Deployment, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if names := containerNames(dep); names != "job-logs" || len(dep.Spec.Template.Spec.InitContainers) != 0 {
		t.Fatalf("shadow without a login node runs %s, init %d", names, len(dep.Spec.Template.Spec.InitContainers))
	}
}

// With a login node, a forward waits for the node, as before.
func TestForwardWaitsForTheNode(t *testing.T) {
	e := newExecutorEnv(t)
	req := testRequest(1)
	req.Spec.PodSets[0].Template.Spec.Containers[0].Ports = []v1.ContainerPort{{ContainerPort: 8888}}
	e.submit(req)
	e.setStatus(0, "", runningContainer(executorCtn, time.Now()))
	if q := e.poll(); state(q) != "running" || len(e.shadow.ensured) != 0 {
		t.Fatalf("a forward without its node: state %s, %d shadows", state(q), len(e.shadow.ensured))
	}
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	e.poll()
	if len(e.shadow.ensured) != 1 || e.shadow.ensured[0].NodeName != "node07" {
		t.Fatalf("shadows %+v", e.shadow.ensured)
	}
}

// A per-user job limit that refuses the second of two independent members for now
// keeps the first: the second waits for the site, is created by a later poll, also
// of a restarted executor, and the workload ends only once both ran. A pod group
// would be deleted whole.
func TestIndependentPodsWaitForTheSite(t *testing.T) {
	e := newExecutorEnv(t)
	e.plugin.with(func(p *fakePlugin) {
		p.capabilities = nil
		p.createCodes = []int{http.StatusOK, http.StatusServiceUnavailable, http.StatusServiceUnavailable}
		p.createText = "QOSMaxSubmitJobPerUserLimit"
	})
	q := e.submit(markIndependent(testRequest(2)))
	if q.Status.JobID != "4242" || q.Status.Ranks[1].PodUID != "" || !strings.Contains(q.Status.Message, "rank 1 waits for the site") {
		t.Fatalf("submit: job %q, %+v", q.Status.JobID, q.Status)
	}
	start := time.Now().Add(-time.Minute)
	e.setStatus(0, "node07", terminatedContainer(executorCtn, 0, "Completed", start, time.Now()))
	q = e.poll()
	if state(q) != "queued" || q.Status.Reason != reasonWaitingForSite || !strings.Contains(q.Status.Message, "rank 1 waits for the site: the site takes no job now: QOSMaxSubmitJobPerUserLimit") ||
		len(e.plugin.deletedUIDs()) != 0 {
		t.Fatalf("rank 1 refused again: %s %+v, %d deleted", state(q), q.Status, len(e.plugin.deletedUIDs()))
	}
	e.restart()
	if q := e.poll(); q.Status.JobID != "4242,4243" || q.Status.Ranks[1].PodUID != memberUID(executorWorkload, 1) || jobEnded(q) {
		t.Fatalf("rank 1 taken: %s %+v", state(q), q.Status)
	}
	e.setStatus(1, "node08", terminatedContainer(executorCtn, 0, "Completed", start, time.Now()))
	if q := e.poll(); state(q) != "succeeded" || q.Status.JobID != "4242,4243" {
		t.Fatalf("both ran: %s %+v", state(q), q.Status)
	}
	if n := len(e.plugin.createdPods()); n != 4 {
		t.Fatalf("%d /create calls, want rank 0 once and rank 1 until the site took it", n)
	}
	if err := e.remove(); err != nil || len(e.plugin.deletedUIDs()) != 2 {
		t.Fatalf("delete: %v, %d members deleted", err, len(e.plugin.deletedUIDs()))
	}
}

// A pending rank refused for good fails the workload.
func TestIndependentPendingRankRefused(t *testing.T) {
	e := newExecutorEnv(t)
	e.plugin.with(func(p *fakePlugin) {
		p.capabilities = nil
		p.createCodes = []int{http.StatusOK, http.StatusServiceUnavailable, http.StatusBadRequest}
		p.createText = "Invalid qos"
	})
	e.submit(markIndependent(testRequest(2)))
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	if q := e.poll(); state(q) != "failed" || q.Status.Reason != "Rejected" || q.Status.Message != "the site refused rank 1: Invalid qos" {
		t.Fatalf("refused rank: %s %+v", state(q), q.Status)
	}
}

// A pending rank is not created when the workload is deleted first.
func TestIndependentPendingRankDeleted(t *testing.T) {
	e := newExecutorEnv(t)
	e.plugin.with(func(p *fakePlugin) {
		p.capabilities = nil
		p.createCodes = []int{http.StatusOK, http.StatusServiceUnavailable}
	})
	e.submit(markIndependent(testRequest(2)))
	if err := e.remove(); err != nil || len(e.plugin.deletedUIDs()) != 1 {
		t.Fatalf("delete: %v, %d members deleted, want only the created one", err, len(e.plugin.deletedUIDs()))
	}
}
