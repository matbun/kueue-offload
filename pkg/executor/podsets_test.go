package executor

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

const workerCtn = "worker"

// twoPodSetRequest is a workload of a PodSet of drivers and one of workers, each
// with its own container.
func twoPodSetRequest(drivers, workers int32) *v1alpha1.OffloadRequest {
	req := testRequest(drivers)
	req.Spec.PodSets[0].Name = "driver"
	req.Spec.PodSets[0].Template.Spec.Containers[0].Command = []string{"sh", "-c", "echo driver"}
	req.Spec.PodSets = append(req.Spec.PodSets, v1alpha1.PodSet{
		Name:        "worker",
		Flavors:     testFlavors(),
		Count:       workers,
		Annotations: map[string]string{flagsKey: flavorFlags},
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Name:    workerCtn,
			Image:   "docker://alpine",
			Command: []string{"sh", "-c", "echo worker"},
		}}}},
	})
	return req
}

func TestSeveralPodSetsAreOnePodGroup(t *testing.T) {
	e := newExecutorEnv(t)
	req := twoPodSetRequest(2, 1)
	if q := e.submit(req); q.Status.JobID != executorJobID {
		t.Fatalf("jobID = %q", q.Status.JobID)
	}

	pods := e.plugin.createdPods()
	if len(pods) != 3 {
		t.Fatalf("%d /create calls, want 3", len(pods))
	}
	if n := len(e.plugin.podGroupCalls()); n != 0 {
		t.Fatalf("%d /createPodGroup calls to a plugin without %s", n, CapabilityPodGroupCreate)
	}
	for i, want := range []struct{ podSet, role, command string }{
		{"driver", GangRoleHead, "echo driver"}, {"driver", GangRoleWorker, "echo driver"}, {"worker", GangRoleWorker, "echo worker"},
	} {
		m := pods[i]
		if m.Labels[JobCompletionIndexLabel] != string(rune('0'+i)) || m.Annotations[PodSetAnnotation] != want.podSet ||
			m.Labels[PodGroupNameLabel] != executorWorkload || m.Annotations[PodGroupTotalCountAnnotation] != "3" ||
			m.Annotations[GangRoleAnnotation] != want.role || string(m.UID) != memberUID(executorWorkload, i) {
			t.Errorf("member %d: labels %v annotations %v", i, m.Labels, m.Annotations)
		}
		if got := strings.Join(m.Spec.Containers[0].Command, " "); !strings.Contains(got, want.command) {
			t.Errorf("member %d runs %q, want %q", i, got, want.command)
		}
	}
	if env := envOf(pods[2].Spec.Containers[0]); env[RankEnv] != "2" || env[WorldSizeEnv] != "3" {
		t.Errorf("member 2 env %v", env)
	}

	if q := e.poll(); len(q.Status.Ranks) != 3 || q.Status.Ranks[2].PodUID != memberUID(executorWorkload, 2) {
		t.Fatalf("ranks: %+v", q.Status.Ranks)
	}

	// Each member's log is its own container's.
	e.setLog(0, executorCtn, "zero\n")
	e.setLog(1, executorCtn, "one\n")
	e.setLog(2, workerCtn, "two\n")
	rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload)
	if want := "[rank 0] zero\n[rank 1] one\n[rank 2] two\n"; rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Fatalf("logs = %d %q, want %q", rec.Code, rec.Body.String(), want)
	}
	rec = e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload+"&container="+workerCtn)
	if want := "[rank 2] two\n"; rec.Body.String() != want {
		t.Fatalf("worker logs = %q, want %q", rec.Body.String(), want)
	}

	if err := e.remove(); err != nil {
		t.Fatal(err)
	}
	if n := len(e.plugin.deletedUIDs()); n != 3 {
		t.Errorf("%d members deleted, want 3", n)
	}
}

// Every member keeps its own PodSet's resources; sizing the one allocation is the
// plugin's.
func TestPodSetsKeepTheirResources(t *testing.T) {
	e := newExecutorEnv(t)
	req := twoPodSetRequest(1, 1)
	req.Spec.PodSets[0].Template.Spec.Containers[0].Resources.Limits = v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("2"), v1.ResourceMemory: resource.MustParse("8Gi"),
	}
	req.Spec.PodSets[1].Template.Spec.Containers[0].Resources.Limits = v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("16"), v1.ResourceMemory: resource.MustParse("4Gi"), "nvidia.com/gpu": resource.MustParse("2"),
	}
	e.submit(req)
	pods := e.plugin.createdPods()
	if got := pods[0].Spec.Containers[0].Resources.Limits.Cpu().String(); got != "2" {
		t.Errorf("driver CPUs %s", got)
	}
	if got := pods[1].Spec.Containers[0].Resources.Limits.Name("nvidia.com/gpu", resource.DecimalSI).String(); got != "2" {
		t.Errorf("worker GPUs %s", got)
	}
}

func TestPodSetsRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1alpha1.OffloadRequest)
		want   string
	}{
		{"different queues", func(r *v1alpha1.OffloadRequest) {
			r.Spec.PodSets[1].Annotations = map[string]string{flagsKey: "--account=proj42 --partition=gpu --qos=prod"}
		}, "annotations differ from PodSet driver's"},
		{"launcher with different images", func(r *v1alpha1.OffloadRequest) {
			r.Spec.PodSets[0].Template.Spec.Containers[0].Command = r.Spec.PodSets[1].Template.Spec.Containers[0].Command
			r.Spec.PodSets[1].Template.Annotations = map[string]string{LauncherAnnotation: "mpi"}
		}, "same image and command"},
		{"launcher with different commands", func(r *v1alpha1.OffloadRequest) {
			r.Spec.PodSets[1].Template.Spec.Containers[0].Image = r.Spec.PodSets[0].Template.Spec.Containers[0].Image
			r.Spec.PodSets[0].Template.Annotations = map[string]string{LauncherAnnotation: "mpi"}
		}, "same image and command"},
		{"too many members", func(r *v1alpha1.OffloadRequest) {
			r.Spec.PodSets[0].Count, r.Spec.PodSets[1].Count = maxWorkloadMembers, 1
		}, "more than"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			req := twoPodSetRequest(1, 1)
			tt.mutate(req)
			e.refusedWith(e.take(req), tt.want)
			if n := len(e.plugin.createdPods()); n != 0 {
				t.Fatalf("the plugin got %d pods", n)
			}
		})
	}

	// The same container in every PodSet runs under a launcher.
	req := twoPodSetRequest(1, 1)
	req.Spec.PodSets[1].Template.Spec.Containers[0] = *req.Spec.PodSets[0].Template.Spec.Containers[0].DeepCopy()
	req.Spec.PodSets[1].Template.ObjectMeta = metav1.ObjectMeta{Annotations: map[string]string{LauncherAnnotation: "mpi"}}
	if aerr := validatePodSetsShareAllocation(req.Spec.PodSets); aerr != nil {
		t.Fatalf("same container refused: %s", aerr.msg)
	}
}

// Each PodSet's references are read and resolved for its own members.
func TestPodSetsResolveTheirOwnReferences(t *testing.T) {
	e := newExecutorEnv(t, &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "hf-token", Namespace: executorNS}, Data: map[string][]byte{"token": []byte("s3cr3t")}})
	req := twoPodSetRequest(1, 1)
	ps := &req.Spec.PodSets[1]
	ps.SecretRefs = []string{"hf-token"}
	ps.Template.Spec.Containers[0].Env = []v1.EnvVar{{Name: "HF_TOKEN", ValueFrom: &v1.EnvVarSource{SecretKeyRef: &v1.SecretKeySelector{
		LocalObjectReference: v1.LocalObjectReference{Name: "hf-token"}, Key: "token",
	}}}}
	e.submit(req)
	pods := e.plugin.createdPods()
	if env := envOf(pods[1].Spec.Containers[0]); env["HF_TOKEN"] != "s3cr3t" {
		t.Fatalf("worker env %v", env)
	}
	if _, ok := envOf(pods[0].Spec.Containers[0])["HF_TOKEN"]; ok {
		t.Fatalf("the driver got the worker's Secret")
	}
}

// listPodGroupCreate makes e's plugin list pod-group-create next to pod-group.
func (e *executorEnv) listPodGroupCreate() {
	e.plugin.with(func(p *fakePlugin) { p.capabilities = []string{CapabilityPodGroup, CapabilityPodGroupCreate} })
}

// memberUIDs are the UIDs of the workload's first n members, in rank order.
func memberUIDs(n int) []string {
	uids := make([]string, n)
	for i := range uids {
		uids[i] = memberUID(executorWorkload, i)
	}
	return uids
}

// A plugin that lists pod-group-create gets a pod group in one /createPodGroup call,
// its members in rank order, and no /create. Status and delete stay per
// member.
func TestPodGroupInOneCreatePodGroup(t *testing.T) {
	e := newExecutorEnv(t)
	e.listPodGroupCreate()
	q := e.submit(twoPodSetRequest(1, 2))
	if q.Status.JobID != executorJobID || len(q.Status.Ranks) != 3 {
		t.Fatalf("jobID %q, ranks %+v", q.Status.JobID, q.Status.Ranks)
	}
	for i, r := range q.Status.Ranks {
		if r.Rank != int32(i) || r.PodUID != memberUID(executorWorkload, i) || r.JobID != "" || r.State != v1alpha1.RankQueued {
			t.Errorf("rank %d: %+v", i, r)
		}
	}
	if n := len(e.plugin.createdPods()); n != 0 {
		t.Fatalf("%d /create calls besides /createPodGroup", n)
	}
	calls := e.plugin.podGroupCalls()
	if len(calls) != 1 || calls[0].GroupUID != executorWorkload {
		t.Fatalf("%d /createPodGroup calls, want one for group %s", len(calls), executorWorkload)
	}
	if len(calls[0].Pods) != 3 {
		t.Fatalf("%d pods in the group, want 3", len(calls[0].Pods))
	}
	for rank, data := range calls[0].Pods {
		pod, role, podSet := data.Pod, GangRoleWorker, "worker"
		if rank == 0 {
			role, podSet = GangRoleHead, "driver"
		}
		if string(pod.UID) != memberUID(executorWorkload, rank) || pod.Labels[JobCompletionIndexLabel] != strconv.Itoa(rank) ||
			pod.Labels[PodGroupNameLabel] != executorWorkload || pod.Annotations[PodGroupTotalCountAnnotation] != "3" ||
			pod.Annotations[PodSetAnnotation] != podSet || pod.Annotations[GangRoleAnnotation] != role {
			t.Errorf("rank %d out of order or mislabelled: %s %v %v", rank, pod.UID, pod.Labels, pod.Annotations)
		}
		if env := envOf(pod.Spec.Containers[0]); env[RankEnv] != strconv.Itoa(rank) || env[WorldSizeEnv] != "3" {
			t.Errorf("rank %d env %v", rank, env)
		}
	}

	since := time.Now().Add(-time.Minute)
	for i, ctn := range []string{executorCtn, workerCtn, workerCtn} {
		e.setStatus(i, "node0"+strconv.Itoa(7+i), runningContainer(ctn, since))
	}
	if q := e.poll(); state(q) != "running" || q.Status.NodeName != "node07" || q.Status.Ranks[2].State != v1alpha1.RankRunning {
		t.Fatalf("running: %s %+v", state(q), q.Status)
	}
	if err := e.remove(); err != nil {
		t.Fatal(err)
	}
	if got := e.plugin.deletedUIDs(); !slices.Equal(got, memberUIDs(3)) {
		t.Fatalf("deleted %v, want every member", got)
	}
}

// The plugin's ping answer reaches the executor through any API server, so a
// plugin that lists pod-group-create can sit behind one without /createPodGroup.
// That one answers 404 and submits nothing: the members go to /create one by one,
// as for a plugin without the capability.
func TestPodGroupWithoutCreatePodGroupRoute(t *testing.T) {
	e := newExecutorEnv(t)
	if hasCreatePodGroup(t, e.d.InterLink.URL) {
		t.Skip("the interLink API server has /createPodGroup")
	}
	e.d.InterLink.HTTP = &http.Client{}
	e.listPodGroupCreate()
	q := e.submit(twoPodSetRequest(1, 2))
	if q.Status.JobID != executorJobID || len(q.Status.Ranks) != 3 {
		t.Fatalf("jobID %q, ranks %+v", q.Status.JobID, q.Status.Ranks)
	}
	for i, r := range q.Status.Ranks {
		if r.PodUID != memberUID(executorWorkload, i) || r.JobID != "" || r.State != v1alpha1.RankQueued {
			t.Errorf("rank %d: %+v", i, r)
		}
	}
	if n := len(e.plugin.podGroupCalls()); n != 0 {
		t.Fatalf("%d /createPodGroup calls reached the plugin", n)
	}
	var uids []string
	for _, pod := range e.plugin.createdPods() {
		uids = append(uids, string(pod.UID))
	}
	if !slices.Equal(uids, memberUIDs(3)) {
		t.Fatalf("/create for %v, want every member in rank order", uids)
	}
	if got := e.plugin.deletedUIDs(); len(got) != 0 {
		t.Fatalf("deleted %v after the 404", got)
	}
}

// A refused /createPodGroup deletes every member, as a refused /create does: a 503
// leaves the request waiting and submits it again at the next poll, a 400 refuses
// it.
func TestCreatePodGroupRefusals(t *testing.T) {
	tests := []struct {
		name      string
		code      int
		text      string
		permanent bool
		want      string
	}{
		{"submit limit", http.StatusServiceUnavailable, "QOSMaxSubmitJobPerUserLimit", false, "the site takes no job now: QOSMaxSubmitJobPerUserLimit"},
		{"refused", http.StatusBadRequest, "Invalid account", true, "Invalid account"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			e.listPodGroupCreate()
			e.plugin.with(func(p *fakePlugin) { p.groupCodes, p.groupText = []int{tt.code}, tt.text })
			q := e.take(twoPodSetRequest(1, 2))
			if tt.permanent {
				e.refusedWith(q, tt.want)
			} else {
				waitingWith(t, q, tt.want, true)
			}
			if got := e.plugin.deletedUIDs(); !slices.Equal(got, memberUIDs(3)) {
				t.Fatalf("deleted %v, want every member", got)
			}
			if n := len(e.plugin.createdPods()); n != 0 {
				t.Fatalf("%d /create calls besides /createPodGroup", n)
			}
			if tt.permanent {
				return
			}
			if q := e.poll(); q.Status.JobID != executorJobID || q.Status.Message != "" || len(e.plugin.podGroupCalls()) != 2 {
				t.Fatalf("submit after the refusal: %s %+v, %d calls", state(q), q.Status, len(e.plugin.podGroupCalls()))
			}
		})
	}
}

// An answer to /createPodGroup that does not list every member once, in rank
// order, with one job ID deletes the members and leaves the request waiting.
func TestCreatePodGroupMalformedAnswer(t *testing.T) {
	tests := []struct {
		name   string
		answer func([]commonIL.CreateStruct) []commonIL.CreateStruct
		want   string
	}{
		{"a member missing", func(r []commonIL.CreateStruct) []commonIL.CreateStruct { return r[:2] }, "lists 2 pods, not the 3 members"},
		{"members out of order", func(r []commonIL.CreateStruct) []commonIL.CreateStruct {
			r[1], r[2] = r[2], r[1]
			return r
		}, `lists pod "` + memberUID(executorWorkload, 2) + `" at rank 1`},
		{"two jobs", func(r []commonIL.CreateStruct) []commonIL.CreateStruct {
			r[2].PodJID = "4243"
			return r
		}, "gives rank 2 job 4243 and rank 0 job 4242"},
		{"no job ID", func(r []commonIL.CreateStruct) []commonIL.CreateStruct {
			for i := range r {
				r[i].PodJID = ""
			}
			return r
		}, "gives rank 0 no job ID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			e.listPodGroupCreate()
			e.plugin.with(func(p *fakePlugin) { p.groupAnswer = tt.answer })
			waitingWith(t, e.take(twoPodSetRequest(1, 2)), "the answer to /createPodGroup "+tt.want, true)
			if got := e.plugin.deletedUIDs(); !slices.Equal(got, memberUIDs(3)) {
				t.Fatalf("deleted %v, want every member", got)
			}
		})
	}
}

// A single pod and independent pods go member by member to /create, also on a
// plugin that lists pod-group-create.
func TestCreatePodGroupOnlyForPodGroups(t *testing.T) {
	tests := []struct {
		name  string
		req   *v1alpha1.OffloadRequest
		jobID string
	}{
		{"single pod", testRequest(1), "4242"},
		{"independent pods", markIndependent(twoPodSetRequest(1, 2)), "4242,4243,4244"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			e.listPodGroupCreate()
			q := e.submit(tt.req)
			if q.Status.JobID != tt.jobID || len(e.plugin.createdPods()) != len(q.Status.Ranks) || len(e.plugin.podGroupCalls()) != 0 {
				t.Fatalf("jobID %q, %d /create and %d /createPodGroup calls for %d ranks",
					q.Status.JobID, len(e.plugin.createdPods()), len(e.plugin.podGroupCalls()), len(q.Status.Ranks))
			}
		})
	}
}
