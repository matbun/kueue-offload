package node

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

const (
	testNS     = "alice"
	testWL     = "job-train-5d565"
	testNode   = "kueue-offload-hpc-a"
	testHostIP = "10.100.1.7"
)

var (
	t0 = time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Minute)
	t2 = t0.Add(time.Hour)
)

// testPod is the pod of rank, bound to the virtual node, with an init
// container and two containers.
func testPod(name string, rank string) *corev1.Pod {
	ann := map[string]string{v1alpha1.WorkloadAnnotation: testWL}
	if rank != "" {
		ann[v1alpha1.RankAnnotation] = rank
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: testNS, UID: types.UID("uid-" + name),
			Labels:      map[string]string{v1alpha1.NodeLabel: testNode},
			Annotations: ann,
		},
		Spec: corev1.PodSpec{
			NodeName:       testNode,
			RestartPolicy:  corev1.RestartPolicyNever,
			InitContainers: []corev1.Container{{Name: "fetch", Image: "busybox"}},
			Containers: []corev1.Container{
				{Name: "main", Image: "train:1", ReadinessProbe: &corev1.Probe{}},
				{Name: "metrics", Image: "exporter:1"},
			},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodPending,
			Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}},
		},
	}
}

func testWorkload() *kueue.Workload {
	return &kueue.Workload{ObjectMeta: metav1.ObjectMeta{Name: testWL, Namespace: testNS, UID: "uid-wl"}}
}

// testRequest is a request of two members in one PodSet whose template gives
// the main container a readiness probe.
func testRequest(conds ...metav1.Condition) *v1alpha1.OffloadRequest {
	return &v1alpha1.OffloadRequest{
		ObjectMeta: metav1.ObjectMeta{Name: testWL + "-hpc-1", Namespace: testNS},
		Spec: v1alpha1.OffloadRequestSpec{
			Workload: v1alpha1.WorkloadReference{Name: testWL, UID: "uid-wl"},
			Attempt:  1,
			PodSets: []v1alpha1.PodSet{{
				Name: "main", Count: 2,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: "main", ReadinessProbe: &corev1.Probe{}},
					{Name: "metrics"},
				}}},
			}},
		},
		Status: v1alpha1.OffloadRequestStatus{Conditions: conds, JobID: "4711"},
	}
}

func cond(ty, reason string) metav1.Condition {
	return metav1.Condition{Type: ty, Status: metav1.ConditionTrue, Reason: reason, Message: "detail of " + reason}
}

func running(name string, at time.Time, ready bool) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, Ready: ready, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(at)}}}
}

func exited(name string, code int32) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		ExitCode: code, StartedAt: metav1.NewTime(t1), FinishedAt: metav1.NewTime(t2),
	}}}
}

func runningRequest() *v1alpha1.OffloadRequest {
	q := testRequest(cond(v1alpha1.OffloadRequestAdmitted, "Running"))
	q.Status.StartTime = &metav1.Time{Time: t0}
	q.Status.Ranks = []v1alpha1.RankStatus{
		{Rank: 0, PodUID: "m0", State: v1alpha1.RankRunning, Containers: []corev1.ContainerStatus{running("main", t1, true), running("metrics", t1, true)}},
		{Rank: 1, PodUID: "m1", State: v1alpha1.RankRunning, Containers: []corev1.ContainerStatus{running("main", t1, false)}},
	}
	return q
}

func condition(st corev1.PodStatus, ty corev1.PodConditionType) *corev1.PodCondition {
	for i := range st.Conditions {
		if st.Conditions[i].Type == ty {
			return &st.Conditions[i]
		}
	}
	return nil
}

func code(cs corev1.ContainerStatus) int32 {
	if cs.State.Terminated == nil {
		return -1
	}
	return cs.State.Terminated.ExitCode
}

// Each situation of a pod's member gives the pod the phase, reason and
// container states a kubelet would show.
func TestPodStatusTable(t *testing.T) {
	finished := func(reason string, ranks ...v1alpha1.RankStatus) *v1alpha1.OffloadRequest {
		q := testRequest(cond(v1alpha1.OffloadRequestFinished, reason))
		q.Status.StartTime, q.Status.CompletionTime = &metav1.Time{Time: t1}, &metav1.Time{Time: t2}
		q.Status.Ranks = ranks
		return q
	}
	rejected := testWorkload()
	rejected.Annotations = map[string]string{v1alpha1.FailedAnnotation: "the site refuses accounts it does not serve"}
	queued := testRequest(cond(v1alpha1.OffloadRequestQuotaReserved, "Submitted"))
	queued.Status.Reason, queued.Status.Message = "Priority", "waiting for resources"
	stagingIn := testRequest(cond(v1alpha1.OffloadRequestStagingIn, "Copying"))
	waitingRank := runningRequest()
	waitingRank.Status.Ranks[1] = v1alpha1.RankStatus{Rank: 1, State: v1alpha1.RankQueued}

	cases := []struct {
		name     string
		pod      *corev1.Pod
		wl       *kueue.Workload
		q        *v1alpha1.OffloadRequest
		phase    corev1.PodPhase
		reason   string
		message  string
		waiting  string
		initCode int32
		codes    []int32
	}{
		{name: "no rank", pod: testPod("train-0", ""), wl: testWorkload(), q: runningRequest(),
			phase: corev1.PodFailed, reason: ReasonNotOffloaded, message: "without being offloaded", codes: []int32{1, 1}},
		{name: "no workload annotation", pod: func() *corev1.Pod {
			p := testPod("train-0", "0")
			delete(p.Annotations, v1alpha1.WorkloadAnnotation)
			return p
		}(), wl: nil, q: nil, phase: corev1.PodFailed, reason: ReasonNotOffloaded, codes: []int32{1, 1}},
		{name: "rejected", pod: testPod("train-0", "0"), wl: rejected, q: nil,
			phase: corev1.PodFailed, reason: ReasonRejected, message: "the site refuses accounts it does not serve", codes: []int32{1, 1}},
		{name: "no live request", pod: testPod("train-0", "0"), wl: testWorkload(), q: nil,
			phase: corev1.PodPending, waiting: ReasonWaitingForAttempt},
		{name: "queued with the executor's reason", pod: testPod("train-0", "0"), wl: testWorkload(), q: queued,
			phase: corev1.PodPending, waiting: "Priority"},
		{name: "staging in", pod: testPod("train-0", "0"), wl: testWorkload(), q: stagingIn,
			phase: corev1.PodPending, waiting: ReasonStagingIn},
		{name: "not started", pod: testPod("train-0", "0"), wl: testWorkload(), q: testRequest(),
			phase: corev1.PodPending, waiting: ReasonQueued},
		{name: "rank waits for the site", pod: testPod("train-1", "1"), wl: testWorkload(), q: waitingRank,
			phase: corev1.PodPending, waiting: ReasonQueued},
		{name: "running", pod: testPod("train-0", "0"), wl: testWorkload(), q: runningRequest(),
			phase: corev1.PodRunning, initCode: 0},
		{name: "succeeded", pod: testPod("train-0", "0"), wl: testWorkload(),
			q:     finished(v1alpha1.OffloadRequestSucceeded, v1alpha1.RankStatus{Rank: 0, State: v1alpha1.RankCompleted, Containers: []corev1.ContainerStatus{exited("main", 0)}}),
			phase: corev1.PodSucceeded, codes: []int32{0, 0}},
		{name: "failed rank", pod: testPod("train-1", "1"), wl: testWorkload(),
			q: finished(v1alpha1.OffloadRequestFailed,
				v1alpha1.RankStatus{Rank: 0, State: v1alpha1.RankCompleted, Containers: []corev1.ContainerStatus{exited("main", 0)}},
				v1alpha1.RankStatus{Rank: 1, State: v1alpha1.RankFailed, Containers: []corev1.ContainerStatus{exited("main", 3)}}),
			phase: corev1.PodFailed, message: "job 4711 failed: detail of Failed", codes: []int32{3, 1}},
		{name: "completed rank of a failed job", pod: testPod("train-0", "0"), wl: testWorkload(),
			q: finished(v1alpha1.OffloadRequestFailed,
				v1alpha1.RankStatus{Rank: 0, State: v1alpha1.RankCompleted, Containers: []corev1.ContainerStatus{exited("main", 0)}},
				v1alpha1.RankStatus{Rank: 1, State: v1alpha1.RankFailed, Containers: []corev1.ContainerStatus{exited("main", 3)}}),
			phase: corev1.PodSucceeded, codes: []int32{0, 0}},
		{name: "rank 0 of a failed job whose ranks all completed", pod: testPod("train-0", "0"), wl: testWorkload(),
			q: finished(v1alpha1.OffloadRequestFailed,
				v1alpha1.RankStatus{Rank: 0, State: v1alpha1.RankCompleted, Containers: []corev1.ContainerStatus{exited("main", 0)}},
				v1alpha1.RankStatus{Rank: 1, State: v1alpha1.RankCompleted}),
			phase: corev1.PodFailed, message: "job 4711 failed: detail of Failed", codes: []int32{0, 1}},
		{name: "rank 1 of a failed job whose ranks all completed", pod: testPod("train-1", "1"), wl: testWorkload(),
			q: finished(v1alpha1.OffloadRequestFailed,
				v1alpha1.RankStatus{Rank: 0, State: v1alpha1.RankCompleted},
				v1alpha1.RankStatus{Rank: 1, State: v1alpha1.RankCompleted}),
			phase: corev1.PodSucceeded, codes: []int32{0, 0}},
		{name: "rank the site never took, of a failed job", pod: testPod("train-1", "1"), wl: testWorkload(),
			q: finished(v1alpha1.OffloadRequestFailed,
				v1alpha1.RankStatus{Rank: 0, State: v1alpha1.RankCompleted},
				v1alpha1.RankStatus{Rank: 1, State: v1alpha1.RankQueued}),
			phase: corev1.PodFailed, codes: []int32{1, 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := podStatus(c.pod, c.wl, c.q, rankAnnotation(c.pod), testHostIP, t2)
			if st.Phase != c.phase || st.Reason != c.reason {
				t.Fatalf("phase %s reason %q, want %s %q (message %q)", st.Phase, st.Reason, c.phase, c.reason, st.Message)
			}
			if !strings.Contains(st.Message, c.message) {
				t.Errorf("message %q, want it to contain %q", st.Message, c.message)
			}
			if st.HostIP != testHostIP || len(st.HostIPs) != 1 || st.HostIPs[0].IP != testHostIP {
				t.Errorf("hostIP %q %v, want the node's %s", st.HostIP, st.HostIPs, testHostIP)
			}
			if len(st.ContainerStatuses) != 2 || st.ContainerStatuses[0].Name != "main" || st.ContainerStatuses[1].Name != "metrics" {
				t.Fatalf("container statuses %+v", st.ContainerStatuses)
			}
			if len(st.InitContainerStatuses) != 1 {
				t.Fatalf("init container statuses %+v", st.InitContainerStatuses)
			}
			if condition(st, corev1.PodScheduled) == nil {
				t.Errorf("the binding's PodScheduled condition is gone: %+v", st.Conditions)
			}
			switch c.phase {
			case corev1.PodPending:
				for _, cs := range st.ContainerStatuses {
					if cs.State.Waiting == nil || cs.State.Waiting.Reason != c.waiting {
						t.Errorf("container %s state %+v, want waiting with reason %s", cs.Name, cs.State, c.waiting)
					}
				}
				if c.q != nil && c.q.Status.Message != "" && st.ContainerStatuses[0].State.Waiting.Message != c.q.Status.Message {
					t.Errorf("waiting message %q, want the executor's %q", st.ContainerStatuses[0].State.Waiting.Message, c.q.Status.Message)
				}
				if st.StartTime != nil || st.PodIP != "" {
					t.Errorf("a pending pod has startTime %v and IP %q", st.StartTime, st.PodIP)
				}
			case corev1.PodRunning:
				if !st.StartTime.Equal(&metav1.Time{Time: t1}) {
					t.Errorf("startTime %v, want the member's start %v", st.StartTime, t1)
				}
				for _, cs := range st.ContainerStatuses {
					if cs.State.Running == nil || !cs.State.Running.StartedAt.Equal(&metav1.Time{Time: t1}) || !ptr.Deref(cs.Started, false) {
						t.Errorf("container %s state %+v, want running since %v", cs.Name, cs.State, t1)
					}
				}
				if code(st.InitContainerStatuses[0]) != c.initCode {
					t.Errorf("init container %+v, want exit %d", st.InitContainerStatuses[0].State, c.initCode)
				}
			default:
				for i, want := range c.codes {
					if got := code(st.ContainerStatuses[i]); got != want {
						t.Errorf("container %s exit %d, want %d", st.ContainerStatuses[i].Name, got, want)
					}
				}
				if code(st.InitContainerStatuses[0]) != 0 {
					t.Errorf("init container exit %d, want 0", code(st.InitContainerStatuses[0]))
				}
				if r := condition(st, corev1.PodReady); r == nil || r.Status != corev1.ConditionFalse {
					t.Errorf("a pod that ended has Ready %+v", r)
				}
			}
		})
	}
}

// A pod that ended keeps its status: a kubelet never writes a terminal pod
// again, and the API server refuses its containers leaving the terminated
// state.
func TestPodStatusTerminalKept(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		pod := testPod("train-0", "0")
		pod.Status.Phase, pod.Status.Message = phase, "ended before"
		st := podStatus(pod, testWorkload(), runningRequest(), 0, testHostIP, t2)
		if st.Phase != phase || st.Message != "ended before" || st.HostIP != "" || st.ContainerStatuses != nil {
			t.Errorf("%s pod rewritten: %+v", phase, st)
		}
	}
}

func readyOf(st corev1.PodStatus) (bool, string) {
	r := condition(st, corev1.PodReady)
	if r == nil {
		return false, "no Ready condition"
	}
	return r.Status == corev1.ConditionTrue, r.Message
}

// A container is ready while its member runs and the job is not staging out;
// a container the request's template probes is ready once the site reports it
// ready, and a probe only the pod has does not count.
func TestPodStatusReady(t *testing.T) {
	t.Run("probed container ready at the site", func(t *testing.T) {
		st := podStatus(testPod("train-0", "0"), testWorkload(), runningRequest(), 0, testHostIP, t2)
		if ok, msg := readyOf(st); !ok {
			t.Errorf("rank 0 not ready: %s", msg)
		}
		if !st.ContainerStatuses[0].Ready || !st.ContainerStatuses[1].Ready {
			t.Errorf("containers %+v, want both ready", st.ContainerStatuses)
		}
		if c := condition(st, corev1.ContainersReady); c == nil || c.Status != corev1.ConditionTrue {
			t.Errorf("ContainersReady %+v", c)
		}
	})
	t.Run("probed container not ready at the site", func(t *testing.T) {
		st := podStatus(testPod("train-1", "1"), testWorkload(), runningRequest(), 1, testHostIP, t2)
		if ok, _ := readyOf(st); ok {
			t.Error("rank 1 ready while the site reports its probed container not ready")
		}
		if st.ContainerStatuses[0].Ready || !st.ContainerStatuses[1].Ready {
			t.Errorf("containers %+v, want main not ready and metrics, unprobed, ready", st.ContainerStatuses)
		}
	})
	t.Run("probe only on the pod", func(t *testing.T) {
		q := runningRequest()
		q.Spec.PodSets[0].Template.Spec.Containers[0].ReadinessProbe = nil
		st := podStatus(testPod("train-1", "1"), testWorkload(), q, 1, testHostIP, t2)
		if ok, msg := readyOf(st); !ok {
			t.Errorf("a probe the request's template has not holds the pod back: %s", msg)
		}
	})
	t.Run("staging out", func(t *testing.T) {
		q := runningRequest()
		q.Status.Conditions = append(q.Status.Conditions, cond(v1alpha1.OffloadRequestStagingOut, "Copying"))
		st := podStatus(testPod("train-0", "0"), testWorkload(), q, 0, testHostIP, t2)
		if ok, msg := readyOf(st); ok || !strings.Contains(msg, "staging out") || st.Phase != corev1.PodRunning {
			t.Errorf("phase %s ready %v (%s), want running, not ready, staging out", st.Phase, ok, msg)
		}
	})
	t.Run("member ended, job runs on", func(t *testing.T) {
		q := runningRequest()
		q.Status.Ranks[1].State = v1alpha1.RankCompleted
		q.Spec.PodSets[0].Template.Spec.Containers[0].ReadinessProbe = nil
		st := podStatus(testPod("train-1", "1"), testWorkload(), q, 1, testHostIP, t2)
		if ok, _ := readyOf(st); ok || st.Phase != corev1.PodRunning {
			t.Errorf("phase %s ready %v, want an ended member running and not ready", st.Phase, ok)
		}
	})
}

// Rank 0 carries the shadow's IP, so that Services select it, and is not ready
// until the shadow is; the other ranks have no IP and do not wait for it.
func TestPodStatusShadow(t *testing.T) {
	q := runningRequest()
	q.Spec.PodSets[0].Template.Spec.Containers[0].ReadinessProbe = nil
	q.Status.Shadow = &v1alpha1.ShadowReference{Deployment: "shadow-x", PodIP: "10.42.0.9"}
	st := podStatus(testPod("train-0", "0"), testWorkload(), q, 0, testHostIP, t2)
	if st.PodIP != "10.42.0.9" || len(st.PodIPs) != 1 || st.PodIPs[0].IP != "10.42.0.9" {
		t.Errorf("rank 0 IP %q %v, want the shadow's", st.PodIP, st.PodIPs)
	}
	if ok, msg := readyOf(st); ok || !strings.Contains(msg, "shadow") {
		t.Errorf("rank 0 ready %v (%s) while the shadow is not", ok, msg)
	}
	other := podStatus(testPod("train-1", "1"), testWorkload(), q, 1, testHostIP, t2)
	if other.PodIP != "" {
		t.Errorf("rank 1 has IP %q", other.PodIP)
	}
	if ok, msg := readyOf(other); !ok {
		t.Errorf("rank 1 waits for the shadow: %s", msg)
	}
	q.Status.Shadow.Ready = true
	if ok, msg := readyOf(podStatus(testPod("train-0", "0"), testWorkload(), q, 0, testHostIP, t2)); !ok {
		t.Errorf("rank 0 not ready with the shadow ready: %s", msg)
	}
}

// A pod shown running goes back to pending when its attempt is given up and
// the next one waits, and its init containers keep the terminated state the
// API server lets no pod that is not restarted leave.
func TestPodStatusPendingAgain(t *testing.T) {
	pod := testPod("train-0", "0")
	pod.Status = podStatus(pod, testWorkload(), runningRequest(), 0, testHostIP, t1)
	if pod.Status.Phase != corev1.PodRunning {
		t.Fatalf("phase %s", pod.Status.Phase)
	}
	st := podStatus(pod, testWorkload(), nil, 0, testHostIP, t2)
	if st.Phase != corev1.PodPending || st.StartTime != nil {
		t.Fatalf("phase %s startTime %v, want pending again", st.Phase, st.StartTime)
	}
	if st.InitContainerStatuses[0].State.Terminated == nil {
		t.Errorf("init container left the terminated state: %+v", st.InitContainerStatuses[0].State)
	}
	if c := condition(st, corev1.PodInitialized); c == nil || c.Status != corev1.ConditionTrue {
		t.Errorf("Initialized %+v, want true with the init container done", c)
	}
	if st.ContainerStatuses[0].State.Waiting == nil {
		t.Errorf("container %+v, want waiting", st.ContainerStatuses[0].State)
	}
}

// The init containers of a running pod are as the site reports them.
func TestPodStatusInitFromSite(t *testing.T) {
	q := runningRequest()
	q.Status.Ranks[0].InitContainers = []corev1.ContainerStatus{exited("fetch", 0)}
	st := podStatus(testPod("train-0", "0"), testWorkload(), q, 0, testHostIP, t2)
	got := st.InitContainerStatuses[0].State.Terminated
	if got == nil || !got.FinishedAt.Equal(&metav1.Time{Time: t2}) {
		t.Errorf("init container %+v, want the site's, finished at %v", st.InitContainerStatuses[0].State, t2)
	}

	// A sidecar (an init container that restarts always) runs with the job.
	pod := testPod("train-0", "0")
	pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{Name: "proxy", RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways)})
	st = podStatus(pod, testWorkload(), runningRequest(), 0, testHostIP, t2)
	if side := st.InitContainerStatuses[1]; side.State.Running == nil || !side.Ready {
		t.Errorf("sidecar %+v ready %v, want running and ready", side.State, side.Ready)
	}
}

// Ranks are numbered across the request's PodSets in order.
func TestRankTemplate(t *testing.T) {
	q := testRequest()
	q.Spec.PodSets = []v1alpha1.PodSet{
		{Name: "head", Count: 1, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Name: "head"}}},
		{Name: "workers", Count: 3, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Name: "workers"}}},
	}
	for rank, want := range map[int]string{0: "head", 1: "workers", 3: "workers"} {
		if got := rankTemplate(q, rank); got == nil || got.Name != want {
			t.Errorf("rank %d template %v, want %s", rank, got, want)
		}
	}
	if got := rankTemplate(q, 4); got != nil {
		t.Errorf("rank 4 of 4 members has template %s", got.Name)
	}
	pod := testPod("train-0", "4")
	if st := podStatus(pod, testWorkload(), q, 4, testHostIP, t2); st.Phase != corev1.PodFailed || st.Reason != ReasonNotOffloaded {
		t.Errorf("rank past the members: phase %s reason %s", st.Phase, st.Reason)
	}
}
