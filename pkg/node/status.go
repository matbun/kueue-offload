package node

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// Reasons the virtual node gives a pod, on the pod or on its waiting
// containers.
const (
	// ReasonNotOffloaded fails a pod bound to the node without being offloaded:
	// it names no Workload or no rank.
	ReasonNotOffloaded = "NotOffloaded"
	// ReasonRejected fails the pods of a Workload whose job will never run.
	ReasonRejected = "Rejected"
	// ReasonQueued, ReasonStagingIn and ReasonWaitingForAttempt are why the
	// containers of a pod whose member has not started wait, when the executor
	// gives no reason of its own.
	ReasonQueued            = "Queued"
	ReasonStagingIn         = "StagingIn"
	ReasonWaitingForAttempt = "WaitingForAttempt"
)

// Reasons of the pod conditions, as a kubelet writes them.
const (
	reasonNotInitialized = "ContainersNotInitialized"
	reasonNotReady       = "ContainersNotReady"
	reasonCompleted      = "PodCompleted"
)

// podStatus is the status of pod, bound to the virtual node whose address is
// hostIP, while it stands for member rank of the job of its Workload wl: the
// newest attempt q that is not being deleted, nil when there is none. rank is
// negative when the pod has no rank. A pod already Succeeded or Failed keeps its
// status. podStatus does not write: the caller patches the pod when the result
// differs.
func podStatus(pod *corev1.Pod, wl *kueue.Workload, q *v1alpha1.OffloadRequest, rank int, hostIP string, now time.Time) corev1.PodStatus {
	st := pod.Status.DeepCopy()
	if podFinished(pod) {
		return *st
	}
	now = now.Truncate(time.Second)
	st.HostIP, st.HostIPs = "", nil
	if net.ParseIP(hostIP) != nil {
		st.HostIP, st.HostIPs = hostIP, []corev1.HostIP{{IP: hostIP}}
	}
	members := 0
	if q != nil {
		members = memberCount(q)
	}
	switch {
	case rank < 0 || pod.Annotations[v1alpha1.WorkloadAnnotation] == "":
		rejectedStatus(st, pod, ReasonNotOffloaded, fmt.Sprintf("bound to %s without being offloaded: no %s or %s annotation",
			pod.Spec.NodeName, v1alpha1.WorkloadAnnotation, v1alpha1.RankAnnotation), now)
	case wl != nil && wl.Annotations[v1alpha1.FailedAnnotation] != "":
		rejectedStatus(st, pod, ReasonRejected, wl.Annotations[v1alpha1.FailedAnnotation], now)
	case q == nil:
		pendingStatus(st, pod, ReasonWaitingForAttempt, "", now)
	case rank >= members:
		rejectedStatus(st, pod, ReasonNotOffloaded, fmt.Sprintf("rank %d: %s has %d member(s)", rank, jobOf(q), members), now)
	case meta.IsStatusConditionTrue(q.Status.Conditions, v1alpha1.OffloadRequestFinished):
		finishedStatus(st, pod, q, rank, now)
	case !started(q) || rankWaits(q, rank):
		reason := q.Status.Reason
		switch {
		case reason != "":
		case meta.IsStatusConditionTrue(q.Status.Conditions, v1alpha1.OffloadRequestStagingIn):
			reason = ReasonStagingIn
		default:
			reason = ReasonQueued
		}
		pendingStatus(st, pod, reason, q.Status.Message, now)
	default:
		runningStatus(st, pod, q, rank, now)
	}
	return *st
}

// started reports whether q's job started at the site.
func started(q *v1alpha1.OffloadRequest) bool {
	return q.Status.StartTime != nil ||
		meta.IsStatusConditionTrue(q.Status.Conditions, v1alpha1.OffloadRequestAdmitted) ||
		meta.IsStatusConditionTrue(q.Status.Conditions, v1alpha1.OffloadRequestFinished)
}

// rankWaits reports whether member rank of q's started job has not started
// itself: it waits for the site to take it, or the site has it queued.
func rankWaits(q *v1alpha1.OffloadRequest, rank int) bool {
	r := rankOf(q, rank)
	return r == nil || r.PodUID == "" || r.State == v1alpha1.RankQueued
}

func rankOf(q *v1alpha1.OffloadRequest, rank int) *v1alpha1.RankStatus {
	for i := range q.Status.Ranks {
		if int(q.Status.Ranks[i].Rank) == rank {
			return &q.Status.Ranks[i]
		}
	}
	return nil
}

// memberCount is the number of members of q's job, every PodSet's count.
func memberCount(q *v1alpha1.OffloadRequest) int {
	n := 0
	for _, ps := range q.Spec.PodSets {
		n += int(ps.Count)
	}
	return n
}

// rankTemplate is the template of the PodSet of q that member rank belongs to:
// the PodSets' members are numbered in order.
func rankTemplate(q *v1alpha1.OffloadRequest, rank int) *corev1.PodTemplateSpec {
	off := 0
	for i := range q.Spec.PodSets {
		off += int(q.Spec.PodSets[i].Count)
		if rank < off {
			return &q.Spec.PodSets[i].Template
		}
	}
	return nil
}

// pendingStatus is the status of a pod whose member waits: every container
// waits with reason and message. Init containers that ended keep their state,
// which a pod that is not restarted may not leave.
func pendingStatus(st *corev1.PodStatus, pod *corev1.Pod, reason, msg string, now time.Time) {
	st.Phase, st.Reason, st.Message = corev1.PodPending, "", ""
	st.StartTime, st.PodIP, st.PodIPs = nil, "", nil
	waiting := corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: msg}}
	initialized := true
	inits := make([]corev1.ContainerStatus, 0, len(pod.Spec.InitContainers))
	for _, ctr := range pod.Spec.InitContainers {
		cs := corev1.ContainerStatus{Name: ctr.Name, Image: ctr.Image, State: waiting, Started: ptr.To(false)}
		if old := statusOf(pod.Status.InitContainerStatuses, ctr.Name); old != nil && old.State.Terminated != nil {
			cs = *old.DeepCopy()
		} else {
			initialized = false
		}
		inits = append(inits, cs)
	}
	st.InitContainerStatuses = nilIfEmpty(inits)
	st.ContainerStatuses = make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	for _, ctr := range pod.Spec.Containers {
		st.ContainerStatuses = append(st.ContainerStatuses, corev1.ContainerStatus{
			Name: ctr.Name, Image: ctr.Image, State: waiting, Started: ptr.To(false),
		})
	}
	unready := "containers with unready status: [" + strings.Join(containerNames(pod.Spec.Containers), " ") + "]"
	st.Conditions = setPodCondition(st.Conditions, corev1.PodReadyToStartContainers, corev1.ConditionFalse, "", "", now)
	if initialized {
		st.Conditions = setPodCondition(st.Conditions, corev1.PodInitialized, corev1.ConditionTrue, "", "", now)
	} else {
		st.Conditions = setPodCondition(st.Conditions, corev1.PodInitialized, corev1.ConditionFalse, reasonNotInitialized,
			"containers with incomplete status: ["+strings.Join(containerNames(pod.Spec.InitContainers), " ")+"]", now)
	}
	st.Conditions = setPodCondition(st.Conditions, corev1.ContainersReady, corev1.ConditionFalse, reasonNotReady, unready, now)
	st.Conditions = setPodCondition(st.Conditions, corev1.PodReady, corev1.ConditionFalse, reasonNotReady, unready, now)
}

// runningStatus is the status of a pod whose member runs, or ended while the
// rest of the job runs or stages out. Its containers run since the member's
// containers started; its init containers ended, as the site reports them or
// with exit code 0. A container is ready while the member runs and the job is
// not staging out, and, when the request's template gives it a readiness
// probe, once the site reports it ready: the site runs the probe in the job,
// for example a notebook's test of the socket its server listens on. Rank 0
// carries the shadow's IP, so that Services select it as any pod, and is not
// ready until the shadow is.
func runningStatus(st *corev1.PodStatus, pod *corev1.Pod, q *v1alpha1.OffloadRequest, rank int, now time.Time) {
	r := rankOf(q, rank)
	start := memberStart(r)
	if start == nil {
		start = q.Status.StartTime
	}
	if start == nil {
		start = st.StartTime
	}
	if start == nil {
		start = &metav1.Time{Time: now}
	}
	sh := q.Status.Shadow
	st.Phase, st.Reason, st.Message, st.StartTime = corev1.PodRunning, "", "", start.DeepCopy()
	st.PodIP, st.PodIPs = "", nil
	if rank == 0 && sh != nil && net.ParseIP(sh.PodIP) != nil {
		st.PodIP, st.PodIPs = sh.PodIP, []corev1.PodIP{{IP: sh.PodIP}}
	}

	// notReady holds every container back, a probe at the site only its own.
	notReady := ""
	switch {
	case meta.IsStatusConditionTrue(q.Status.Conditions, v1alpha1.OffloadRequestStagingOut):
		notReady = jobOf(q) + " ended, staging out"
	case r.State != v1alpha1.RankRunning:
		notReady = fmt.Sprintf("rank %d of %s ended, the job runs on", rank, jobOf(q))
	case rank == 0 && sh != nil && !sh.Ready:
		notReady = "the shadow of " + jobOf(q) + " is not ready"
	}
	probed := map[string]bool{}
	if t := rankTemplate(q, rank); t != nil {
		for _, ctr := range t.Spec.Containers {
			if ctr.ReadinessProbe != nil {
				probed[ctr.Name] = true
			}
		}
	}
	ready := func(name string) bool {
		if notReady != "" {
			return false
		}
		if !probed[name] {
			return true
		}
		cs := statusOf(r.Containers, name)
		return cs != nil && cs.Ready
	}

	st.InitContainerStatuses = nil
	for _, ctr := range pod.Spec.InitContainers {
		own := statusOf(r.InitContainers, ctr.Name)
		switch {
		case ctr.RestartPolicy != nil && *ctr.RestartPolicy == corev1.ContainerRestartPolicyAlways:
			st.InitContainerStatuses = append(st.InitContainerStatuses, runningContainer(ctr, own, *start, notReady == ""))
		case own != nil && own.State.Terminated != nil:
			st.InitContainerStatuses = append(st.InitContainerStatuses, corev1.ContainerStatus{
				Name: ctr.Name, Image: ctr.Image, Started: ptr.To(false), State: corev1.ContainerState{Terminated: own.State.Terminated.DeepCopy()},
			})
		default:
			st.InitContainerStatuses = append(st.InitContainerStatuses, terminatedContainer(ctr, 0, *start, *start))
		}
	}
	st.ContainerStatuses = make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	var unready []string
	for _, ctr := range pod.Spec.Containers {
		ok := ready(ctr.Name)
		if !ok {
			unready = append(unready, ctr.Name)
		}
		st.ContainerStatuses = append(st.ContainerStatuses, runningContainer(ctr, statusOf(r.Containers, ctr.Name), *start, ok))
	}

	readyStatus, reason, msg := corev1.ConditionTrue, "", ""
	if len(unready) > 0 {
		readyStatus, reason, msg = corev1.ConditionFalse, reasonNotReady, notReady
		if msg == "" {
			msg = fmt.Sprintf("containers not ready at the site: [%s]", strings.Join(unready, " "))
		}
	}
	st.Conditions = setPodCondition(st.Conditions, corev1.PodReadyToStartContainers, corev1.ConditionTrue, "", "", now)
	st.Conditions = setPodCondition(st.Conditions, corev1.PodInitialized, corev1.ConditionTrue, "", "", now)
	st.Conditions = setPodCondition(st.Conditions, corev1.ContainersReady, readyStatus, reason, msg, now)
	st.Conditions = setPodCondition(st.Conditions, corev1.PodReady, readyStatus, reason, msg, now)
}

// finishedStatus is the status of a pod whose job finished. A pod of a job that
// succeeded succeeds. Of a job that failed, a pod fails unless its member
// completed; when every member completed (a failed stage-out, a site that
// refused the job), rank 0 fails, so that the owner fails too. Containers take
// their member's exit codes by name; one without a code exits with the member's
// code, else 0 in a pod that succeeds and 1 in one that fails.
func finishedStatus(st *corev1.PodStatus, pod *corev1.Pod, q *v1alpha1.OffloadRequest, rank int, now time.Time) {
	c := meta.FindStatusCondition(q.Status.Conditions, v1alpha1.OffloadRequestFinished)
	succeeded := c.Reason == v1alpha1.OffloadRequestSucceeded
	completed := func(i int) bool {
		r := rankOf(q, i)
		return r != nil && r.State == v1alpha1.RankCompleted
	}
	fails := false
	if !succeeded {
		fails = !completed(rank)
		if rank == 0 && !fails {
			fails = true
			for i := range memberCount(q) {
				if !completed(i) {
					fails = false
					break
				}
			}
		}
	}

	r := rankOf(q, rank)
	end := q.Status.CompletionTime
	if end == nil {
		end = &metav1.Time{Time: now}
	}
	start := memberStart(r)
	if start == nil {
		start = q.Status.StartTime
	}
	if start == nil {
		start = st.StartTime
	}
	if start == nil {
		start = end
	}
	if st.StartTime == nil {
		st.StartTime = start.DeepCopy()
	}

	st.Phase, st.Reason, st.Message = corev1.PodSucceeded, "", ""
	if fails {
		detail := c.Message
		if detail == "" {
			detail = q.Status.Message
		}
		st.Phase, st.Message = corev1.PodFailed, withDetail(jobOf(q)+" failed", detail)
	}
	var memberCode int32
	var own, ownInit []corev1.ContainerStatus
	if r != nil {
		memberCode, own, ownInit = r.ExitCode, r.Containers, r.InitContainers
	}
	st.ContainerStatuses = make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	for _, ctr := range pod.Spec.Containers {
		cs := statusOf(own, ctr.Name)
		var t *corev1.ContainerStateTerminated
		if cs != nil {
			t = cs.State.Terminated
			if t == nil {
				t = cs.LastTerminationState.Terminated
			}
		}
		if t != nil {
			st.ContainerStatuses = append(st.ContainerStatuses, corev1.ContainerStatus{
				Name: ctr.Name, Image: ctr.Image, Started: ptr.To(false), State: corev1.ContainerState{Terminated: t.DeepCopy()},
			})
			continue
		}
		code := memberCode
		switch {
		case code != 0:
		case fails:
			code = 1
		}
		st.ContainerStatuses = append(st.ContainerStatuses, terminatedContainer(ctr, code, *start, *end))
	}
	st.InitContainerStatuses = nil
	for _, ctr := range pod.Spec.InitContainers {
		if cs := statusOf(ownInit, ctr.Name); cs != nil && cs.State.Terminated != nil {
			st.InitContainerStatuses = append(st.InitContainerStatuses, corev1.ContainerStatus{
				Name: ctr.Name, Image: ctr.Image, Started: ptr.To(false), State: corev1.ContainerState{Terminated: cs.State.Terminated.DeepCopy()},
			})
			continue
		}
		st.InitContainerStatuses = append(st.InitContainerStatuses, terminatedContainer(ctr, 0, *start, *start))
	}
	terminalConditions(st, now)
}

// rejectedStatus fails a pod whose member never runs, with reason and msg, as
// a kubelet fails a pod it refuses: its containers exit with code 1 and its
// init containers with 0.
func rejectedStatus(st *corev1.PodStatus, pod *corev1.Pod, reason, msg string, now time.Time) {
	at := metav1.Time{Time: now}
	st.Phase, st.Reason, st.Message = corev1.PodFailed, reason, msg
	st.PodIP, st.PodIPs = "", nil
	if st.StartTime == nil {
		st.StartTime = at.DeepCopy()
	}
	st.InitContainerStatuses = nil
	for _, ctr := range pod.Spec.InitContainers {
		st.InitContainerStatuses = append(st.InitContainerStatuses, terminatedContainer(ctr, 0, at, at))
	}
	st.ContainerStatuses = make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	for _, ctr := range pod.Spec.Containers {
		st.ContainerStatuses = append(st.ContainerStatuses, terminatedContainer(ctr, 1, at, at))
	}
	terminalConditions(st, now)
}

// terminalConditions are a kubelet's conditions of a pod that ended.
func terminalConditions(st *corev1.PodStatus, now time.Time) {
	st.Conditions = setPodCondition(st.Conditions, corev1.PodReadyToStartContainers, corev1.ConditionFalse, "", "", now)
	st.Conditions = setPodCondition(st.Conditions, corev1.PodInitialized, corev1.ConditionTrue, reasonCompleted, "", now)
	st.Conditions = setPodCondition(st.Conditions, corev1.ContainersReady, corev1.ConditionFalse, reasonCompleted, "", now)
	st.Conditions = setPodCondition(st.Conditions, corev1.PodReady, corev1.ConditionFalse, reasonCompleted, "", now)
}

// runningContainer is ctr running since the site's start of it, else since
// start.
func runningContainer(ctr corev1.Container, own *corev1.ContainerStatus, start metav1.Time, ready bool) corev1.ContainerStatus {
	if own != nil && own.State.Running != nil && !own.State.Running.StartedAt.IsZero() {
		start = own.State.Running.StartedAt
	}
	return corev1.ContainerStatus{
		Name: ctr.Name, Image: ctr.Image, Ready: ready, Started: ptr.To(true),
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: start}},
	}
}

func terminatedContainer(ctr corev1.Container, code int32, started, finished metav1.Time) corev1.ContainerStatus {
	reason := "Completed"
	if code != 0 {
		reason = "Error"
	}
	return corev1.ContainerStatus{
		Name: ctr.Name, Image: ctr.Image, Started: ptr.To(false),
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: code, Reason: reason, StartedAt: started, FinishedAt: finished,
		}},
	}
}

// memberStart is when the site started the member's first container, or nil.
func memberStart(r *v1alpha1.RankStatus) *metav1.Time {
	if r == nil {
		return nil
	}
	var start *metav1.Time
	for _, cs := range r.Containers {
		var t metav1.Time
		switch {
		case cs.State.Running != nil:
			t = cs.State.Running.StartedAt
		case cs.State.Terminated != nil:
			t = cs.State.Terminated.StartedAt
		}
		if !t.IsZero() && (start == nil || t.Before(start)) {
			start = &t
		}
	}
	return start
}

func statusOf(statuses []corev1.ContainerStatus, name string) *corev1.ContainerStatus {
	for i := range statuses {
		if statuses[i].Name == name {
			return &statuses[i]
		}
	}
	return nil
}

func containerNames(ctrs []corev1.Container) []string {
	names := make([]string, 0, len(ctrs))
	for _, c := range ctrs {
		names = append(names, c.Name)
	}
	return names
}

func nilIfEmpty(s []corev1.ContainerStatus) []corev1.ContainerStatus {
	if len(s) == 0 {
		return nil
	}
	return s
}

// setPodCondition sets the condition of type ty, moving its lastTransitionTime
// only when its status changes.
func setPodCondition(conds []corev1.PodCondition, ty corev1.PodConditionType, status corev1.ConditionStatus, reason, msg string, now time.Time) []corev1.PodCondition {
	c := corev1.PodCondition{Type: ty, Status: status, Reason: reason, Message: msg, LastTransitionTime: metav1.NewTime(now)}
	for i := range conds {
		if conds[i].Type == ty {
			if conds[i].Status == status {
				c.LastTransitionTime = conds[i].LastTransitionTime
			}
			conds[i] = c
			return conds
		}
	}
	return append(conds, c)
}

func podFinished(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// rankAnnotation is the rank pod stands for, or -1 when it has none.
func rankAnnotation(pod *corev1.Pod) int {
	n, err := strconv.Atoi(pod.Annotations[v1alpha1.RankAnnotation])
	if err != nil || n < 0 {
		return -1
	}
	return n
}

func jobOf(q *v1alpha1.OffloadRequest) string {
	if q.Status.JobID == "" {
		return "the job"
	}
	return "job " + q.Status.JobID
}

func withDetail(msg, detail string) string {
	if detail != "" {
		msg += ": " + detail
	}
	return msg
}
