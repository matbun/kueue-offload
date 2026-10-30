package offload

import (
	"context"
	"fmt"
	"net"
	"slices"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/shadow"
)

// Reasons written on the owner's conditions and on the owner pods.
const (
	ReasonOffload  = "KueueOffload"
	ReasonRejected = "Rejected"
)

// workloadOwner is what a Workload finishes through: its controller, a
// batch/v1 Job, a JobSet, a TrainJob, a RayJob or a RayCluster, or the pods of
// Kueue's pod integration, one pod or a pod group.
type workloadOwner struct {
	ref  *metav1.OwnerReference
	pods []metav1.OwnerReference
	// workload is the Workload's key and uid its UID: a RayCluster's run ends
	// by deactivating it.
	workload types.NamespacedName
	uid      types.UID
}

// ownerKind is a controller owner the offload controller finishes through its status.
type ownerKind struct {
	gvk schema.GroupVersionKind
	// complete and failed are the condition types Kueue's integration reads.
	complete, failed string
	// terminalState is set for JobSet, which also carries status.terminalState.
	terminalState bool
}

var (
	jobKind        = ownerKind{gvk: batchv1.SchemeGroupVersion.WithKind("Job")}
	jobSetKind     = ownerKind{gvk: schema.GroupVersionKind{Group: "jobset.x-k8s.io", Version: "v1alpha2", Kind: "JobSet"}, complete: "Completed", failed: "Failed", terminalState: true}
	trainJobKind   = ownerKind{gvk: schema.GroupVersionKind{Group: "trainer.kubeflow.org", Version: "v1alpha1", Kind: "TrainJob"}, complete: "Complete", failed: "Failed"}
	rayJobKind     = ownerKind{gvk: schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayJob"}}
	rayClusterKind = ownerKind{gvk: schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayCluster"}}
	ownerKinds     = []ownerKind{jobKind, jobSetKind, trainJobKind, rayJobKind, rayClusterKind}
)

func kindOf(ref *metav1.OwnerReference) (ownerKind, bool) {
	for _, k := range ownerKinds {
		if ref.APIVersion == k.gvk.GroupVersion().String() && ref.Kind == k.gvk.Kind {
			return k, true
		}
	}
	return ownerKind{}, false
}

// ownerOf returns the Workload's owner, or nil and a message saying why the
// offload controller cannot handle it.
func ownerOf(wl *kueue.Workload) (*workloadOwner, string) {
	key := types.NamespacedName{Namespace: wl.Namespace, Name: wl.Name}
	c := metav1.GetControllerOf(wl)
	if c != nil {
		if _, ok := kindOf(c); ok {
			return &workloadOwner{ref: c, workload: key, uid: wl.UID}, ""
		}
	}
	if c != nil && !isPod(c) {
		return nil, unsupportedOwner(c)
	}
	// A single pod is the Workload's controller; the members of a pod group
	// own it without a controller.
	var pods []metav1.OwnerReference
	for i := range wl.OwnerReferences {
		ref := &wl.OwnerReferences[i]
		if !isPod(ref) {
			return nil, unsupportedOwner(ref)
		}
		pods = append(pods, *ref)
	}
	if len(pods) == 0 {
		return nil, unsupportedOwner(nil)
	}
	return &workloadOwner{pods: pods, workload: key, uid: wl.UID}, ""
}

// singlePod is the pod of a Workload of one plain pod, its controller, or nil.
func (o *workloadOwner) singlePod() *metav1.OwnerReference {
	if o == nil || o.ref != nil || len(o.pods) != 1 || !ptr.Deref(o.pods[0].Controller, false) {
		return nil
	}
	return &o.pods[0]
}

func isPod(ref *metav1.OwnerReference) bool {
	return ref.APIVersion == "v1" && ref.Kind == "Pod"
}

func unsupportedOwner(ref *metav1.OwnerReference) string {
	what := "no owner"
	if ref != nil {
		what = fmt.Sprintf("owner %s %s", ref.APIVersion, ref.Kind)
	}
	return "kueue-offload handles only batch/v1 Job, jobset.x-k8s.io/v1alpha2 JobSet, " +
		"trainer.kubeflow.org/v1alpha1 TrainJob, ray.io/v1 RayJob and RayCluster, and v1 Pod workloads, this one has " + what
}

// readOwner returns the owner the executor is told about and the object it
// read: the controller, or the first owner pod that still exists, named after
// its pod group. A nil reference means the owner is gone.
func readOwner(ctx context.Context, r client.Reader, namespace string, o *workloadOwner) (*v1alpha1.OwnerReference, metav1.Object, error) {
	if o.ref != nil {
		k, _ := kindOf(o.ref)
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(k.gvk)
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: o.ref.Name}, obj); err != nil {
			return nil, nil, client.IgnoreNotFound(err)
		}
		if obj.GetUID() != o.ref.UID {
			return nil, nil, nil
		}
		ref := &v1alpha1.OwnerReference{APIVersion: o.ref.APIVersion, Kind: o.ref.Kind, Name: o.ref.Name, UID: o.ref.UID}
		return ref, obj, nil
	}
	for _, p := range o.pods {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: p.Name}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, nil, err
		}
		if pod.UID != p.UID {
			continue
		}
		name := pod.Name
		if g := pod.Labels[podconstants.GroupNameLabel]; g != "" {
			name = g
		}
		return &v1alpha1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: name, UID: pod.UID}, pod, nil
	}
	return nil, nil, nil
}

// finishOwner writes the terminal state on the Workload's owner. A RayCluster
// has none that Kueue reads: when its job ends its Workload is deactivated, and
// when it is rejected Kueue deactivates it.
func finishOwner(ctx context.Context, c client.Client, r client.Reader, namespace string, o *workloadOwner, out jobOutcome, now time.Time) error {
	if o.ref == nil {
		return finishPods(ctx, c, r, namespace, o.pods, out, now)
	}
	key := types.NamespacedName{Namespace: namespace, Name: o.ref.Name}
	switch k, _ := kindOf(o.ref); k {
	case jobKind:
		return finishJob(ctx, c, r, key, o.ref.UID, out, now)
	case rayJobKind:
		return finishRayJob(ctx, c, r, key, o.ref.UID, out, now)
	case rayClusterKind:
		if out.reason == ReasonRejected {
			return nil
		}
		return deactivateWorkload(ctx, c, r, o.workload, o.uid)
	default:
		return finishConditions(ctx, c, r, k, key, o.ref.UID, out, now)
	}
}

// jobOutcome is the terminal state written on the owner.
type jobOutcome struct {
	succeeded      bool
	reason         string
	message        string
	startTime      *metav1.Time
	completionTime *metav1.Time
	// exitCodes by container name, from the head rank; nil when the executor
	// reported none.
	exitCodes map[string]int32
}

// finishJob patches the owner Job's status to Complete or Failed.
// The Job controller stops syncing a finished Job and Kueue then marks the
// Workload Finished. Clusters that require SuccessCriteriaMet or FailureTarget
// before the terminal condition get them; a cluster that refuses them with 422
// gets the patch again without them.
func finishJob(ctx context.Context, c client.Client, r client.Reader, key types.NamespacedName, uid types.UID, o jobOutcome, now time.Time) error {
	job := &batchv1.Job{}
	if err := r.Get(ctx, key, job); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if uid != "" && job.UID != uid {
		return nil
	}
	if jobFinished(job) {
		return nil
	}
	err := c.Status().Patch(ctx, finishedJob(job, o, now, true), client.MergeFrom(job))
	if apierrors.IsInvalid(err) {
		err = c.Status().Patch(ctx, finishedJob(job, o, now, false), client.MergeFrom(job))
	}
	return err
}

func jobFinished(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func finishedJob(orig *batchv1.Job, o jobOutcome, now time.Time, interim bool) *batchv1.Job {
	job := orig.DeepCopy()
	t := metav1.NewTime(now)
	cond := func(ty batchv1.JobConditionType) batchv1.JobCondition {
		return batchv1.JobCondition{
			Type: ty, Status: corev1.ConditionTrue, Reason: o.reason, Message: o.message,
			LastProbeTime: t, LastTransitionTime: t,
		}
	}
	job.Status.Active = 0
	if o.startTime != nil {
		job.Status.StartTime = o.startTime.DeepCopy()
	} else if job.Status.StartTime == nil {
		job.Status.StartTime = &t
	}
	if o.succeeded {
		n := int32(1)
		if job.Spec.Completions != nil {
			n = *job.Spec.Completions
		} else if job.Spec.Parallelism != nil {
			n = *job.Spec.Parallelism
		}
		job.Status.Succeeded = n
		if job.Spec.CompletionMode != nil && *job.Spec.CompletionMode == batchv1.IndexedCompletion {
			job.Status.CompletedIndexes = indexRange(n)
		}
		job.Status.CompletionTime = &t
		if o.completionTime != nil {
			job.Status.CompletionTime = o.completionTime.DeepCopy()
		}
		if interim {
			job.Status.Conditions = append(job.Status.Conditions, cond(batchv1.JobSuccessCriteriaMet))
		}
		job.Status.Conditions = append(job.Status.Conditions, cond(batchv1.JobComplete))
		return job
	}
	job.Status.Failed = 1
	if interim {
		job.Status.Conditions = append(job.Status.Conditions, cond(batchv1.JobFailureTarget))
	}
	job.Status.Conditions = append(job.Status.Conditions, cond(batchv1.JobFailed))
	return job
}

func indexRange(n int32) string {
	if n <= 1 {
		return "0"
	}
	return fmt.Sprintf("0-%d", n-1)
}

// finishConditions patches a JobSet's or a TrainJob's status with its
// terminal condition, which Kueue's integration for the kind reads to mark the
// Workload Finished. Owners already terminal or gone are skipped.
func finishConditions(ctx context.Context, c client.Client, r client.Reader, k ownerKind, key types.NamespacedName, uid types.UID, o jobOutcome, now time.Time) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(k.gvk)
	if err := r.Get(ctx, key, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if uid != "" && obj.GetUID() != uid {
		return nil
	}
	conds, _, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil {
		return fmt.Errorf("%s %s: status.conditions: %w", k.gvk.Kind, key, err)
	}
	for _, raw := range conds {
		cond, _ := raw.(map[string]any)
		if (cond["type"] == k.complete || cond["type"] == k.failed) && cond["status"] == string(metav1.ConditionTrue) {
			return nil
		}
	}
	ty, terminal := k.complete, "Completed"
	if !o.succeeded {
		ty, terminal = k.failed, "Failed"
	}
	msg := o.message
	if len(msg) > 32768 {
		msg = msg[:32768]
	}
	conds = append(conds, map[string]any{
		"type": ty, "status": string(metav1.ConditionTrue), "reason": o.reason, "message": msg,
		"lastTransitionTime": now.UTC().Format(time.RFC3339),
	})
	done := obj.DeepCopy()
	if err := unstructured.SetNestedSlice(done.Object, conds, "status", "conditions"); err != nil {
		return err
	}
	if k.terminalState {
		if err := unstructured.SetNestedField(done.Object, terminal, "status", "terminalState"); err != nil {
			return err
		}
	}
	return client.IgnoreNotFound(c.Status().Patch(ctx, done, client.MergeFrom(obj)))
}

// finishPods sets every owner pod's phase to Succeeded or Failed, with a
// terminated status for each of its containers and Ready False, so a workflow
// engine that reads the main container sees how the step ended; the message
// ends with the logs command of a pod that has one (LogsAnnotation). Kueue then
// removes its finalizers and marks the Workload Finished. A failed pod of a group is
// first annotated retriable-in-group "false": without it Kueue waits for
// replacement pods and never finishes the group. The pods stay gated:
// spec.nodeName is never set. Pods already terminal or gone are skipped.
func finishPods(ctx context.Context, c client.Client, r client.Reader, namespace string, refs []metav1.OwnerReference, o jobOutcome, now time.Time) error {
	phase := corev1.PodFailed
	if o.succeeded {
		phase = corev1.PodSucceeded
	}
	for _, ref := range refs {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if pod.UID != ref.UID || podFinished(pod) {
			continue
		}
		msg := o.message
		if cmd := pod.Annotations[LogsAnnotation]; cmd != "" {
			msg += "; logs: " + cmd
		}
		if !o.succeeded && pod.Labels[podconstants.GroupNameLabel] != "" &&
			pod.Annotations[podconstants.RetriableInGroupAnnotationKey] != podconstants.RetriableInGroupAnnotationValue {
			annotated := pod.DeepCopy()
			metav1.SetMetaDataAnnotation(&annotated.ObjectMeta, podconstants.RetriableInGroupAnnotationKey, podconstants.RetriableInGroupAnnotationValue)
			if err := c.Patch(ctx, annotated, client.MergeFrom(pod)); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return err
			}
			pod = annotated
		}
		done := pod.DeepCopy()
		done.Status.Phase, done.Status.Reason, done.Status.Message = phase, o.reason, msg
		for _, ty := range []corev1.PodConditionType{corev1.ContainersReady, corev1.PodReady} {
			done.Status.Conditions = setPodCondition(done.Status.Conditions, ty, corev1.ConditionFalse, ReasonOffload, "", now)
		}
		done.Status.InitContainerStatuses = terminatedStatuses(pod.Spec.InitContainers, o, now, true)
		done.Status.ContainerStatuses = terminatedStatuses(pod.Spec.Containers, o, now, false)
		if err := c.Status().Patch(ctx, done, client.MergeFrom(pod)); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

// kubeletConditions are the pod conditions a kubelet sets on a pod it runs.
var kubeletConditions = []corev1.PodConditionType{corev1.PodInitialized, corev1.ContainersReady, corev1.PodReady}

// LogsAnnotation on an owner pod is the command that prints its job's logs:
// kubectl logs cannot, since the pod never runs on a node, and the shadow's
// job-logs container streams them.
const LogsAnnotation = "offload.interlink.eu/logs"

// logsCommand is the command that prints the logs of q's job, empty when the
// job has no shadow. --tail=-1: with a selector kubectl prints the last 10
// lines only.
func logsCommand(q *v1alpha1.OffloadRequest) string {
	if q.Status.Shadow == nil || q.Status.Shadow.Deployment == "" {
		return ""
	}
	return fmt.Sprintf("kubectl logs -n %s -l %s=%s -c %s --tail=-1", q.Namespace, shadow.LabelShadowOf, q.Spec.Workload.UID, shadow.JobLogsContainer)
}

// syncSinglePod shows the job of a Workload of one plain pod on that pod, as a
// kubelet reports a pod it runs and as MultiKueue copies a worker's pod status
// onto the manager's gated pod. While q is Admitted: phase Running, init
// containers terminated and containers running since the job's start, Ready and
// ContainersReady while the shadow is ready, the job is not staging out and
// every container with a readiness probe is ready at the site, the shadow pod's
// IP, and the command that prints the job's logs, in the status message and in
// LogsAnnotation. With no running job (q nil or not Admitted) a pod shown
// Running goes back to Pending, a gated pod's status. The pod stays gated, so
// Kueue's pod controller keeps counting it suspended; terminal pods are
// finishPods'.
func syncSinglePod(ctx context.Context, c client.Client, r client.Reader, namespace string, o *workloadOwner, q *v1alpha1.OffloadRequest, now time.Time) error {
	ref := o.singlePod()
	if ref == nil {
		return nil
	}
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, pod); err != nil {
		return client.IgnoreNotFound(err)
	}
	if pod.UID != ref.UID || podFinished(pod) {
		return nil
	}
	running := q != nil && meta.IsStatusConditionTrue(q.Status.Conditions, v1alpha1.OffloadRequestAdmitted)
	if running {
		if cmd := logsCommand(q); cmd != "" && pod.Annotations[LogsAnnotation] != cmd {
			annotated := pod.DeepCopy()
			metav1.SetMetaDataAnnotation(&annotated.ObjectMeta, LogsAnnotation, cmd)
			if err := c.Patch(ctx, annotated, client.MergeFrom(pod)); err != nil {
				return client.IgnoreNotFound(err)
			}
			pod = annotated
		}
	}
	done := pod.DeepCopy()
	st := &done.Status
	switch {
	case running:
		runningStatus(done, q, now)
	case st.Phase == corev1.PodRunning:
		st.Phase = corev1.PodPending
		st.Conditions = slices.DeleteFunc(st.Conditions, func(c corev1.PodCondition) bool { return slices.Contains(kubeletConditions, c.Type) })
		st.PodIP, st.PodIPs, st.StartTime, st.Message = "", nil, nil, ""
		st.InitContainerStatuses, st.ContainerStatuses = nil, nil
	}
	if equality.Semantic.DeepEqual(pod.Status, done.Status) {
		return nil
	}
	return client.IgnoreNotFound(c.Status().Patch(ctx, done, client.MergeFrom(pod)))
}

// runningStatus sets pod's status to that of a pod running q's job. A
// container with a readiness probe is ready once the head rank's container of
// that name is: the site runs the probe in the job, for example a notebook's
// test of the socket its server listens on.
func runningStatus(pod *corev1.Pod, q *v1alpha1.OffloadRequest, now time.Time) {
	st, sh := &pod.Status, q.Status.Shadow
	// jobNotReady holds every container back, a probe at the site only its own.
	jobNotReady := ""
	switch {
	case meta.IsStatusConditionTrue(q.Status.Conditions, v1alpha1.OffloadRequestStagingOut):
		jobNotReady = jobOf(q) + " ended, staging out"
	case sh == nil || !sh.Ready:
		jobNotReady = "the shadow of " + jobOf(q) + " is not ready"
	}
	probed := siteReadiness(pod, q)
	ctrReady := func(name string) bool {
		r, ok := probed[name]
		return jobNotReady == "" && (!ok || r)
	}
	notReady := jobNotReady
	for _, ctr := range pod.Spec.Containers {
		if notReady == "" && !ctrReady(ctr.Name) {
			notReady = "container " + ctr.Name + " of " + jobOf(q) + " is not ready"
		}
	}
	ready, readyStatus, reason := notReady == "", corev1.ConditionTrue, ""
	if !ready {
		readyStatus, reason = corev1.ConditionFalse, ReasonOffload
	}
	start := q.Status.StartTime
	if start == nil {
		start = st.StartTime
	}
	if start == nil {
		start = &metav1.Time{Time: now.Truncate(time.Second)}
	}

	st.Phase, st.StartTime = corev1.PodRunning, start.DeepCopy()
	st.Message = ""
	if cmd := logsCommand(q); cmd != "" {
		st.Message = "logs: " + cmd
	}
	st.PodIP, st.PodIPs = "", nil
	if sh != nil && net.ParseIP(sh.PodIP) != nil {
		st.PodIP, st.PodIPs = sh.PodIP, []corev1.PodIP{{IP: sh.PodIP}}
	}
	st.Conditions = setPodCondition(st.Conditions, corev1.PodInitialized, corev1.ConditionTrue, "", "", now)
	for _, ty := range []corev1.PodConditionType{corev1.ContainersReady, corev1.PodReady} {
		st.Conditions = setPodCondition(st.Conditions, ty, readyStatus, reason, notReady, now)
	}
	st.InitContainerStatuses = terminatedStatuses(pod.Spec.InitContainers, jobOutcome{succeeded: true, startTime: start, completionTime: start}, now, true)
	st.ContainerStatuses = make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	for _, ctr := range pod.Spec.Containers {
		st.ContainerStatuses = append(st.ContainerStatuses, corev1.ContainerStatus{
			Name: ctr.Name, Image: ctr.Image, Ready: ctrReady(ctr.Name), Started: ptr.To(true),
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: *start}},
		})
	}
}

// siteReadiness is, for each of pod's containers with a readiness probe,
// whether the head rank's container of that name is ready at the site.
func siteReadiness(pod *corev1.Pod, q *v1alpha1.OffloadRequest) map[string]bool {
	var out map[string]bool
	for _, ctr := range pod.Spec.Containers {
		if ctr.ReadinessProbe == nil {
			continue
		}
		if out == nil {
			out = map[string]bool{}
		}
		out[ctr.Name] = false
		if len(q.Status.Ranks) == 0 {
			continue
		}
		for _, cs := range q.Status.Ranks[0].Containers {
			if cs.Name == ctr.Name {
				out[ctr.Name] = cs.Ready
			}
		}
	}
	return out
}

// setPodCondition sets the condition of type ty, moving its lastTransitionTime
// only when its status changes.
func setPodCondition(conds []corev1.PodCondition, ty corev1.PodConditionType, status corev1.ConditionStatus, reason, msg string, now time.Time) []corev1.PodCondition {
	c := corev1.PodCondition{Type: ty, Status: status, Reason: reason, Message: msg, LastTransitionTime: metav1.NewTime(now.Truncate(time.Second))}
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

// terminatedStatuses gives each container a terminated state. Init containers
// exit 0; the others take the executor's exit code for their name, or 0 for a
// completed job and 1 for a failed one.
func terminatedStatuses(containers []corev1.Container, o jobOutcome, now time.Time, init bool) []corev1.ContainerStatus {
	if len(containers) == 0 {
		return nil
	}
	finished := metav1.NewTime(now)
	if o.completionTime != nil {
		finished = *o.completionTime
	}
	started := finished
	if o.startTime != nil {
		started = *o.startTime
	}
	out := make([]corev1.ContainerStatus, 0, len(containers))
	for _, ctr := range containers {
		code, ok := o.exitCodes[ctr.Name]
		switch {
		case init:
			code = 0
		case !ok && o.succeeded:
			code = 0
		case !ok:
			code = 1
		}
		reason := "Completed"
		if code != 0 {
			reason = "Error"
		}
		out = append(out, corev1.ContainerStatus{
			Name: ctr.Name, Image: ctr.Image,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: code, Reason: reason, StartedAt: started, FinishedAt: finished,
			}},
		})
	}
	return out
}

// exitCodes reads the head rank's container exit codes from the executor.
func exitCodes(st *v1alpha1.OffloadRequestStatus) map[string]int32 {
	if len(st.Ranks) == 0 || len(st.Ranks[0].Containers) == 0 {
		return nil
	}
	out := map[string]int32{}
	for _, cs := range st.Ranks[0].Containers {
		if t := cs.State.Terminated; t != nil {
			out[cs.Name] = t.ExitCode
		} else if t := cs.LastTerminationState.Terminated; t != nil {
			out[cs.Name] = t.ExitCode
		}
	}
	return out
}

func podFinished(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}
