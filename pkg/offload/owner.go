package offload

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/pkg/executorapi"
)

// Reasons written on the owner's conditions and on the owner pods.
const (
	ReasonOffload  = "KueueOffload"
	ReasonRejected = "Rejected"
)

// podGroupNameLabel names the pod group of Kueue's pod integration.
const podGroupNameLabel = "kueue.x-k8s.io/pod-group-name"

// workloadOwner is what a Workload finishes through: its controller, a
// batch/v1 Job (design D1), a JobSet or a TrainJob, or the pods of Kueue's
// pod integration, one pod or a pod group (D2).
type workloadOwner struct {
	ref  *metav1.OwnerReference
	pods []metav1.OwnerReference
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
	jobKind      = ownerKind{gvk: batchv1.SchemeGroupVersion.WithKind("Job")}
	jobSetKind   = ownerKind{gvk: schema.GroupVersionKind{Group: "jobset.x-k8s.io", Version: "v1alpha2", Kind: "JobSet"}, complete: "Completed", failed: "Failed", terminalState: true}
	trainJobKind = ownerKind{gvk: schema.GroupVersionKind{Group: "trainer.kubeflow.org", Version: "v1alpha1", Kind: "TrainJob"}, complete: "Complete", failed: "Failed"}
	ownerKinds   = []ownerKind{jobKind, jobSetKind, trainJobKind}
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
	c := metav1.GetControllerOf(wl)
	if c != nil {
		if _, ok := kindOf(c); ok {
			return &workloadOwner{ref: c}, ""
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
	return &workloadOwner{pods: pods}, ""
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
		"trainer.kubeflow.org/v1alpha1 TrainJob and v1 Pod workloads, this one has " + what
}

// readOwner returns the owner the executor is told about and the annotations
// forwarded from it (design D5): the controller, or the first owner pod that
// still exists, named after its pod group. A nil reference means the owner is
// gone.
func readOwner(ctx context.Context, r client.Reader, namespace string, o *workloadOwner) (*executorapi.OwnerReference, map[string]string, error) {
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
		ref := &executorapi.OwnerReference{APIVersion: o.ref.APIVersion, Kind: o.ref.Kind, Name: o.ref.Name, UID: string(o.ref.UID)}
		return ref, obj.GetAnnotations(), nil
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
		if g := pod.Labels[podGroupNameLabel]; g != "" {
			name = g
		}
		return &executorapi.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: name, UID: string(pod.UID)}, pod.Annotations, nil
	}
	return nil, nil, nil
}

// finishOwner writes the terminal state on the Workload's owner.
func finishOwner(ctx context.Context, c client.Client, r client.Reader, namespace string, o *workloadOwner, out jobOutcome, now time.Time) error {
	if o.ref == nil {
		return finishPods(ctx, c, r, namespace, o.pods, out, now)
	}
	key := types.NamespacedName{Namespace: namespace, Name: o.ref.Name}
	k, _ := kindOf(o.ref)
	if k == jobKind {
		return finishJob(ctx, c, r, key, o.ref.UID, out, now)
	}
	return finishConditions(ctx, c, r, k, key, o.ref.UID, out, now)
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

// finishJob patches the owner Job's status to Complete or Failed (design D1).
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
// Workload Finished (design D2). Owners already terminal or gone are skipped.
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

// finishPods sets every owner pod's phase to Succeeded or Failed (design D2),
// with a terminated status for each of its containers, so a workflow engine
// that reads the main container sees how the step ended. Kueue then removes
// its finalizers and marks the Workload Finished. The pods stay gated:
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
		done := pod.DeepCopy()
		done.Status.Phase, done.Status.Reason, done.Status.Message = phase, o.reason, o.message
		done.Status.InitContainerStatuses = terminatedStatuses(pod.Spec.InitContainers, o, now, true)
		done.Status.ContainerStatuses = terminatedStatuses(pod.Spec.Containers, o, now, false)
		if err := c.Status().Patch(ctx, done, client.MergeFrom(pod)); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
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
func exitCodes(st *executorapi.JobStatus) map[string]int32 {
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
