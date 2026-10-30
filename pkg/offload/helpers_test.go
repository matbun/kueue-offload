package offload

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"
	"sigs.k8s.io/kueue/pkg/util/admissioncheck"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// flagsKey is the Slurm plugin's annotation for extra sbatch flags.
const flagsKey = "slurm-job.vk.io/flags"

const (
	testSite = "hpc-a"
	testNS   = "alice"
	wlName   = "job-train-5d565"
	wlUID    = types.UID("0b6c-wl")
	jobUID   = types.UID("0b6c-job")
)

// recordingChecks stands in for the server-side apply writer, which the fake
// client cannot merge as a list keyed by name. It records each write and
// applies it to the stored Workload the way the API server would.
type recordingChecks struct {
	c      client.Client
	writes []kueue.AdmissionCheckState
}

func (w *recordingChecks) WriteCheck(ctx context.Context, wl *kueue.Workload, st kueue.AdmissionCheckState) error {
	w.writes = append(w.writes, st)
	cur := &kueue.Workload{}
	if err := w.c.Get(ctx, client.ObjectKeyFromObject(wl), cur); err != nil {
		return err
	}
	found := false
	for i := range cur.Status.AdmissionChecks {
		if cur.Status.AdmissionChecks[i].Name == st.Name {
			cur.Status.AdmissionChecks[i].State = st.State
			cur.Status.AdmissionChecks[i].Message = st.Message
			cur.Status.AdmissionChecks[i].LastTransitionTime = st.LastTransitionTime
			cur.Status.AdmissionChecks[i].RequeueAfterSeconds = st.RequeueAfterSeconds
			found = true
		}
	}
	if !found {
		cur.Status.AdmissionChecks = append(cur.Status.AdmissionChecks, st)
	}
	return w.c.Status().Update(ctx, cur)
}

type env struct {
	t      *testing.T
	c      client.Client
	checks *recordingChecks
	events *events.FakeRecorder
	r      *WorkloadReconciler
	now    time.Time
}

func testScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, kueue.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func offloadConfig() *v1alpha1.OffloadConfig {
	return &v1alpha1.OffloadConfig{
		ObjectMeta: metav1.ObjectMeta{Name: testSite},
		Spec: v1alpha1.OffloadConfigSpec{
			Workers:             []string{testSite},
			PollInterval:        &metav1.Duration{Duration: 15 * time.Second},
			ExecutorLostTimeout: &metav1.Duration{Duration: 10 * time.Minute},
			RetryStrategy: &kueue.ProvisioningRequestRetryStrategy{
				BackoffLimitCount: ptr.To[int32](2), BackoffBaseSeconds: ptr.To[int32](60), BackoffMaxSeconds: ptr.To[int32](1800),
			},
			Flavors: map[string]v1alpha1.FlavorTarget{
				"hpc-a-gpu":   {Annotations: map[string]string{flagsKey: "--account={{.Account}} --partition=gpu"}},
				"hpc-a-cpu":   {Annotations: map[string]string{flagsKey: "--account={{.Account}} --partition=cpu"}},
				"hpc-a-debug": {Annotations: map[string]string{flagsKey: "--account={{.Account}} --partition=gpu --qos=debug --time={{or .MaxExecMinutes 30}}"}},
			},
		},
	}
}

func admissionCheck(name, controller, site string) *kueue.AdmissionCheck {
	return &kueue.AdmissionCheck{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: kueue.AdmissionCheckSpec{
			ControllerName: controller,
			Parameters:     &kueue.AdmissionCheckParametersReference{APIGroup: "interlink.eu", Kind: "OffloadConfig", Name: site},
		},
	}
}

func clusterQueue() *kueue.ClusterQueue {
	return &kueue.ClusterQueue{
		ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-proj42", Labels: map[string]string{v1alpha1.AccountLabel: "proj42"}},
		Spec: kueue.ClusterQueueSpec{
			AdmissionChecksStrategy: &kueue.AdmissionChecksStrategy{
				AdmissionChecks: []kueue.AdmissionCheckStrategyRule{{Name: "other-check"}, {Name: "hpc-a"}},
			},
		},
	}
}

func ownerJob() *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: "train", Namespace: testNS, UID: jobUID,
			Annotations: map[string]string{
				"slurm-job.vk.io/flags": "--time=04:00:00 --gres=gpu:1",
				"interlink.eu/mode":     "owner",
				"example.org/unrelated": "x",
			},
		},
		Spec: batchv1.JobSpec{
			Parallelism:    ptr.To[int32](4),
			Completions:    ptr.To[int32](4),
			CompletionMode: ptr.To(batchv1.IndexedCompletion),
			Suspend:        ptr.To(true),
		},
	}
}

func podTemplate() corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"interlink.eu/mode": "template"}},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:  "train",
				Image: "ghcr.io/example/train:1.4",
				Env: []corev1.EnvVar{{Name: "HF_TOKEN", ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "hf-token"}, Key: "token"},
				}}},
				EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "train-env"}}}},
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("8"), "nvidia.com/gpu": resource.MustParse("1"),
				}},
			}},
			Volumes: []corev1.Volume{
				{Name: "cfg", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "train-config"}}}},
				{Name: "mix", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
					{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "s3-creds"}}},
					{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "hf-token"}}},
				}}}},
			},
		},
	}
}

func workload() *kueue.Workload {
	return &kueue.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name: wlName, Namespace: testNS, UID: wlUID,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1", Kind: "Job", Name: "train", UID: jobUID, Controller: ptr.To(true),
			}},
		},
		Spec: kueue.WorkloadSpec{
			PodSets: []kueue.PodSet{{Name: "main", Count: 4, Template: podTemplate()}},
		},
		Status: kueue.WorkloadStatus{
			Conditions: []metav1.Condition{{
				Type: kueue.WorkloadQuotaReserved, Status: metav1.ConditionTrue, Reason: "QuotaReserved",
				LastTransitionTime: metav1.Now(),
			}},
			Admission: &kueue.Admission{
				ClusterQueue: "hpc-a-proj42",
				PodSetAssignments: []kueue.PodSetAssignment{{
					Name:    "main",
					Flavors: map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-gpu", "nvidia.com/gpu": "hpc-a-gpu"},
					Count:   ptr.To[int32](4),
				}},
			},
			AdmissionChecks: []kueue.AdmissionCheckState{
				{Name: "other-check", State: kueue.CheckStatePending, Message: "other", LastTransitionTime: metav1.Now()},
				{Name: "hpc-a", State: kueue.CheckStatePending, LastTransitionTime: metav1.Now()},
			},
		},
	}
}

type envOption func(objs *[]client.Object, funcs *interceptor.Funcs)

const (
	podGroup  = "train-group"
	podSetKey = "a1b2c3d4"
)

// ownerPod is a pod Kueue's pod integration holds gated.
func ownerPod(i int, group bool) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("train-%d", i), Namespace: testNS, UID: types.UID(fmt.Sprintf("0b6c-pod-%d", i)),
			Labels: map[string]string{"kueue.x-k8s.io/queue-name": "hpc-a"},
			Annotations: map[string]string{
				"slurm-job.vk.io/flags": "--time=01:00:00",
				"interlink.eu/mode":     "owner",
				"example.org/unrelated": "x",
			},
		},
		Spec:   podTemplate().Spec,
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	p.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}}
	if group {
		p.Labels[podconstants.GroupNameLabel] = podGroup
	}
	return p
}

// podOwned makes the Workload owned by n pods instead of the Job, as Kueue's
// pod integration builds it: a single pod is the controller, the members of a
// group are owners without a controller, and the group is one PodSet named by
// a hash of the pod shape.
func podOwned(n int) envOption {
	return func(objs *[]client.Object, _ *interceptor.Funcs) {
		var refs []metav1.OwnerReference
		for i := 0; i < n; i++ {
			p := ownerPod(i, n > 1)
			*objs = append(*objs, p)
			ref := metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: p.Name, UID: p.UID}
			if n == 1 {
				ref.Controller = ptr.To(true)
			}
			refs = append(refs, ref)
		}
		for _, o := range *objs {
			if wl, ok := o.(*kueue.Workload); ok {
				wl.OwnerReferences = refs
				wl.Spec.PodSets = []kueue.PodSet{{Name: podSetKey, Count: int32(n), Template: podTemplate()}}
				wl.Status.Admission.PodSetAssignments[0].Name = podSetKey
				wl.Status.Admission.PodSetAssignments[0].Count = ptr.To(int32(n))
			}
		}
	}
}

func newUnstructured(k ownerKind) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(k.gvk)
	return u
}

const customUID = types.UID("0b6c-custom")

// customOwned makes the Workload owned by a JobSet or a TrainJob named train,
// stored unstructured as the offload controller reads it.
func customOwned(k ownerKind) envOption {
	return func(objs *[]client.Object, _ *interceptor.Funcs) {
		u := newUnstructured(k)
		u.SetName("train")
		u.SetNamespace(testNS)
		u.SetUID(customUID)
		u.SetAnnotations(map[string]string{
			"slurm-job.vk.io/flags": "--time=02:00:00",
			"interlink.eu/mode":     "owner",
			"example.org/unrelated": "x",
		})
		*objs = append(*objs, u)
		for _, o := range *objs {
			if wl, ok := o.(*kueue.Workload); ok {
				wl.OwnerReferences = []metav1.OwnerReference{{APIVersion: k.gvk.GroupVersion().String(), Kind: k.gvk.Kind,
					Name: "train", UID: customUID, Controller: ptr.To(true)}}
			}
		}
	}
}

func (e *env) custom(k ownerKind) *unstructured.Unstructured {
	e.t.Helper()
	u := newUnstructured(k)
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "train"}, u); err != nil {
		e.t.Fatal(err)
	}
	return u
}

// testClient builds the fake client with the indexes and status subresources
// the reconcilers use.
func testClient(t *testing.T, objs []client.Object, funcs interceptor.Funcs) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).
		WithStatusSubresource(&kueue.Workload{}, &kueue.AdmissionCheck{}, &v1alpha1.OffloadRequest{}, &v1alpha1.OffloadWorker{},
			&batchv1.Job{}, &corev1.Pod{}, newUnstructured(jobSetKind), newUnstructured(trainJobKind)).
		WithIndex(&v1alpha1.OffloadRequest{}, RequestWorkloadKey, indexRequestWorkload).
		WithIndex(&kueue.AdmissionCheck{}, AdmissionCheckUsingConfigKey,
			admissioncheck.IndexerByConfigFunction(ControllerName, v1alpha1.GroupVersion.WithKind("OffloadConfig"))).
		WithInterceptorFuncs(funcs).Build()
}

func newEnv(t *testing.T, opts ...envOption) *env {
	t.Helper()
	objs := []client.Object{
		offloadConfig(), admissionCheck("hpc-a", ControllerName, testSite),
		admissionCheck("other-check", "example.org/other", "x"),
		clusterQueue(), ownerJob(), workload(),
	}
	e := &env{t: t, events: events.NewFakeRecorder(100), now: time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)}
	// The fake client sets no creationTimestamp; the API server would.
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if q, ok := obj.(*v1alpha1.OffloadRequest); ok {
				q.CreationTimestamp = metav1.NewTime(e.now)
			}
			return c.Create(ctx, obj, opts...)
		},
	}
	for _, o := range opts {
		o(&objs, &funcs)
	}
	e.c = testClient(t, objs, funcs)
	e.checks = &recordingChecks{c: e.c}
	configs, err := NewConfigHelper(e.c)
	if err != nil {
		t.Fatal(err)
	}
	e.r = &WorkloadReconciler{
		Client: e.c, APIReader: e.c, Checks: e.checks, Configs: configs,
		Recorder: e.events,
		Now:      func() time.Time { return e.now },
	}
	return e
}

func (e *env) reconcile() ctrl.Result {
	e.t.Helper()
	res, err := e.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: wlName}})
	if err != nil {
		e.t.Fatalf("reconcile: %v", err)
	}
	return res
}

func (e *env) workload() *kueue.Workload {
	e.t.Helper()
	wl := &kueue.Workload{}
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: wlName}, wl); err != nil {
		e.t.Fatal(err)
	}
	return wl
}

func (e *env) check(name kueue.AdmissionCheckReference) kueue.AdmissionCheckState {
	e.t.Helper()
	st := findCheck(e.workload(), name)
	if st == nil {
		e.t.Fatalf("check %s missing", name)
	}
	return *st
}

func (e *env) job() *batchv1.Job {
	e.t.Helper()
	j := &batchv1.Job{}
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "train"}, j); err != nil {
		e.t.Fatal(err)
	}
	return j
}

func (e *env) pod(name string) *corev1.Pod {
	e.t.Helper()
	p := &corev1.Pod{}
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, p); err != nil {
		e.t.Fatal(err)
	}
	return p
}

func findCheck(wl *kueue.Workload, name kueue.AdmissionCheckReference) *kueue.AdmissionCheckState {
	return admissioncheck.FindAdmissionCheck(wl.Status.AdmissionChecks, name)
}

// requestOf is the name of the check hpc-a's request for an attempt.
func requestOf(attempt int32) string { return requestName(wlName, "hpc-a", attempt) }

// request reads an attempt's OffloadRequest of check hpc-a.
func (e *env) request(attempt int32) *v1alpha1.OffloadRequest {
	e.t.Helper()
	q := &v1alpha1.OffloadRequest{}
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: requestOf(attempt)}, q); err != nil {
		e.t.Fatalf("request %d: %v", attempt, err)
	}
	return q
}

// requests lists every OffloadRequest of the namespace by name.
func (e *env) requests() map[string]*v1alpha1.OffloadRequest {
	e.t.Helper()
	list := &v1alpha1.OffloadRequestList{}
	if err := e.c.List(context.Background(), list, client.InNamespace(testNS)); err != nil {
		e.t.Fatal(err)
	}
	out := map[string]*v1alpha1.OffloadRequest{}
	for i := range list.Items {
		out[list.Items[i].Name] = &list.Items[i]
	}
	return out
}

// takes is the executor taking an attempt: its finalizer, then Accepted and a
// heartbeat.
func (e *env) takes(attempt int32) {
	e.t.Helper()
	q := e.request(attempt)
	q.Finalizers = append(q.Finalizers, v1alpha1.ExecutorFinalizer)
	if err := e.c.Update(context.Background(), q); err != nil {
		e.t.Fatal(err)
	}
	e.report(attempt, func(st *v1alpha1.OffloadRequestStatus) {
		e.condition(st, v1alpha1.OffloadRequestAccepted, metav1.ConditionTrue, "Accepted", "")
	})
}

// report is the executor writing an attempt's status, with a heartbeat now.
func (e *env) report(attempt int32, f func(st *v1alpha1.OffloadRequestStatus)) {
	e.t.Helper()
	q := e.request(attempt)
	f(&q.Status)
	q.Status.LastSyncTime = &metav1.Time{Time: e.now}
	if err := e.c.Status().Update(context.Background(), q); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) condition(st *v1alpha1.OffloadRequestStatus, ty string, status metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{Type: ty, Status: status, Reason: reason, Message: msg,
		LastTransitionTime: metav1.NewTime(e.now)})
}

// queued, running, finished and evicted are the executor's reports for a job.
func (e *env) queued(job, reason, msg string) func(st *v1alpha1.OffloadRequestStatus) {
	return func(st *v1alpha1.OffloadRequestStatus) {
		st.JobID, st.Reason, st.Message = job, reason, msg
		e.condition(st, v1alpha1.OffloadRequestQuotaReserved, metav1.ConditionTrue, "Submitted", "")
	}
}

func (e *env) running(node string, start *metav1.Time) func(st *v1alpha1.OffloadRequestStatus) {
	return func(st *v1alpha1.OffloadRequestStatus) {
		st.NodeName, st.StartTime, st.Reason, st.Message = node, start, "", ""
		e.condition(st, v1alpha1.OffloadRequestAdmitted, metav1.ConditionTrue, "Running", "")
	}
}

func (e *env) finished(succeeded bool, msg string) func(st *v1alpha1.OffloadRequestStatus) {
	return func(st *v1alpha1.OffloadRequestStatus) {
		reason := v1alpha1.OffloadRequestSucceeded
		if !succeeded {
			reason = v1alpha1.OffloadRequestFailed
		}
		e.condition(st, v1alpha1.OffloadRequestFinished, metav1.ConditionTrue, reason, msg)
	}
}

func (e *env) evicted(msg string) func(st *v1alpha1.OffloadRequestStatus) {
	return func(st *v1alpha1.OffloadRequestStatus) {
		e.condition(st, v1alpha1.OffloadRequestEvicted, metav1.ConditionTrue, "Cancelled", msg)
	}
}

// submitted reconciles the first attempt into existence and has the executor
// take it and submit job 123, queued for Priority.
func (e *env) submitted() {
	e.t.Helper()
	e.reconcile()
	e.takes(1)
	e.report(1, e.queued("123", "Priority", ""))
	e.reconcile()
}

// evictAndReserve does what Kueue does after a Retry: it resets the check to
// Pending, counting the retry in retryCount and keeping requeueAfterSeconds
// (resetChecksOnEviction in pkg/workload/admissionchecks.go), then reserves
// quota again.
func (e *env) evictAndReserve(name kueue.AdmissionCheckReference) {
	e.t.Helper()
	e.mutateWorkload(func(wl *kueue.Workload) {
		st := findCheck(wl, name)
		if st.State == kueue.CheckStateRetry {
			st.RetryCount = ptr.To(ptr.Deref(st.RetryCount, 0) + 1)
		}
		st.State, st.Message = kueue.CheckStatePending, "Reset to Pending after eviction. Previously: Retry"
	})
}

// evictPerFlavor does what Kueue does after a Retry of a check that covers
// only some flavors: eviction clears the admission and with it the check's
// state, retryCount included, and the next reservation adds it again as new.
func (e *env) evictPerFlavor(name kueue.AdmissionCheckReference) {
	e.t.Helper()
	e.mutateWorkload(func(wl *kueue.Workload) {
		st := findCheck(wl, name)
		*st = kueue.AdmissionCheckState{Name: name, State: kueue.CheckStatePending, LastTransitionTime: metav1.NewTime(e.now)}
	})
}

// loseReservation does what Kueue does when it evicts the Workload.
func (e *env) loseReservation() {
	e.t.Helper()
	e.mutateWorkload(func(wl *kueue.Workload) {
		meta.SetStatusCondition(&wl.Status.Conditions, metav1.Condition{Type: kueue.WorkloadQuotaReserved, Status: metav1.ConditionFalse, Reason: "Pending"})
		meta.SetStatusCondition(&wl.Status.Conditions, metav1.Condition{Type: kueue.WorkloadEvicted, Status: metav1.ConditionTrue, Reason: "Preempted"})
		wl.Status.Admission = nil
	})
}

// reserveAgain does what Kueue does when it reserves quota again.
func (e *env) reserveAgain() {
	e.t.Helper()
	admission := workload().Status.Admission
	e.mutateWorkload(func(wl *kueue.Workload) {
		meta.SetStatusCondition(&wl.Status.Conditions, metav1.Condition{Type: kueue.WorkloadQuotaReserved, Status: metav1.ConditionTrue, Reason: "QuotaReserved"})
		meta.RemoveStatusCondition(&wl.Status.Conditions, kueue.WorkloadEvicted)
		wl.Status.Admission = admission
	})
}

// mutateWorkload changes the stored Workload, as Kueue would.
func (e *env) mutateWorkload(f func(wl *kueue.Workload)) {
	e.t.Helper()
	wl := e.workload()
	f(wl)
	if err := e.c.Status().Update(context.Background(), wl); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) drainEvents() []string {
	var out []string
	for {
		select {
		case ev := <-e.events.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func hasEvent(evs []string, prefix string) bool {
	for _, ev := range evs {
		if strings.HasPrefix(ev, prefix) {
			return true
		}
	}
	return false
}

func jobCondition(j *batchv1.Job, ty batchv1.JobConditionType) *batchv1.JobCondition {
	for i := range j.Status.Conditions {
		if j.Status.Conditions[i].Type == ty {
			return &j.Status.Conditions[i]
		}
	}
	return nil
}
