package offload

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/executorapi"
)

const (
	testSite  = "hpc-a"
	testNS    = "alice"
	testDNS   = "kueue-offload"
	wlName    = "job-train-5d565"
	wlUID     = types.UID("0b6c-wl")
	jobUID    = types.UID("0b6c-job")
	testToken = "token-1"
)

// fakeExecutor is an httptest executor that records every call.
type fakeExecutor struct {
	t   *testing.T
	srv *httptest.Server

	mu         sync.Mutex
	submits    []executorapi.SubmitRequest
	submitHdrs []http.Header
	deletes    []string
	gets       int

	submitCode int
	submitBody any
	jobCode    int
	job        executorapi.JobStatus
}

func newFakeExecutor(t *testing.T) *fakeExecutor {
	a := &fakeExecutor{t: t, submitCode: http.StatusCreated, jobCode: http.StatusOK,
		submitBody: executorapi.SubmitResponse{APIVersion: executorapi.APIVersion, JobID: "123", Accepted: true},
		job:        executorapi.JobStatus{APIVersion: executorapi.APIVersion, JobID: "123", State: executorapi.StateQueued, Reason: "Priority"},
	}
	a.srv = httptest.NewServer(http.HandlerFunc(a.serve))
	t.Cleanup(a.srv.Close)
	return a
}

func (a *fakeExecutor) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	reply := func(code int, body any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(body)
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/submit":
		var req executorapi.SubmitRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			a.t.Errorf("decoding submit: %v", err)
		}
		a.submits = append(a.submits, req)
		a.submitHdrs = append(a.submitHdrs, r.Header.Clone())
		reply(a.submitCode, a.submitBody)
	case strings.HasPrefix(r.URL.Path, "/jobs/") && r.Method == http.MethodDelete:
		a.deletes = append(a.deletes, strings.TrimPrefix(r.URL.Path, "/jobs/"))
		w.WriteHeader(http.StatusNoContent)
	case strings.HasPrefix(r.URL.Path, "/jobs/") && r.Method == http.MethodGet:
		a.gets++
		if a.jobCode != http.StatusOK {
			reply(a.jobCode, executorapi.ErrorResponse{Permanent: a.jobCode == http.StatusNotFound, Message: "no job"})
			return
		}
		reply(http.StatusOK, a.job)
	default:
		a.t.Errorf("unexpected executor call %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

func (a *fakeExecutor) counts() (submits, deletes, gets int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.submits), len(a.deletes), a.gets
}

func (a *fakeExecutor) set(f func(a *fakeExecutor)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	f(a)
}

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
			found = true
		}
	}
	if !found {
		cur.Status.AdmissionChecks = append(cur.Status.AdmissionChecks, st)
	}
	return w.c.Status().Update(ctx, cur)
}

type env struct {
	t        *testing.T
	c        client.Client
	executor *fakeExecutor
	checks   *recordingChecks
	events   *events.FakeRecorder
	r        *WorkloadReconciler
	now      time.Time
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
			PollInterval:        &metav1.Duration{Duration: 15 * time.Second},
			ExecutorLostTimeout: &metav1.Duration{Duration: 10 * time.Minute},
			MaxRetries:          ptr.To[int32](2),
			Flavors: map[string]v1alpha1.FlavorTarget{
				"hpc-a-gpu":   {Partition: "gpu"},
				"hpc-a-cpu":   {Partition: "cpu"},
				"hpc-a-debug": {Partition: "gpu", QOS: "debug"},
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
		ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-proj42"},
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
		p.Labels[podGroupNameLabel] = podGroup
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

func newEnv(t *testing.T, opts ...envOption) *env {
	t.Helper()
	executor := newFakeExecutor(t)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNS,
		Annotations: map[string]string{ExecutorEndpointAnnotation: executor.srv.URL}}}
	objs := []client.Object{
		offloadConfig(), admissionCheck("hpc-a", ControllerName, testSite),
		admissionCheck("other-check", "example.org/other", "x"),
		clusterQueue(), ns, ownerJob(), workload(),
	}
	funcs := interceptor.Funcs{}
	for _, o := range opts {
		o(&objs, &funcs)
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).
		WithStatusSubresource(&kueue.Workload{}, &kueue.AdmissionCheck{}, &v1alpha1.OffloadRecord{}, &batchv1.Job{}, &corev1.Pod{},
			newUnstructured(jobSetKind), newUnstructured(trainJobKind)).
		WithInterceptorFuncs(funcs).Build()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, c: c, executor: executor, checks: &recordingChecks{c: c}, events: events.NewFakeRecorder(100),
		now: time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)}
	e.r = &WorkloadReconciler{
		Client: c, APIReader: c, Checks: e.checks,
		Executor:  &ExecutorClient{HTTP: &http.Client{Timeout: 5 * time.Second}, Token: &TokenSource{Path: tokenFile}},
		Recorder:  e.events,
		Site:      testSite,
		Namespace: testDNS,
		Now:       func() time.Time { return e.now },
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

func (e *env) record() *v1alpha1.OffloadRecord {
	e.t.Helper()
	rec := &v1alpha1.OffloadRecord{}
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: testDNS, Name: string(wlUID)}, rec); err != nil {
		e.t.Fatalf("record: %v", err)
	}
	return rec
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

func (e *env) mutateRecord(f func(rec *v1alpha1.OffloadRecord)) {
	e.t.Helper()
	rec := e.record()
	f(rec)
	if err := e.c.Status().Update(context.Background(), rec); err != nil {
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
