package executor

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

const (
	executorNS       = "alice"
	executorSite     = "hpc-a"
	executorAccount  = "proj42"
	executorWorkload = "0b6c1d2e-3f40-4a5b-8c6d-7e8f9a0b1c2d"
	executorJobID    = "4242"
	executorCtn      = "main"
	workloadName     = "job-train-5d565"
	checkName        = "hpc-a"
	testFlavor       = "hpc-a-gpu"
	busyboxImage     = "docker://busybox"
	// flavorFlags is what the offload controller expands the flavor of the
	// requests below to.
	flavorFlags = "--account=proj42 --partition=gpu --qos=debug"
	flagsKey    = "slurm-job.vk.io/flags"
)

// executorEnv is an executor in front of a real interLink API server, which
// passes its calls to a fake plugin. Its OffloadRequests and Workloads live in
// a fake client, the rest of its namespace in a fake clientset.
type executorEnv struct {
	t      *testing.T
	d      *Executor
	kube   *fake.Clientset
	c      client.WithWatch
	shadow *recordingShadow
	mux    *http.ServeMux
	plugin *fakePlugin
	// key is the request the test drives.
	key types.NamespacedName
}

type recordingShadow struct {
	mu      sync.Mutex
	ensured []ShadowSpec
	deleted []string
}

func (s *recordingShadow) Ensure(_ context.Context, spec ShadowSpec) (ShadowRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensured = append(s.ensured, spec)
	return ShadowRef{Deployment: "shadow-" + spec.WorkloadName, Service: "shadow-" + spec.WorkloadName}, nil
}

func (s *recordingShadow) Delete(_ context.Context, uid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, uid)
	return nil
}

func testConfig(interLinkURL string) Config {
	return Config{
		Listen:    defaultListen,
		InterLink: InterLinkConfig{URL: interLinkURL},
		Executor: ExecutorConfig{
			Namespace:    executorNS,
			pollInterval: defaultPollInterval,
			lostTimeout:  defaultLostTimeout,
		},
		Sites:  map[string]SiteConfig{executorSite: {AllowedAccounts: []string{executorAccount}}},
		Shadow: ShadowConfig{Enabled: true, LoginHost: "login.hpc-a.example.org"},
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, kueue.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func newExecutorEnv(t *testing.T, objects ...runtime.Object) *executorEnv {
	t.Helper()
	plugin := newFakePlugin(t)
	api := startAPIServer(t, plugin.srv.URL)
	// Every namespace has the cluster's CA, published by the API server.
	hasRootCA := false
	for _, o := range objects {
		if cm, ok := o.(*v1.ConfigMap); ok && cm.Name == "kube-root-ca.crt" {
			hasRootCA = true
		}
	}
	if !hasRootCA {
		objects = append(objects, &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: executorNS}, Data: map[string]string{"ca.crt": "-----BEGIN CERTIFICATE-----\n"}})
	}
	kube := fake.NewClientset(objects...)
	c := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).
		WithStatusSubresource(&kueue.Workload{}, &v1alpha1.OffloadRequest{}).Build()
	shadow := &recordingShadow{}
	d := New(testConfig(api), &InterLink{URL: api, HTTP: &http.Client{}}, kube, c, c, shadow)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux)
	return &executorEnv{t: t, d: d, kube: kube, c: c, shadow: shadow, mux: mux, plugin: plugin}
}

// restart is a new executor on the same cluster and API server.
func (e *executorEnv) restart() {
	e.t.Helper()
	e.d = New(e.d.Config, e.d.InterLink, e.kube, e.c, e.c, e.d.Shadow)
	e.mux = http.NewServeMux()
	e.d.RegisterRoutes(e.mux)
}

func (e *executorEnv) do(method, target string) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	return rec
}

// testRequest is attempt 1 of the offload controller's request for a Job's
// Workload of count pods.
func testRequest(count int32) *v1alpha1.OffloadRequest {
	return &v1alpha1.OffloadRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:       workloadName + "-" + checkName + "-1",
			Namespace:  executorNS,
			UID:        "1a2b3c4d-0000-4000-8000-000000000001",
			Labels:     map[string]string{v1alpha1.SiteLabel: executorSite},
			Finalizers: []string{v1alpha1.AttemptFinalizer},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: kueue.GroupVersion.String(), Kind: "Workload", Name: workloadName, UID: executorWorkload, Controller: ptr.To(true),
			}},
		},
		Spec: v1alpha1.OffloadRequestSpec{
			Workload:       v1alpha1.WorkloadReference{Name: workloadName, UID: executorWorkload},
			AdmissionCheck: checkName,
			Attempt:        1,
			Site:           executorSite,
			Account:        executorAccount,
			Owner:          v1alpha1.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: "train", UID: "owner-uid"},
			PodSets: []v1alpha1.PodSet{{
				Name:        "main",
				Flavors:     testFlavors(),
				Count:       count,
				Annotations: map[string]string{flagsKey: flavorFlags},
				Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
					Name:    executorCtn,
					Image:   busyboxImage,
					Command: []string{"sh", "-c", "echo hello"},
				}}}},
			}},
		},
	}
}

func testFlavors() map[v1.ResourceName]kueue.ResourceFlavorReference {
	return map[v1.ResourceName]kueue.ResourceFlavorReference{v1.ResourceCPU: testFlavor}
}

// attempt is q as the offload controller makes its attempt n.
func attempt(q *v1alpha1.OffloadRequest, n int32) *v1alpha1.OffloadRequest {
	q = q.DeepCopy()
	q.Name = workloadName + "-" + checkName + "-" + strconv.Itoa(int(n))
	q.UID = types.UID("1a2b3c4d-0000-4000-8000-00000000000" + strconv.Itoa(int(n)))
	q.Spec.Attempt = n
	return q
}

// workloadOf is the Workload the offload controller made q for: its quota
// reserved, with q's check Pending and q's PodSets admitted.
func workloadOf(q *v1alpha1.OffloadRequest) *kueue.Workload {
	wl := &kueue.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: q.Spec.Workload.Name, Namespace: q.Namespace, UID: q.Spec.Workload.UID},
		Status: kueue.WorkloadStatus{
			Conditions: []metav1.Condition{{
				Type: kueue.WorkloadQuotaReserved, Status: metav1.ConditionTrue, Reason: "QuotaReserved", LastTransitionTime: metav1.Now(),
			}},
			Admission:       &kueue.Admission{ClusterQueue: "hpc-a-proj42"},
			AdmissionChecks: []kueue.AdmissionCheckState{{Name: q.Spec.AdmissionCheck, State: kueue.CheckStatePending, LastTransitionTime: metav1.Now()}},
		},
	}
	for _, ps := range q.Spec.PodSets {
		wl.Spec.PodSets = append(wl.Spec.PodSets, kueue.PodSet{Name: ps.Name, Count: ps.Count, Template: *ps.Template.DeepCopy()})
		wl.Status.Admission.PodSetAssignments = append(wl.Status.Admission.PodSetAssignments, kueue.PodSetAssignment{
			Name: ps.Name, Flavors: ps.Flavors, Count: ptr.To(ps.Count),
		})
	}
	return wl
}

// add creates q with the Workload it was made for, or makes the Workload that,
// and makes q the request the test drives.
func (e *executorEnv) add(q *v1alpha1.OffloadRequest) {
	e.t.Helper()
	ctx := context.Background()
	wl, cur := workloadOf(q), &kueue.Workload{}
	if err := e.c.Get(ctx, client.ObjectKeyFromObject(wl), cur); err != nil {
		if err := e.c.Create(ctx, wl); err != nil {
			e.t.Fatal(err)
		}
	} else {
		cur.Spec = wl.Spec
		if err := e.c.Update(ctx, cur); err != nil {
			e.t.Fatal(err)
		}
		cur.Status = wl.Status
		if err := e.c.Status().Update(ctx, cur); err != nil {
			e.t.Fatal(err)
		}
	}
	e.addRequest(q)
}

// addRequest creates q alone and makes it the request the test drives.
func (e *executorEnv) addRequest(q *v1alpha1.OffloadRequest) {
	e.t.Helper()
	if err := e.c.Create(context.Background(), q.DeepCopy()); err != nil {
		e.t.Fatal(err)
	}
	e.key = client.ObjectKeyFromObject(q)
}

func (e *executorEnv) reconcileKey(key types.NamespacedName) (ctrl.Result, error) {
	return e.d.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
}

// reconcile reconciles the request the test drives, which must not fail.
func (e *executorEnv) reconcile() {
	e.t.Helper()
	if _, err := e.reconcileKey(e.key); err != nil {
		e.t.Fatalf("reconcile: %v", err)
	}
}

func (e *executorEnv) get(key types.NamespacedName) *v1alpha1.OffloadRequest {
	e.t.Helper()
	q := &v1alpha1.OffloadRequest{}
	if err := e.c.Get(context.Background(), key, q); err != nil {
		e.t.Fatal(err)
	}
	return q
}

// request is the request the test drives, as the cluster has it.
func (e *executorEnv) request() *v1alpha1.OffloadRequest {
	e.t.Helper()
	return e.get(e.key)
}

// take adds q and reconciles it once.
func (e *executorEnv) take(q *v1alpha1.OffloadRequest) *v1alpha1.OffloadRequest {
	e.t.Helper()
	e.add(q)
	e.reconcile()
	return e.request()
}

// submit adds q and reconciles it, which must submit its job.
func (e *executorEnv) submit(q *v1alpha1.OffloadRequest) *v1alpha1.OffloadRequest {
	e.t.Helper()
	got := e.take(q)
	if got.Status.JobID == "" || state(got) != "queued" || !meta.IsStatusConditionTrue(got.Status.Conditions, v1alpha1.OffloadRequestAccepted) {
		e.t.Fatalf("not submitted: %s %+v", state(got), got.Status)
	}
	return got
}

// poll reconciles the request the test drives again and returns it.
func (e *executorEnv) poll() *v1alpha1.OffloadRequest {
	e.t.Helper()
	e.reconcile()
	return e.request()
}

// remove deletes the request the test drives, as the offload controller does,
// and reconciles it.
func (e *executorEnv) remove() error {
	e.t.Helper()
	if err := e.c.Delete(context.Background(), e.request()); err != nil {
		e.t.Fatal(err)
	}
	_, err := e.reconcileKey(e.key)
	return err
}

// state names the request's state as the offload controller reads its
// conditions.
func state(q *v1alpha1.OffloadRequest) string {
	conds := q.Status.Conditions
	if c := meta.FindStatusCondition(conds, v1alpha1.OffloadRequestAccepted); c != nil && c.Status == metav1.ConditionFalse {
		return "refused"
	}
	if c := meta.FindStatusCondition(conds, v1alpha1.OffloadRequestFinished); c != nil && c.Status == metav1.ConditionTrue {
		return strings.ToLower(c.Reason)
	}
	for _, s := range []struct{ ty, name string }{
		{v1alpha1.OffloadRequestEvicted, "evicted"},
		{v1alpha1.OffloadRequestStagingOut, "staging out"},
		{v1alpha1.OffloadRequestAdmitted, "running"},
		{v1alpha1.OffloadRequestQuotaReserved, "queued"},
		{v1alpha1.OffloadRequestStagingIn, "staging in"},
	} {
		if meta.IsStatusConditionTrue(conds, s.ty) {
			return s.name
		}
	}
	return "waiting"
}

func condition(q *v1alpha1.OffloadRequest, ty string) metav1.Condition {
	if c := meta.FindStatusCondition(q.Status.Conditions, ty); c != nil {
		return *c
	}
	return metav1.Condition{}
}

// refusedWith checks that q was refused for good with a message holding want,
// and has no job.
func (e *executorEnv) refusedWith(q *v1alpha1.OffloadRequest, want string) {
	e.t.Helper()
	if c := condition(q, v1alpha1.OffloadRequestAccepted); state(q) != "refused" || !strings.Contains(c.Message, want) || q.Status.Message != c.Message || q.Status.JobID != "" {
		e.t.Fatalf("%s %+v, want refused with %q", state(q), q.Status, want)
	}
}

// waitingWith checks that q is not submitted yet and waits for want, with a
// heartbeat unless the plugin cannot be reached.
func waitingWith(t *testing.T, q *v1alpha1.OffloadRequest, want string, heartbeat bool) {
	t.Helper()
	if state(q) == "refused" || q.Status.JobID != "" || !strings.Contains(q.Status.Message, want) || (q.Status.LastSyncTime != nil) != heartbeat {
		t.Fatalf("%s %+v, want waiting with %q (heartbeat %v)", state(q), q.Status, want, heartbeat)
	}
}

// setStatus makes the plugin report member i with the containers' states.
func (e *executorEnv) setStatus(i int, node string, containers ...v1.ContainerStatus) {
	e.plugin.with(func(p *fakePlugin) {
		p.status[memberUID(executorWorkload, i)] = commonIL.PodStatus{JobID: executorJobID, NodeName: node, Containers: containers}
	})
}

func metaTime(t time.Time) metav1.Time { return metav1.NewTime(t.UTC().Truncate(time.Second)) }

func envOf(c v1.Container) map[string]string {
	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	return env
}

func ptrBool(b bool) *bool { return &b }

func TestSubmitSinglePod(t *testing.T) {
	e := newExecutorEnv(t)
	req := testRequest(1)
	req.Spec.PodSets[0].Template.Annotations = map[string]string{flagsKey: "--gres=gpu:1"}
	q := e.submit(req)
	if q.Status.JobID != executorJobID || len(q.Status.Ranks) != 1 || q.Status.Ranks[0].PodUID != memberUID(executorWorkload, 0) || q.Status.LastSyncTime == nil {
		t.Fatalf("status = %+v", q.Status)
	}
	if !slices.Contains(q.Finalizers, v1alpha1.ExecutorFinalizer) || !slices.Contains(q.Finalizers, v1alpha1.AttemptFinalizer) {
		t.Fatalf("finalizers %v", q.Finalizers)
	}
	pods := e.plugin.createdPods()
	if len(pods) != 1 {
		t.Fatalf("%d /create calls, want 1", len(pods))
	}
	pod := pods[0]
	if pod.Name != "train-0" || pod.Namespace != executorNS || string(pod.UID) != memberUID(executorWorkload, 0) {
		t.Errorf("member: %s/%s %s", pod.Namespace, pod.Name, pod.UID)
	}
	if got := pod.Annotations[flagsKey]; got != "--gres=gpu:1 "+flavorFlags {
		t.Errorf("flags = %q: the flavor's come after the template's own", got)
	}
	if _, ok := pod.Labels[PodGroupNameLabel]; ok {
		t.Errorf("a single pod is in a pod group: %v", pod.Labels)
	}
	if env := envOf(pod.Spec.Containers[0]); env[RankEnv] != "0" || env[WorldSizeEnv] != "1" {
		t.Errorf("env %v", env)
	}

	e.setStatus(0, "", waitingContainer(executorCtn, "Priority", "waits for a higher priority job"))
	q = e.poll()
	if state(q) != "queued" || q.Status.Reason != "Priority" || q.Status.Message != "waits for a higher priority job" || q.Status.JobID != executorJobID ||
		condition(q, v1alpha1.OffloadRequestAdmitted).Status != metav1.ConditionFalse {
		t.Fatalf("queued: %s %+v", state(q), q.Status)
	}
	if phase := e.plugin.phase(0); phase != v1.PodPending {
		t.Errorf("phase sent to /status = %q, want Pending", phase)
	}
}

// The owner's annotations reach every template; the template's own value wins.
func TestOwnerAnnotations(t *testing.T) {
	e := newExecutorEnv(t)
	req := testRequest(1)
	req.Spec.OwnerAnnotations = map[string]string{flagsKey: "--exclusive", "interlink.eu/launcher-args": "--nproc=4"}
	req.Spec.PodSets[0].Template.Annotations = map[string]string{"interlink.eu/launcher-args": "--nproc=8"}
	e.submit(req)
	pod := e.plugin.createdPods()[0]
	if pod.Annotations[flagsKey] != "--exclusive "+flavorFlags || pod.Annotations["interlink.eu/launcher-args"] != "--nproc=8" {
		t.Fatalf("annotations %v", pod.Annotations)
	}
	if got := e.request().Spec.PodSets[0].Template.Annotations; len(got) != 1 {
		t.Fatalf("the request's template was changed: %v", got)
	}
}

func TestSubmitGangIsOnePodGroup(t *testing.T) {
	e := newExecutorEnv(t)
	if q := e.submit(testRequest(3)); q.Status.JobID != executorJobID {
		t.Fatalf("jobID = %q", q.Status.JobID)
	}
	pods := e.plugin.createdPods()
	if len(pods) != 3 {
		t.Fatalf("%d /create calls, want 3", len(pods))
	}
	for i, pod := range pods {
		role := GangRoleWorker
		if i == 0 {
			role = GangRoleHead
		}
		if string(pod.UID) != memberUID(executorWorkload, i) || pod.Labels[JobCompletionIndexLabel] != strconv.Itoa(i) ||
			pod.Labels[PodGroupNameLabel] != executorWorkload || pod.Annotations[PodGroupTotalCountAnnotation] != "3" ||
			pod.Annotations[GangRoleAnnotation] != role || pod.Annotations[flagsKey] != flavorFlags {
			t.Errorf("member %d out of order or mislabelled: %s %v %v", i, pod.UID, pod.Labels, pod.Annotations)
		}
		if env := envOf(pod.Spec.Containers[0]); env[RankEnv] != strconv.Itoa(i) || env[WorldSizeEnv] != "3" {
			t.Errorf("member %d env %v", i, env)
		}
	}
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		uid := memberUID(executorWorkload, i)
		if seen[uid] || !workloadUIDPattern.MatchString(uid) {
			t.Fatalf("member %d UID %q is not a distinct UUID", i, uid)
		}
		seen[uid] = true
	}

	since := time.Now().Add(-time.Minute)
	for i, node := range []string{"node07", "node08", "node09"} {
		e.setStatus(i, node, runningContainer(executorCtn, since))
	}
	e.plugin.with(func(p *fakePlugin) {
		st := p.status[memberUID(executorWorkload, 0)]
		st.SocketDir = "/scratch/alice/sockets/0b6c"
		p.status[memberUID(executorWorkload, 0)] = st
	})
	q := e.poll()
	st := q.Status
	if state(q) != "running" || st.NodeName != "node07" || strings.Join(st.Nodes, ",") != "node07,node08,node09" ||
		st.SocketDir != "/scratch/alice/sockets/0b6c" || st.StartTime == nil || !st.StartTime.Equal(&metav1.Time{Time: metaTime(since).Time}) {
		t.Fatalf("running: %s %+v", state(q), st)
	}
	if len(st.Ranks) != 3 || st.Ranks[2].PodUID != memberUID(executorWorkload, 2) || st.Ranks[2].State != v1alpha1.RankRunning {
		t.Fatalf("ranks: %+v", st.Ranks)
	}
	if len(e.shadow.ensured) != 1 || e.shadow.ensured[0].NodeName != "node07" || e.shadow.ensured[0].SocketDir != "/scratch/alice/sockets/0b6c" {
		t.Fatalf("shadow: %+v", e.shadow.ensured)
	}
	if st.Shadow == nil || st.Shadow.Deployment != "shadow-train" {
		t.Fatalf("status shadow %+v", st.Shadow)
	}
	e.poll()
	if phase := e.plugin.phase(2); phase != v1.PodRunning {
		t.Errorf("phase sent to /status once running = %q", phase)
	}
}

// A submitted request is polled, never submitted again, also by a restarted
// executor, which follows the job from the request's status alone.
func TestSubmitOnceAndRecovered(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(testRequest(2))
	e.poll()
	if n := len(e.plugin.createdPods()); n != 2 {
		t.Fatalf("%d /create calls, want 2", n)
	}

	e.restart()
	end := time.Now()
	for i := 0; i < 2; i++ {
		e.setStatus(i, "node07", terminatedContainer(executorCtn, 0, "Completed", end.Add(-time.Hour), end))
	}
	q := e.poll()
	if state(q) != "succeeded" || q.Status.CompletionTime == nil || !q.Status.CompletionTime.Equal(&metav1.Time{Time: metaTime(end).Time}) {
		t.Fatalf("after restart: %s %+v", state(q), q.Status)
	}
	if n := len(e.plugin.createdPods()); n != 2 {
		t.Fatalf("%d /create calls after the restart, want 2", n)
	}
	// An ended job is not polled again.
	statuses := 0
	e.plugin.with(func(p *fakePlugin) { statuses = p.statuses })
	e.poll()
	e.plugin.with(func(p *fakePlugin) {
		if p.statuses != statuses {
			t.Errorf("an ended job's status was read again")
		}
	})
}

// The cache can lag behind the executor's own status write: a request it shows
// unsubmitted is read again from the API server before a submit.
func TestStaleCacheDoesNotSubmitAgain(t *testing.T) {
	e := newExecutorEnv(t)
	req := testRequest(1)
	stale := ctrlfake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&v1alpha1.OffloadRequest{}).
		WithObjects(workloadOf(req), req.DeepCopy()).Build()
	e.submit(req)
	e.d.Client = stale
	e.reconcile()
	if n := len(e.plugin.createdPods()); n != 1 {
		t.Fatalf("%d /create calls, want 1", n)
	}
}

// Kueue charts the requests, so they also size the job when no limit is set.
func TestRequestsSizeTheJob(t *testing.T) {
	e := newExecutorEnv(t)
	req := testRequest(1)
	req.Spec.PodSets[0].Template.Spec.Containers[0].Resources = v1.ResourceRequirements{
		Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("20"), v1.ResourceMemory: resource.MustParse("8Gi"), "nvidia.com/gpu": resource.MustParse("1")},
		Limits:   v1.ResourceList{"nvidia.com/gpu": resource.MustParse("1"), v1.ResourceMemory: resource.MustParse("16Gi")},
	}
	e.submit(req)
	limits := e.plugin.createdPods()[0].Spec.Containers[0].Resources.Limits
	if limits.Cpu().String() != "20" || limits.Memory().String() != "16Gi" || limits.Name("nvidia.com/gpu", resource.DecimalSI).String() != "1" {
		t.Fatalf("limits %v", limits)
	}
}

func TestSubmitRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1alpha1.OffloadRequest)
		want   string
	}{
		{"account not allowed", func(r *v1alpha1.OffloadRequest) { r.Spec.Account = "proj99" }, `account "proj99" is not allowed`},
		{"unknown site", func(r *v1alpha1.OffloadRequest) { r.Spec.Site = "hpc-b" }, `unknown site "hpc-b"`},
		{"workload UID not a UUID", func(r *v1alpha1.OffloadRequest) {
			r.Spec.Workload.UID, r.OwnerReferences[0].UID = "train-1", "train-1"
		}, "invalid workload UID"},
		{"no PodSets", func(r *v1alpha1.OffloadRequest) { r.Spec.PodSets = nil }, "no PodSets"},
		{"unlisted reference", func(r *v1alpha1.OffloadRequest) {
			r.Spec.PodSets[0].Template.Spec.Containers[0].EnvFrom = []v1.EnvFromSource{{SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: "hf-token"}}}}
		}, `Secret "hf-token", which is not in secretRefs`},
		{"missing reference", func(r *v1alpha1.OffloadRequest) {
			r.Spec.PodSets[0].ConfigMapRefs = []string{"train-config"}
			r.Spec.PodSets[0].Template.Spec.Volumes = []v1.Volume{{Name: "cfg", VolumeSource: v1.VolumeSource{ConfigMap: &v1.ConfigMapVolumeSource{LocalObjectReference: v1.LocalObjectReference{Name: "train-config"}}}}}
		}, `cannot read ConfigMap "train-config"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			req := testRequest(1)
			tt.mutate(req)
			q := e.take(req)
			e.refusedWith(q, tt.want)
			if !slices.Contains(q.Finalizers, v1alpha1.ExecutorFinalizer) {
				t.Errorf("a refused request is not held: %v", q.Finalizers)
			}
			// A refusal is final.
			if q := e.poll(); state(q) != "refused" || len(e.plugin.createdPods()) != 0 {
				t.Fatalf("after the refusal: %s", state(q))
			}
		})
	}
}

// A request of another namespace or site is not the executor's.
func TestOthersRequestsIgnored(t *testing.T) {
	e := newExecutorEnv(t)
	other := testRequest(1)
	other.Namespace = "bob"
	e.add(other)
	site := attempt(testRequest(1), 2)
	site.Labels[v1alpha1.SiteLabel] = "hpc-b"
	e.add(site)
	for _, key := range []types.NamespacedName{client.ObjectKeyFromObject(other), client.ObjectKeyFromObject(site)} {
		if _, err := e.reconcileKey(key); err != nil {
			t.Fatal(err)
		}
		if q := e.get(key); len(q.Finalizers) != 1 || len(q.Status.Conditions) != 0 {
			t.Fatalf("%s was taken: %v %+v", key, q.Finalizers, q.Status)
		}
	}
	if n := len(e.plugin.createdPods()); n != 0 {
		t.Fatalf("%d /create calls", n)
	}
}

// Before it submits, the executor checks the request against the Workload it
// names: a request the offload controller did not make for that Workload is
// refused, and one whose Workload no longer holds its reservation waits.
func TestWorkloadCheck(t *testing.T) {
	refusals := []struct {
		name     string
		request  func(*v1alpha1.OffloadRequest)
		workload func(*kueue.Workload)
		want     string
	}{
		{"no controller owner", func(q *v1alpha1.OffloadRequest) { q.OwnerReferences = nil }, nil, "is not controlled by Workload " + workloadName},
		{"owner of another UID", func(q *v1alpha1.OffloadRequest) { q.OwnerReferences[0].UID = "0b6c1d2e-3f40-4a5b-8c6d-000000000000" }, nil, "is not controlled by"},
		{"owner of another kind", func(q *v1alpha1.OffloadRequest) { q.OwnerReferences[0].Kind = "Job" }, nil, "is not controlled by"},
		{"no Workload", nil, func(wl *kueue.Workload) { wl.Name = "other" }, "Workload " + workloadName + " does not exist"},
		{"a recreated Workload", nil, func(wl *kueue.Workload) { wl.UID = "0b6c1d2e-3f40-4a5b-8c6d-000000000000" }, "has UID 0b6c1d2e-3f40-4a5b-8c6d-000000000000"},
		{"no such check", nil, func(wl *kueue.Workload) { wl.Status.AdmissionChecks[0].Name = "other" }, "has no admission check " + checkName},
		{"another template", func(q *v1alpha1.OffloadRequest) {
			q.Spec.PodSets[0].Template.Spec.Containers[0].Command = []string{"sh", "-c", "id"}
		}, nil,
			"PodSet main: the template is not the Workload's"},
		{"another count", nil, func(wl *kueue.Workload) { wl.Status.Admission.PodSetAssignments[0].Count = ptr.To[int32](2) }, "PodSet main: count 1, admitted 2"},
		{"another flavor", nil, func(wl *kueue.Workload) {
			wl.Status.Admission.PodSetAssignments[0].Flavors = map[v1.ResourceName]kueue.ResourceFlavorReference{v1.ResourceCPU: "hpc-a-cpu"}
		}, "PodSet main: flavors"},
		{"another PodSet", nil, func(wl *kueue.Workload) { wl.Spec.PodSets[0].Name = "worker" }, "PodSet 0 is main, the Workload's is worker"},
		{"more PodSets", nil, func(wl *kueue.Workload) { wl.Spec.PodSets = append(wl.Spec.PodSets, wl.Spec.PodSets[0]) }, "1 PodSets, the Workload has 2"},
	}
	for _, tt := range refusals {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			req := testRequest(1)
			wl := workloadOf(req)
			if tt.request != nil {
				tt.request(req)
			}
			if tt.workload != nil {
				tt.workload(wl)
			}
			if err := e.c.Create(context.Background(), wl); err != nil {
				t.Fatal(err)
			}
			e.addRequest(req)
			e.reconcile()
			e.refusedWith(e.request(), tt.want)
			if n := len(e.plugin.createdPods()); n != 0 {
				t.Fatalf("%d /create calls", n)
			}
		})
	}

	waits := []struct {
		name     string
		workload func(*kueue.Workload)
		want     string
	}{
		{"no reservation", func(wl *kueue.Workload) { wl.Status.Conditions[0].Status = metav1.ConditionFalse }, "holds no quota reservation"},
		{"evicted", func(wl *kueue.Workload) {
			wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{Type: kueue.WorkloadEvicted, Status: metav1.ConditionTrue, Reason: "Preempted", LastTransitionTime: metav1.Now()})
		}, "holds no quota reservation"},
		{"check in Retry", func(wl *kueue.Workload) { wl.Status.AdmissionChecks[0].State = kueue.CheckStateRetry }, "admission check hpc-a of Workload " + workloadName + " is Retry, not Pending"},
	}
	for _, tt := range waits {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			req := testRequest(1)
			good := workloadOf(req)
			wl := good.DeepCopy()
			tt.workload(wl)
			if err := e.c.Create(context.Background(), wl); err != nil {
				t.Fatal(err)
			}
			e.addRequest(req)
			e.reconcile()
			waitingWith(t, e.request(), tt.want, true)
			if n := len(e.plugin.createdPods()); n != 0 {
				t.Fatalf("%d /create calls", n)
			}
			wl = &kueue.Workload{}
			if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: executorNS, Name: workloadName}, wl); err != nil {
				t.Fatal(err)
			}
			wl.Status = good.Status
			if err := e.c.Status().Update(context.Background(), wl); err != nil {
				t.Fatal(err)
			}
			if q := e.poll(); q.Status.JobID != executorJobID {
				t.Fatalf("once the Workload holds its reservation: %s %+v", state(q), q.Status)
			}
		})
	}
}

// A new attempt waits until the job of the one it replaces is cancelled: both
// carry the Workload's UID in their members, shadow and stage Jobs.
func TestNextAttemptWaitsForTheCancel(t *testing.T) {
	e := newExecutorEnv(t)
	first := testRequest(1)
	e.submit(first)
	if err := e.c.Delete(context.Background(), e.request()); err != nil {
		t.Fatal(err)
	}
	second := attempt(first, 2)
	waitingWith(t, e.take(second), "waiting for the job of OffloadRequest "+first.Name+" to be cancelled", true)
	if n := len(e.plugin.createdPods()); n != 1 {
		t.Fatalf("%d /create calls, want only the first attempt's", n)
	}
	if _, err := e.reconcileKey(client.ObjectKeyFromObject(first)); err != nil {
		t.Fatal(err)
	}
	if got := e.plugin.deletedUIDs(); len(got) != 1 {
		t.Fatalf("/delete for %v", got)
	}
	if q := e.poll(); q.Status.JobID != executorJobID || len(e.plugin.createdPods()) != 2 {
		t.Fatalf("second attempt: %s %+v", state(q), q.Status)
	}
}

// The plugin's answer to /create reaches the request as backends.md 3.4 says: a
// 503 is waited for holding the quota, 400, 403 and 422 refuse, anything else
// goes without a heartbeat, which the offload controller gives up on after
// executorLostTimeout. The members created so far are deleted, and the next
// reconcile submits again.
func TestCreateRefusals(t *testing.T) {
	const text = `sbatch: error: Batch job submission failed: Invalid account <proj99> & "x"`
	tests := []struct {
		name        string
		codes       []int
		permanent   bool
		unreachable bool
		want        string
	}{
		{"submit limit", []int{http.StatusOK, http.StatusServiceUnavailable}, false, false, "the site takes no job now: " + text},
		{"refused", []int{http.StatusOK, http.StatusBadRequest}, true, false, text},
		{"forbidden", []int{http.StatusForbidden}, true, false, text},
		{"unprocessable", []int{http.StatusUnprocessableEntity}, true, false, text},
		{"plugin error", []int{http.StatusOK, http.StatusInternalServerError}, false, true, "answered 500: " + text},
		{"plugin timeout", []int{http.StatusGatewayTimeout}, false, true, "answered 504"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			e.plugin.with(func(p *fakePlugin) { p.createCodes, p.createText = tt.codes, text })
			q := e.take(testRequest(2))
			if tt.permanent {
				e.refusedWith(q, tt.want)
			} else {
				waitingWith(t, q, tt.want, !tt.unreachable)
			}
			var want []string
			for i := range tt.codes {
				want = append(want, memberUID(executorWorkload, i))
			}
			if got := e.plugin.deletedUIDs(); !slices.Equal(got, want) {
				t.Fatalf("deleted %v, want the members created so far %v", got, want)
			}
			if tt.permanent {
				return
			}
			if q := e.poll(); q.Status.JobID != executorJobID || q.Status.Message != "" {
				t.Fatalf("submit after the refusal: %s %+v", state(q), q.Status)
			}
		})
	}
}

// An API server or plugin that cannot be reached is not a refusal of the job.
func TestCreateWithoutAnswer(t *testing.T) {
	e := newExecutorEnv(t)
	e.plugin.srv.Close()
	waitingWith(t, e.take(testRequest(1)), "the interLink API server answered 500", false)
	e.d.InterLink.URL = "http://127.0.0.1:1"
	waitingWith(t, e.poll(), "cannot be reached", false)
}

// A buffering plugin that never names the job leaves no job behind.
func TestCreateWithoutJobID(t *testing.T) {
	e := newExecutorEnv(t)
	req := testRequest(1)
	req.Spec.PodSets = append(req.Spec.PodSets, v1alpha1.PodSet{Name: "worker", Flavors: testFlavors(), Count: 1, Annotations: req.Spec.PodSets[0].Annotations, Template: req.Spec.PodSets[0].Template})
	e.plugin.with(func(p *fakePlugin) { p.holdGroups = true })
	waitingWith(t, e.take(req), "no job ID", true)
	if n := len(e.plugin.deletedUIDs()); n != 2 {
		t.Fatalf("%d members deleted, want 2", n)
	}
}

// Job states from /status alone (backends.md 3.5), as the request's conditions.
func TestJobStates(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	end := time.Now().Add(-time.Minute)
	tests := []struct {
		name                  string
		ranks                 [][]v1.ContainerStatus
		wantState, wantReason string
		wantMessage           string
		wantExit              int32
	}{
		{"queued", [][]v1.ContainerStatus{
			{waitingContainer(executorCtn, "Resources", "")}, {waitingContainer(executorCtn, "Resources", "")},
		}, "queued", "Resources", "", 0},
		{"completed", [][]v1.ContainerStatus{
			{terminatedContainer(executorCtn, 0, "Completed", start, end)}, {terminatedContainer(executorCtn, 0, "Completed", start, end)},
		}, "succeeded", "Completed", "job 4242 completed", 0},
		{"failed by exit code", [][]v1.ContainerStatus{
			{terminatedContainer(executorCtn, 0, "", start, end)}, {terminatedContainer(executorCtn, 3, "", start, end)},
		}, "failed", "NonZeroExitCode", "rank 1 exited with code 3", 0},
		{"time limit", [][]v1.ContainerStatus{
			{terminatedContainer(executorCtn, 1, "SlurmJobTimeout", start, end)}, {terminatedContainer(executorCtn, 1, "SlurmJobTimeout", start, end)},
		}, "failed", "SlurmJobTimeout", "rank 0 exited with code 1 (SlurmJobTimeout)", 1},
		{"cancelled outside", [][]v1.ContainerStatus{
			{runningContainer(executorCtn, start)}, {terminatedContainer(executorCtn, 0, ReasonCancelled, start, end)},
		}, "evicted", ReasonCancelled, "rank 1 was cancelled outside the executor", 0},
		{"one rank still running", [][]v1.ContainerStatus{
			{runningContainer(executorCtn, start)}, {terminatedContainer(executorCtn, 2, "", start, end)},
		}, "running", "", "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			e.submit(testRequest(2))
			for i, containers := range tt.ranks {
				e.setStatus(i, "", containers...)
			}
			q := e.poll()
			st := q.Status
			if state(q) != tt.wantState || st.Reason != tt.wantReason || st.Message != tt.wantMessage || st.Ranks[0].ExitCode != tt.wantExit {
				t.Fatalf("got %s/%s exit %d %q, want %s/%s exit %d %q", state(q), st.Reason, st.Ranks[0].ExitCode, st.Message, tt.wantState, tt.wantReason, tt.wantExit, tt.wantMessage)
			}
			if jobEnded(q) {
				if st.CompletionTime == nil || st.StartTime == nil {
					t.Fatalf("an ended job without times: %+v", st)
				}
				ty := v1alpha1.OffloadRequestFinished
				if tt.wantState == "evicted" {
					ty = v1alpha1.OffloadRequestEvicted
				}
				if c := condition(q, ty); c.Message != tt.wantMessage {
					t.Errorf("%s message %q, want the job's", ty, c.Message)
				}
			}
			if tt.wantState == "succeeded" && !st.CompletionTime.Equal(&metav1.Time{Time: metaTime(end).Time}) {
				t.Errorf("completionTime %v, want the containers' end %v", st.CompletionTime, end)
			}
		})
	}
}

// A rank the plugin leaves out of its answer for longer than LostTimeout has
// vanished: the job is evicted. Until then the request's Evicted condition is
// False with reason RankMissing, since when it went missing.
func TestVanishedRank(t *testing.T) {
	e := newExecutorEnv(t)
	e.d.Config.Executor.lostTimeout = 100 * time.Millisecond
	e.submit(testRequest(2))
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	q := e.poll()
	if c := condition(q, v1alpha1.OffloadRequestEvicted); state(q) != "running" || c.Status != metav1.ConditionFalse || c.Reason != reasonRankMissing {
		t.Fatalf("missing for less than LostTimeout: %s %+v", state(q), q.Status.Conditions)
	}

	// A member back in the answer clears the clock.
	e.setStatus(1, "node08", runningContainer(executorCtn, time.Now()))
	if q := e.poll(); state(q) != "running" || meta.FindStatusCondition(q.Status.Conditions, v1alpha1.OffloadRequestEvicted) != nil {
		t.Fatalf("after the member came back: %s %+v", state(q), q.Status.Conditions)
	}

	// Missing for longer than LostTimeout: vanished.
	e = newExecutorEnv(t)
	e.d.Config.Executor.lostTimeout = 100 * time.Millisecond
	e.submit(testRequest(2))
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	e.poll()
	time.Sleep(1100 * time.Millisecond)
	q = e.poll()
	if c := condition(q, v1alpha1.OffloadRequestEvicted); state(q) != "evicted" || c.Reason != "Vanished" || !strings.Contains(c.Message, "rank 1 has been missing") {
		t.Fatalf("vanished: %s %+v", state(q), q.Status)
	}
}

// A plugin that does not answer /status gets no heartbeat written.
func TestStatusWithoutAnswerIsUnreachable(t *testing.T) {
	e := newExecutorEnv(t)
	beat := e.submit(testRequest(1)).Status.LastSyncTime
	e.plugin.srv.Close()
	time.Sleep(1100 * time.Millisecond)
	if q := e.poll(); !q.Status.LastSyncTime.Equal(beat) || state(q) != "queued" {
		t.Fatalf("plugin down: %s, heartbeat %v, was %v", state(q), q.Status.LastSyncTime, beat)
	}
}

func TestDelete(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(testRequest(2))
	if err := e.remove(); err != nil {
		t.Fatal(err)
	}
	if got, want := e.plugin.deletedUIDs(), []string{memberUID(executorWorkload, 0), memberUID(executorWorkload, 1)}; !slices.Equal(got, want) {
		t.Fatalf("/delete for %v, want %v", got, want)
	}
	if len(e.shadow.deleted) != 1 || e.shadow.deleted[0] != executorWorkload {
		t.Fatalf("shadow deletes: %v", e.shadow.deleted)
	}
	if q := e.request(); slices.Contains(q.Finalizers, v1alpha1.ExecutorFinalizer) {
		t.Fatalf("finalizers %v", q.Finalizers)
	}
	if rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload); rec.Code != http.StatusNotFound {
		t.Fatalf("logs after the delete = %d", rec.Code)
	}
	e.reconcile()
	if len(e.plugin.deletedUIDs()) != 2 {
		t.Fatalf("a second reconcile deleted again")
	}

	// A plugin that fails the delete keeps the finalizer, so the cancel is
	// tried again.
	down := newExecutorEnv(t)
	down.submit(testRequest(1))
	down.plugin.srv.Close()
	if err := down.remove(); err == nil || !slices.Contains(down.request().Finalizers, v1alpha1.ExecutorFinalizer) {
		t.Fatalf("delete with the plugin down: %v, finalizers %v", err, down.request().Finalizers)
	}
}

// A deleted request's job is cancelled in every state: the members the plugin
// has get /delete, the shadow goes, and the finalizer is removed.
func TestDeleteInEachState(t *testing.T) {
	end := time.Now()
	tests := []struct {
		name    string
		prepare func(e *executorEnv)
		state   string
		deleted int
	}{
		{"waiting to submit", func(e *executorEnv) {
			e.plugin.with(func(p *fakePlugin) { p.createCodes = []int{http.StatusServiceUnavailable} })
			e.take(testRequest(1))
		}, "waiting", 0},
		{"refused", func(e *executorEnv) {
			req := testRequest(1)
			req.Spec.Account = "proj99"
			e.take(req)
		}, "refused", 0},
		{"queued", func(e *executorEnv) { e.submit(testRequest(1)) }, "queued", 1},
		{"running", func(e *executorEnv) {
			e.submit(testRequest(1))
			e.setStatus(0, "node07", runningContainer(executorCtn, end))
			e.poll()
		}, "running", 1},
		{"succeeded", func(e *executorEnv) {
			e.submit(testRequest(1))
			e.setStatus(0, "node07", terminatedContainer(executorCtn, 0, "Completed", end, end))
			e.poll()
		}, "succeeded", 1},
		{"failed", func(e *executorEnv) {
			e.submit(testRequest(1))
			e.setStatus(0, "node07", terminatedContainer(executorCtn, 1, "Error", end, end))
			e.poll()
		}, "failed", 1},
		{"evicted", func(e *executorEnv) {
			e.submit(testRequest(1))
			e.setStatus(0, "node07", terminatedContainer(executorCtn, 0, ReasonCancelled, end, end))
			e.poll()
		}, "evicted", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			tt.prepare(e)
			// A waiting request's member was deleted by the refused submit itself.
			before := len(e.plugin.deletedUIDs())
			if got := state(e.request()); got != tt.state {
				t.Fatalf("state %s, want %s", got, tt.state)
			}
			if err := e.remove(); err != nil {
				t.Fatal(err)
			}
			if n := len(e.plugin.deletedUIDs()) - before; n != tt.deleted {
				t.Fatalf("%d members deleted, want %d", n, tt.deleted)
			}
			if len(e.shadow.deleted) != 1 || slices.Contains(e.request().Finalizers, v1alpha1.ExecutorFinalizer) {
				t.Fatalf("shadow deletes %v, finalizers %v", e.shadow.deleted, e.request().Finalizers)
			}
		})
	}
}

// Only the executor's own changes of a request reconcile it: a status write
// would otherwise poll the job again at once.
func TestRequestChangedPredicate(t *testing.T) {
	q := testRequest(1)
	status := q.DeepCopy()
	status.Status.LastSyncTime = &metav1.Time{Time: time.Now()}
	finalizer := q.DeepCopy()
	finalizer.Finalizers = append(finalizer.Finalizers, v1alpha1.ExecutorFinalizer)
	deleted := q.DeepCopy()
	deleted.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	label := q.DeepCopy()
	label.Labels[v1alpha1.SiteLabel] = "hpc-b"
	for name, tc := range map[string]struct {
		next *v1alpha1.OffloadRequest
		want bool
	}{
		"status": {status, false}, "finalizer": {finalizer, false}, "deletion": {deleted, true}, "label": {label, true},
	} {
		if got := requestChanged.Update(event.UpdateEvent{ObjectOld: q, ObjectNew: tc.next}); got != tc.want {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
}

// setLog sets a member's container log as the plugin returns it.
func (e *executorEnv) setLog(i int, container, text string) {
	e.plugin.with(func(p *fakePlugin) { p.logs[memberUID(executorWorkload, i)+"/"+container] = text })
}

func (e *executorEnv) appendLog(i int, container, text string) {
	e.plugin.with(func(p *fakePlugin) { p.logs[memberUID(executorWorkload, i)+"/"+container] += text })
}

func TestGetLogs(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(testRequest(3))
	for i, text := range []string{"zero a\nzero b\n", "one\n", "two a\ntwo b"} {
		e.setLog(i, executorCtn, text)
	}

	rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload+"&rank=2")
	if rec.Code != http.StatusOK || rec.Body.String() != "two a\ntwo b" {
		t.Fatalf("rank 2 = %d %q", rec.Code, rec.Body.String())
	}
	rec = e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload)
	want := "[rank 0] zero a\n[rank 0] zero b\n[rank 1] one\n[rank 2] two a\n[rank 2] two b\n"
	if rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Fatalf("merged = %d %q, want %q", rec.Code, rec.Body.String(), want)
	}
	rec = e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload+"&tail=2")
	if rec.Body.String() != "[rank 2] two a\n[rank 2] two b\n" {
		t.Fatalf("tail 2 = %q", rec.Body.String())
	}
	for target, code := range map[string]int{
		"/getLogs?workloadUID=" + executorWorkload + "&rank=3":           http.StatusBadRequest,
		"/getLogs?workloadUID=" + executorWorkload + "&container=nope":   http.StatusBadRequest,
		"/getLogs?workloadUID=" + executorWorkload + "&tail=-1":          http.StatusBadRequest,
		"/getLogs?workloadUID=" + "0b6c1d2e-3f40-4a5b-8c6d-000000000000": http.StatusNotFound,
		"/getLogs": http.StatusBadRequest,
	} {
		if rec := e.do(http.MethodGet, target); rec.Code != code {
			t.Errorf("%s = %d, want %d", target, rec.Code, code)
		}
	}
}

// The logs of a Workload are those of its newest attempt's job.
func TestGetLogsOfTheNewestAttempt(t *testing.T) {
	e := newExecutorEnv(t)
	first := testRequest(1)
	e.submit(first)
	e.setLog(0, executorCtn, "attempt 1\n")
	if rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload); rec.Body.String() != "attempt 1\n" {
		t.Fatalf("first attempt = %q", rec.Body.String())
	}
	if err := e.remove(); err != nil {
		t.Fatal(err)
	}
	if rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload); rec.Code != http.StatusNotFound {
		t.Fatalf("between the attempts = %d", rec.Code)
	}
	e.submit(attempt(first, 2))
	e.setLog(0, executorCtn, "attempt 2\n")
	if rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload); rec.Body.String() != "attempt 2\n" {
		t.Fatalf("second attempt = %q", rec.Body.String())
	}
}

func TestGetLogsSinglePodHasNoPrefix(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(testRequest(1))
	e.setLog(0, executorCtn, "hello\n")
	if rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload); rec.Body.String() != "hello\n" {
		t.Fatalf("single member = %q", rec.Body.String())
	}
}

// followLogs reads a followed log through a real server, as the shadow does.
func (e *executorEnv) followLogs(query string) <-chan string {
	srv := httptest.NewServer(e.mux)
	e.t.Cleanup(srv.Close)
	done := make(chan string, 1)
	go func() {
		resp, err := http.Get(srv.URL + "/getLogs?workloadUID=" + executorWorkload + query)
		if err != nil {
			done <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(resp.Body); err != nil {
			done <- "error: " + err.Error()
			return
		}
		done <- buf.String()
	}()
	return done
}

// A followed log waits for the job to start, relays every rank's stream through
// the API server as lines arrive, and ends once every rank's stream ended with its
// container.
func TestGetLogsFollow(t *testing.T) {
	oldPoll := logsPollInterval
	logsPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { logsPollInterval = oldPoll })

	e := newExecutorEnv(t)
	e.submit(testRequest(2))
	e.setLog(0, executorCtn, "first\n")
	done := e.followLogs("&follow=true")

	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 2; i++ {
		e.setStatus(i, "node0"+string(rune('7'+i)), runningContainer(executorCtn, time.Now()))
	}
	e.poll()
	time.Sleep(50 * time.Millisecond)
	e.appendLog(1, executorCtn, "worker up\nhalf")
	time.Sleep(50 * time.Millisecond)
	e.appendLog(0, executorCtn, "second\n")
	e.appendLog(1, executorCtn, " line\nlast")
	select {
	case got := <-done:
		t.Fatalf("stream ended before the job: %q", got)
	case <-time.After(50 * time.Millisecond):
	}
	end := time.Now()
	for i := 0; i < 2; i++ {
		e.setStatus(i, "", terminatedContainer(executorCtn, 0, "Completed", end, end))
	}
	e.plugin.with(func(p *fakePlugin) {
		p.ended[memberUID(executorWorkload, 0)] = true
		p.ended[memberUID(executorWorkload, 1)] = true
	})
	select {
	case got := <-done:
		var rank0, rank1 []string
		for _, line := range strings.SplitAfter(got, "\n") {
			switch {
			case strings.HasPrefix(line, "[rank 0] "):
				rank0 = append(rank0, line)
			case strings.HasPrefix(line, "[rank 1] "):
				rank1 = append(rank1, line)
			case line != "":
				t.Errorf("line without a rank: %q", line)
			}
		}
		if strings.Join(rank0, "") != "[rank 0] first\n[rank 0] second\n" || strings.Join(rank1, "") != "[rank 1] worker up\n[rank 1] half line\n[rank 1] last\n" {
			t.Fatalf("followed = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stream did not end with the job")
	}
}

// A stream that ends while the job runs (a plugin without Follow, or the API
// server losing the plugin's stream) is opened again, and the caller gets each
// line once, until the container ended. Once the job ended, the request's
// status ends the log at once: the drain.
func TestGetLogsFollowReopensEndedStream(t *testing.T) {
	oldReopen := logsReopenDelay
	logsReopenDelay = 10 * time.Millisecond
	t.Cleanup(func() { logsReopenDelay = oldReopen })

	e := newExecutorEnv(t)
	e.submit(testRequest(1))
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	e.poll()
	e.setLog(0, executorCtn, "first\n")
	e.plugin.with(func(p *fakePlugin) { p.ended[memberUID(executorWorkload, 0)] = true })
	done := e.followLogs("&follow=true")

	time.Sleep(60 * time.Millisecond)
	e.appendLog(0, executorCtn, "second\nla")
	time.Sleep(60 * time.Millisecond)
	select {
	case got := <-done:
		t.Fatalf("stream ended before the job: %q", got)
	default:
	}
	e.appendLog(0, executorCtn, "st")
	end := time.Now()
	e.setStatus(0, "", terminatedContainer(executorCtn, 0, "Completed", end, end))
	select {
	case got := <-done:
		if got != "first\nsecond\nlast\n" {
			t.Fatalf("followed = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stream did not end with the job")
	}

	// The job ended: a new follower gets the whole log and the end.
	if q := e.poll(); state(q) != "succeeded" {
		t.Fatalf("state %s", state(q))
	}
	select {
	case got := <-e.followLogs("&follow=true"):
		if got != "first\nsecond\nlast\n" {
			t.Fatalf("after the end = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a follower of an ended job did not end")
	}
}

// With a tail the lines of a new stream cannot be matched to the old one, so a
// stream that ends while the job runs is broken: the client sees the connection
// fail and asks again.
func TestGetLogsFollowTailBrokenStream(t *testing.T) {
	e := newExecutorEnv(t)
	e.submit(testRequest(1))
	e.setStatus(0, "node07", runningContainer(executorCtn, time.Now()))
	e.poll()
	e.setLog(0, executorCtn, "first\n")
	e.plugin.with(func(p *fakePlugin) { p.ended[memberUID(executorWorkload, 0)] = true })
	if got := <-e.followLogs("&follow=true&tail=1"); !strings.HasPrefix(got, "error: ") {
		t.Fatalf("a stream that ended before its container read as complete: %q", got)
	}
}

// /healthz is the interLink API server's ping, which reaches the plugin.
func TestHealthz(t *testing.T) {
	e := newExecutorEnv(t)
	for range 2 {
		if rec := e.do(http.MethodGet, "/healthz"); rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			t.Fatalf("healthz = %d %q", rec.Code, rec.Body.String())
		}
	}
	var pings int
	e.plugin.with(func(p *fakePlugin) { pings = p.pings })
	if pings != 1 {
		t.Fatalf("%d pings, want 1 within %s", pings, healthTTL)
	}
	e.d.healthyUntil = time.Time{}
	e.plugin.srv.Close()
	if rec := e.do(http.MethodGet, "/healthz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("healthz with the plugin down = %d", rec.Code)
	}
	for _, target := range []string{"/submit", "/jobs/" + executorWorkload} {
		if rec := e.do(http.MethodPost, target); rec.Code != http.StatusNotFound {
			t.Errorf("POST %s = %d: the old routes are gone", target, rec.Code)
		}
	}
}

func TestValidateConfig(t *testing.T) {
	nsFile := filepath.Join(t.TempDir(), "namespace")
	if err := os.WriteFile(nsFile, []byte("alice\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := serviceAccountNamespaceFile
	serviceAccountNamespaceFile = nsFile
	t.Cleanup(func() { serviceAccountNamespaceFile = old })

	base := func() Config {
		return Config{
			InterLink: InterLinkConfig{URL: "http://localhost:3000/"},
			Sites:     map[string]SiteConfig{executorSite: {AllowedAccounts: []string{executorAccount}}},
		}
	}
	c := base()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":8000" || c.InterLink.URL != "http://localhost:3000" || c.Executor.Namespace != "alice" ||
		c.Executor.pollInterval != 15*time.Second || c.Executor.lostTimeout != 10*time.Minute {
		t.Fatalf("defaults: %+v", c)
	}

	bad := map[string]func(*Config){
		"no API server":       func(c *Config) { c.InterLink.URL = "" },
		"API server not http": func(c *Config) { c.InterLink.URL = "unix:///run/interlink.sock" },
		"two sites":           func(c *Config) { c.Sites["hpc-b"] = c.Sites[executorSite] },
		"no sites":            func(c *Config) { c.Sites = nil },
		"no accounts":         func(c *Config) { c.Sites[executorSite] = SiteConfig{} },
		"bad lost timeout":    func(c *Config) { c.Executor.LostTimeout = "soon" },
		"bad poll interval":   func(c *Config) { c.Executor.PollInterval = "-1s" },
		"shadow without key":  func(c *Config) { c.Shadow.Enabled = true },
	}
	for name, mutate := range bad {
		c := base()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestLoadConfigRefusesUnknownKeys(t *testing.T) {
	dir := t.TempDir()
	good := "InterLink: {URL: http://localhost:3000}\nExecutor:\n  Namespace: alice\n  PollInterval: 10s\n  LostTimeout: 5m\nSites:\n  hpc-a: {AllowedAccounts: [proj42]}\n"
	if err := os.WriteFile(filepath.Join(dir, "good.yaml"), []byte(good), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(filepath.Join(dir, "good.yaml"))
	if err != nil || c.Executor.lostTimeout != 5*time.Minute || c.Executor.pollInterval != 10*time.Second {
		t.Fatalf("LoadConfig = %+v, %v", c, err)
	}
	for _, key := range []string{"SbatchPath: /usr/bin/sbatch\n", "StateDir: /state\n"} {
		if err := os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte(good+key), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(filepath.Join(dir, "bad.yaml")); err == nil || !strings.Contains(err.Error(), strings.Split(key, ":")[0]) {
			t.Fatalf("%q was accepted: %v", key, err)
		}
	}
}
