package offload

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/yaml"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// The fake client merges server-side apply patches without the list-map
// semantics of status.admissionChecks, so the writer is checked on the patch
// it sends rather than on the merged result.
func TestSSACheckWriterPatch(t *testing.T) {
	var (
		calls   int
		sub     string
		ptype   types.PatchType
		body    []byte
		options client.SubResourcePatchOptions
	)
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(workload()).
		WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(_ context.Context, _ client.Client, s string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
				calls++
				sub, ptype = s, p.Type()
				body, _ = p.Data(obj)
				options.ApplyOptions(opts)
				return nil
			},
		}).Build()
	ltt := metav1.NewTime(time.Date(2026, 10, 29, 10, 14, 2, 0, time.UTC))
	w := &SSACheckWriter{Client: c}
	err := w.WriteCheck(context.Background(), workload(), kueue.AdmissionCheckState{
		Name: "hpc-a", State: kueue.CheckStatePending, Message: "job 1 queued (Priority)", LastTransitionTime: ltt,
		RetryCount: new(int32),
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || sub != "status" || ptype != types.ApplyPatchType {
		t.Fatalf("calls %d subresource %q type %q", calls, sub, ptype)
	}
	if options.FieldManager != FieldManager || options.Force == nil || !*options.Force {
		t.Errorf("field manager %q force %v", options.FieldManager, options.Force)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2",
		"kind":       "Workload",
		"metadata":   map[string]any{"name": wlName, "namespace": testNS},
		"status": map[string]any{"admissionChecks": []any{map[string]any{
			"name": "hpc-a", "state": "Pending", "message": "job 1 queued (Priority)",
			"lastTransitionTime": "2026-10-29T10:14:02Z",
		}}},
	}
	gb, _ := json.Marshal(got)
	wb, _ := json.Marshal(want)
	if string(gb) != string(wb) {
		t.Errorf("apply body\n got %s\nwant %s", gb, wb)
	}

	body, err = checkApplyBody(workload(), kueue.AdmissionCheckState{
		Name: "hpc-a", State: kueue.CheckStateRetry, Message: "lost", LastTransitionTime: ltt,
		RequeueAfterSeconds: ptr.To[int32](120), RetryCount: ptr.To[int32](1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if s := string(body); !strings.Contains(s, `"state":"Retry"`) || !strings.Contains(s, `"requeueAfterSeconds":120`) || strings.Contains(s, "retryCount") {
		t.Errorf("Retry apply body %s, want requeueAfterSeconds and no retryCount", s)
	}
}

func worker(name string, active metav1.ConditionStatus) *v1alpha1.OffloadWorker {
	w := &v1alpha1.OffloadWorker{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.OffloadWorkerSpec{LoginEndpoint: "login." + name + ":22"}}
	if active != "" {
		w.Status.Conditions = []metav1.Condition{{Type: v1alpha1.OffloadWorkerActive, Status: active, Reason: "Probe", LastTransitionTime: metav1.Now()}}
	}
	return w
}

// A check is Active while the OffloadWorker its OffloadConfig names is, as a
// MultiKueue check follows its MultiKueueClusters; checks of other controllers
// are left alone.
func TestAdmissionCheckActiveFromWorkers(t *testing.T) {
	noWorkers := offloadConfig()
	noWorkers.Name, noWorkers.Spec.Workers = "hpc-c", []string{"hpc-c"}
	down := offloadConfig()
	down.Name, down.Spec.Workers = "hpc-b", []string{"hpc-b"}
	c := testClient(t, []client.Object{
		offloadConfig(), down, noWorkers, worker(testSite, metav1.ConditionTrue), worker("hpc-b", metav1.ConditionFalse),
		admissionCheck("hpc-a", ControllerName, testSite), admissionCheck("hpc-b", ControllerName, "hpc-b"),
		admissionCheck("hpc-c", ControllerName, "hpc-c"), admissionCheck("no-config", ControllerName, "hpc-z"),
		admissionCheck("other-check", "example.org/other", testSite),
	}, interceptor.Funcs{})
	configs, err := NewConfigHelper(c)
	if err != nil {
		t.Fatal(err)
	}
	r := &AdmissionCheckReconciler{Client: c, Configs: configs}
	active := func(name string) *metav1.Condition {
		t.Helper()
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: name}}); err != nil {
			t.Fatal(err)
		}
		ac := &kueue.AdmissionCheck{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: name}, ac); err != nil {
			t.Fatal(err)
		}
		return meta.FindStatusCondition(ac.Status.Conditions, kueue.AdmissionCheckActive)
	}
	for name, want := range map[string]struct {
		status          metav1.ConditionStatus
		reason, message string
	}{
		"hpc-a":     {metav1.ConditionTrue, "Active", "The admission check is active"},
		"hpc-b":     {metav1.ConditionFalse, "NoUsableWorkers", "Inactive workers: [hpc-b]"},
		"hpc-c":     {metav1.ConditionFalse, "NoUsableWorkers", "Missing workers: [hpc-c]"},
		"no-config": {metav1.ConditionFalse, "BadConfig", ""},
	} {
		got := active(name)
		if got == nil || got.Status != want.status || got.Reason != want.reason || want.message != "" && got.Message != want.message {
			t.Errorf("check %s Active = %+v, want %s %s %q", name, got, want.status, want.reason, want.message)
		}
	}
	if got := active("other-check"); got != nil {
		t.Errorf("another controller's check touched: %+v", got)
	}

	// The worker coming back makes the check Active; the OffloadWorker's
	// watch reaches the check through its OffloadConfig.
	w := &v1alpha1.OffloadWorker{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "hpc-b"}, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Conditions[0].Status = metav1.ConditionTrue
	if err := c.Status().Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if reqs := r.checksOfWorker(context.Background(), w); len(reqs) != 1 || reqs[0].Name != "hpc-b" {
		t.Errorf("checks of worker hpc-b %v, want hpc-b", reqs)
	}
	if got := active("hpc-b"); got == nil || got.Status != metav1.ConditionTrue {
		t.Errorf("check hpc-b Active = %+v after its worker came back", got)
	}
}

// An OffloadWorker is Active while a TCP connection to its login endpoint
// succeeds; a malformed endpoint is a bad configuration.
func TestWorkerActiveFromEndpoint(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	reachable := ln.Addr().String()
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unreachable := closed.Addr().String()
	closed.Close()
	defer ln.Close()

	for _, tc := range []struct {
		endpoint string
		status   metav1.ConditionStatus
		reason   string
	}{
		{reachable, metav1.ConditionTrue, "Active"},
		{unreachable, metav1.ConditionFalse, "Unreachable"},
		{"login.hpc-a.example.org", metav1.ConditionFalse, "BadConfig"},
		{"login.hpc-a.example.org:ssh", metav1.ConditionFalse, "BadConfig"},
	} {
		w := &v1alpha1.OffloadWorker{ObjectMeta: metav1.ObjectMeta{Name: testSite}, Spec: v1alpha1.OffloadWorkerSpec{LoginEndpoint: tc.endpoint}}
		c := testClient(t, []client.Object{w}, interceptor.Funcs{})
		r := &WorkerReconciler{Client: c}
		res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: testSite}})
		if err != nil {
			t.Fatal(err)
		}
		if res.RequeueAfter != time.Minute {
			t.Errorf("%s: requeue %v, want a probe a minute", tc.endpoint, res.RequeueAfter)
		}
		if err := c.Get(context.Background(), types.NamespacedName{Name: testSite}, w); err != nil {
			t.Fatal(err)
		}
		got := meta.FindStatusCondition(w.Status.Conditions, v1alpha1.OffloadWorkerActive)
		if got == nil || got.Status != tc.status || got.Reason != tc.reason {
			t.Errorf("%s: Active = %+v, want %s %s", tc.endpoint, got, tc.status, tc.reason)
		}
	}
}

func TestFlavorConflictRejected(t *testing.T) {
	s := resolveSite(offloadConfig())
	data := newFlavorData("proj42", 0)
	asg := &kueue.PodSetAssignment{}
	asg.Flavors = map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-cpu", "nvidia.com/gpu": "hpc-a-gpu"}
	if _, err := annotationsFor(asg, s, data); err == nil || !strings.Contains(err.Error(), "hpc-a-cpu and hpc-a-gpu map to different queues") {
		t.Errorf("err = %v, want a conflict between cpu and gpu", err)
	}
	// An unlisted flavor next to a listed one is left out.
	asg.Flavors = map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-gpu", "memory": "unmapped"}
	if ann, err := annotationsFor(asg, s, data); err != nil || ann[flagsKey] != "--account=proj42 --partition=gpu" {
		t.Errorf("gpu with an unmapped flavor gave %v, %v", ann, err)
	}
	asg.Flavors = map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "unmapped"}
	if ann, err := annotationsFor(asg, s, data); err == nil {
		t.Errorf("unmapped flavor gave %v", ann)
	}
	// One project, several queues: the flavor Kueue assigned gives the queue.
	asg.Flavors = map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-debug", "nvidia.com/gpu": "hpc-a-debug"}
	if ann, err := annotationsFor(asg, s, data); err != nil || ann[flagsKey] != "--account=proj42 --partition=gpu --qos=debug --time=30" {
		t.Errorf("debug flavor gave %v, %v", ann, err)
	}
	if _, err := ExpandAnnotations(map[string]string{flagsKey: "--account={{.Acount}}"}, data); err == nil {
		t.Errorf("a template naming an unknown field was expanded")
	}
}

// TestRBACGrantsNoSecrets fails when the offload controller's RBAC reaches what the
// design forbids: Secrets, ConfigMaps, or pods beyond reading them, annotating a
// failed pod group and patching their status for the terminal phase.
func TestRBACGrantsNoSecrets(t *testing.T) {
	files, err := filepath.Glob("../../config/rbac/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no RBAC files: %v", err)
	}
	var rules []rbacv1.PolicyRule
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, doc := range strings.Split(string(data), "\n---") {
			var obj struct {
				Kind  string              `json:"kind"`
				Rules []rbacv1.PolicyRule `json:"rules"`
			}
			if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			if obj.Kind == "ClusterRole" || obj.Kind == "Role" {
				rules = append(rules, obj.Rules...)
			}
		}
	}
	if len(rules) == 0 {
		t.Fatal("no rules found")
	}
	has := func(list []string, v string) bool {
		for _, x := range list {
			if x == v || x == "*" {
				return true
			}
		}
		return false
	}
	sawStatus, sawRequests := false, false
	for _, r := range rules {
		core := has(r.APIGroups, "")
		for _, res := range r.Resources {
			name := strings.SplitN(res, "/", 2)[0]
			if core && (name == "secrets" || name == "configmaps" || res == "*") {
				t.Errorf("RBAC grants %v on %s", r.Verbs, res)
			}
			if core && (name == "pods" || res == "*") {
				// pods patch sets the retriable-in-group annotation on a failed pod group.
				allowed := map[string][]string{"pods": {"get", "list", "watch", "patch"}, "pods/status": {"get", "patch"}}[res]
				for _, v := range r.Verbs {
					if !slices.Contains(allowed, v) {
						t.Errorf("RBAC grants %s on %s", v, res)
					}
				}
			}
		}
		if has(r.APIGroups, "kueue.x-k8s.io") && has(r.Resources, "workloads/status") && has(r.Verbs, "patch") {
			sawStatus = true
		}
		if has(r.APIGroups, "kueue.x-k8s.io") && has(r.Resources, "workloads") && (has(r.Verbs, "update") || has(r.Verbs, "delete")) {
			t.Errorf("RBAC grants %v on workloads", r.Verbs)
		}
		// The executor alone writes an OffloadRequest's status. The controller
		// creates, deletes and patches its own finalizer; it never replaces one.
		if has(r.APIGroups, "interlink.eu") && (has(r.Resources, "offloadrequests/status") || has(r.Resources, "offloadrequests/finalizers")) {
			t.Errorf("RBAC grants %v on %v", r.Verbs, r.Resources)
		}
		if has(r.APIGroups, "interlink.eu") && has(r.Resources, "offloadrequests") {
			verbs := slices.Sorted(slices.Values(r.Verbs))
			if got := strings.Join(verbs, ","); got != "create,delete,get,list,patch,watch" {
				t.Errorf("RBAC grants %s on offloadrequests, want create,delete,get,list,patch,watch", got)
			}
			sawRequests = true
		}
	}
	if !sawStatus || !sawRequests {
		t.Errorf("workloads/status patch %v, offloadrequests create %v: the lint read the wrong files", sawStatus, sawRequests)
	}
}
