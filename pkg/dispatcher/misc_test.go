package dispatcher

import (
	"context"
	"encoding/json"
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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/yaml"
)

func writeFileWithMtime(path, content string, mtime time.Time) error {
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return err
	}
	return os.Chtimes(path, mtime, mtime)
}

func TestTokenSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := writeFileWithMtime(path, "a\n", time.Now()); err != nil {
		t.Fatal(err)
	}
	ts := &TokenSource{Path: path}
	if tok, err := ts.Token(); err != nil || tok != "a" {
		t.Fatalf("token = %q, %v", tok, err)
	}
	if err := writeFileWithMtime(path, "bb", time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if tok, _ := ts.Token(); tok != "bb" {
		t.Errorf("token after change = %q, want bb", tok)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.Token(); err == nil {
		t.Errorf("missing token file gave no error")
	}
}

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
		Name: "hpc-a", State: kueue.CheckStatePending, Message: "Slurm job 1 queued (Priority)", LastTransitionTime: ltt,
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
			"name": "hpc-a", "state": "Pending", "message": "Slurm job 1 queued (Priority)",
			"lastTransitionTime": "2026-10-29T10:14:02Z",
		}}},
	}
	gb, _ := json.Marshal(got)
	wb, _ := json.Marshal(want)
	if string(gb) != string(wb) {
		t.Errorf("apply body\n got %s\nwant %s", gb, wb)
	}
}

func TestAdmissionCheckActive(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(siteConfig(), admissionCheck("hpc-a", ControllerName, testSite),
			admissionCheck("hpc-b", ControllerName, "hpc-b"),
			admissionCheck("other-check", "example.org/other", testSite)).
		WithStatusSubresource(&kueue.AdmissionCheck{}).Build()
	r := &AdmissionCheckReconciler{Client: c, Site: testSite}
	for _, name := range []string{"hpc-a", "hpc-b", "other-check"} {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: name}}); err != nil {
			t.Fatal(err)
		}
	}
	get := func(name string) *kueue.AdmissionCheck {
		ac := &kueue.AdmissionCheck{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: name}, ac); err != nil {
			t.Fatal(err)
		}
		return ac
	}
	if !meta.IsStatusConditionTrue(get("hpc-a").Status.Conditions, kueue.AdmissionCheckActive) {
		t.Errorf("own check not Active")
	}
	for _, name := range []string{"hpc-b", "other-check"} {
		if len(get(name).Status.Conditions) != 0 {
			t.Errorf("check %s touched", name)
		}
	}

	// Without the SiteConfig the check is reported inactive.
	c2 := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(admissionCheck("hpc-a", ControllerName, testSite)).
		WithStatusSubresource(&kueue.AdmissionCheck{}).Build()
	r2 := &AdmissionCheckReconciler{Client: c2, Site: testSite}
	if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "hpc-a"}}); err != nil {
		t.Fatal(err)
	}
	ac := &kueue.AdmissionCheck{}
	_ = c2.Get(context.Background(), types.NamespacedName{Name: "hpc-a"}, ac)
	if cond := meta.FindStatusCondition(ac.Status.Conditions, kueue.AdmissionCheckActive); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Errorf("Active = %+v without a SiteConfig", cond)
	}
}

func TestPartitionConflictRejected(t *testing.T) {
	s := resolveSite(siteConfig())
	asg := &kueue.PodSetAssignment{}
	asg.Flavors = map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "hpc-a-cpu", "nvidia.com/gpu": "hpc-a-gpu"}
	if _, err := partitionFor(asg, s); err == nil || !strings.Contains(err.Error(), "cpu, gpu") {
		t.Errorf("err = %v, want a conflict between cpu and gpu", err)
	}
	asg.Flavors = map[corev1.ResourceName]kueue.ResourceFlavorReference{"cpu": "unmapped"}
	if p, err := partitionFor(asg, s); err != nil || p != "" {
		t.Errorf("unmapped flavor gave %q, %v", p, err)
	}
}

// TestRBACGrantsNoSecrets fails when the dispatcher's RBAC reaches what the
// design forbids: Secrets, ConfigMaps, or pods beyond reading them and
// patching their status for the terminal phase (D2).
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
	sawStatus := false
	for _, r := range rules {
		core := has(r.APIGroups, "")
		for _, res := range r.Resources {
			name := strings.SplitN(res, "/", 2)[0]
			if core && (name == "secrets" || name == "configmaps" || res == "*") {
				t.Errorf("RBAC grants %v on %s", r.Verbs, res)
			}
			if core && (name == "pods" || res == "*") {
				allowed := map[string][]string{"pods": {"get", "list", "watch"}, "pods/status": {"get", "patch"}}[res]
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
	}
	if !sawStatus {
		t.Errorf("workloads/status patch missing: the lint read the wrong files")
	}
}
