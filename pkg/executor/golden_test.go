package executor

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/membership"
	"github.com/matbun/kueue-offload/pkg/offload"
	"github.com/matbun/kueue-offload/pkg/shadow"
)

// referenceFlavorsBefore is the reference OffloadConfig's flavors as they were
// written before flavors carried annotations: a partition and a QOS each.
var referenceFlavorsBefore = map[string]struct{ partition, qos string }{
	"hpc-a-gpu": {partition: "gpu"},
	"hpc-a-cpu": {partition: "cpu"},
}

// oldFlags is the sbatch flags buildMember wrote before flavors carried
// annotations: the template's own, then the account, the partition and the QOS.
func oldFlags(own, account, partition, qos string) string {
	flags := strings.TrimSpace(own + " --account=" + account)
	if partition != "" {
		flags += " --partition=" + partition
	}
	if qos != "" {
		flags += " --qos=" + qos
	}
	return flags
}

func referenceOffloadConfig(t *testing.T) *v1alpha1.OffloadConfig {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "samples", "membership", "offloadconfig.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	sc := &v1alpha1.OffloadConfig{}
	if err := yaml.UnmarshalStrict(raw, sc); err != nil {
		t.Fatal(err)
	}
	return sc
}

// Each flavor of the reference OffloadConfig, expanded by the offload controller
// and appended by the executor, gives the member the sbatch flags it had before;
// a Workload with a maximum execution time also gets --time, in minutes.
func TestReferenceFlavorsRenderTheSameFlags(t *testing.T) {
	sc := referenceOffloadConfig(t)
	names := make([]string, 0, len(sc.Spec.Flavors))
	for name := range sc.Spec.Flavors {
		names = append(names, name)
	}
	slices.Sort(names)
	if want := []string{"hpc-a-cpu", "hpc-a-gpu"}; !slices.Equal(names, want) {
		t.Fatalf("reference flavors %v, want %v", names, want)
	}
	for _, name := range names {
		before := referenceFlavorsBefore[name]
		for _, own := range []string{"", "--gres=gpu:1 --exclusive"} {
			for _, maxExec := range []int32{0, 3601} {
				ann, err := offload.ExpandAnnotations(sc.Spec.Flavors[name].Annotations, offload.FlavorData{
					Account: executorAccount, MaxExecSeconds: maxExec, MaxExecMinutes: (maxExec + 59) / 60,
				})
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				req := jobRequestOf(testRequest(2))
				req.PodSets[0].Annotations = ann
				if own != "" {
					req.PodSets[0].Template.Annotations = map[string]string{flagsKey: own}
				}
				want := oldFlags(own, executorAccount, before.partition, before.qos)
				if maxExec > 0 {
					want += " --time=61"
				}
				for i := 0; i < 2; i++ {
					if got := buildMember(req, i).Annotations[flagsKey]; got != want {
						t.Errorf("%s, own %q, maximumExecutionTimeSeconds %d, member %d: flags %q, want %q", name, own, maxExec, i, got, want)
					}
				}
			}
		}
	}
}

// Submit description lines of the template and of the flavor stay on their own
// lines, so a template line without a trailing newline cannot swallow the site's.
func TestMultiLineAnnotationsJoinByNewline(t *testing.T) {
	const key = "htcondor-job.vk.io/submit-lines"
	for _, own := range []string{"request_disk = 1G", "request_disk = 1G\n"} {
		req := jobRequestOf(testRequest(1))
		req.PodSets[0].Template.Annotations = map[string]string{key: own}
		req.PodSets[0].Annotations = map[string]string{key: "accounting_group = group_proj42\n+MaxWallTimeMins = 20\n"}
		want := "request_disk = 1G\naccounting_group = group_proj42\n+MaxWallTimeMins = 20\n"
		if got := buildMember(req, 0).Annotations[key]; got != want {
			t.Errorf("own %q: %q, want %q", own, got, want)
		}
	}
}

// The sample executor template renders a configuration the executor accepts.
func TestSampleExecutorConfigLoads(t *testing.T) {
	t.Setenv(shadow.ExecutorImageEnv, "")
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "samples", "membership", "executor-template.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{}
	if err := yaml.Unmarshal(raw, cm); err != nil {
		t.Fatal(err)
	}
	tmpl, err := membership.ParseTemplate(cm.Data["template.yaml"])
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, membership.TemplateData{Namespace: executorNS, Site: executorSite, HPCUser: "alice",
		Accounts: []string{executorAccount, "proj99"}, Image: "registry.example.org/kueue-offload:test", ExecutorService: "kueue-offload-executor", ExecutorPort: 8000}); err != nil {
		t.Fatal(err)
	}
	var config string
	for _, doc := range strings.Split(out.String(), "\n---\n") {
		rendered := &corev1.ConfigMap{}
		if err := yaml.Unmarshal([]byte(doc), rendered); err == nil && rendered.Kind == "ConfigMap" {
			config = rendered.Data["executor.yaml"]
		}
	}
	if config == "" {
		t.Fatalf("the sample template renders no executor.yaml:\n%s", out.String())
	}
	path := filepath.Join(t.TempDir(), "executor.yaml")
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	// The namespace comes from the ServiceAccount.
	nsFile := filepath.Join(t.TempDir(), "namespace")
	if err := os.WriteFile(nsFile, []byte(executorNS), 0o600); err != nil {
		t.Fatal(err)
	}
	old := serviceAccountNamespaceFile
	serviceAccountNamespaceFile = nsFile
	t.Cleanup(func() { serviceAccountNamespaceFile = old })
	t.Setenv(shadow.ExecutorImageEnv, "registry.example.org/kueue-offload:test")
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("the sample's executor.yaml is refused: %v\n%s", err, config)
	}
	if site, s := c.Site(); site != executorSite || !slices.Equal(s.AllowedAccounts, []string{executorAccount, "proj99"}) || c.Executor.Namespace != executorNS {
		t.Fatalf("sample config %+v", c)
	}
}
