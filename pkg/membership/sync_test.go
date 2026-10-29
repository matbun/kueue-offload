package membership

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/yaml"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// executorConfig parses the SlurmConfig.yaml the sample template renders.
type executorConfig struct {
	Sites map[string]struct {
		User            string   `json:"User"`
		AllowedAccounts []string `json:"AllowedAccounts"`
	} `json:"Sites"`
}

func (e *env) executorConfig(ns string) executorConfig {
	e.t.Helper()
	cm := e.executorConfigMap(ns)
	if cm == nil {
		e.t.Fatalf("executor ConfigMap missing in %s", ns)
	}
	var c executorConfig
	if err := yaml.Unmarshal([]byte(cm.Data["SlurmConfig.yaml"]), &c); err != nil {
		e.t.Fatalf("executor config of %s does not parse: %v\n%s", ns, err, cm.Data["SlurmConfig.yaml"])
	}
	return c
}

func gpuQuota(t *testing.T, cq kueue.ClusterQueue) int64 {
	t.Helper()
	for _, rg := range cq.Spec.ResourceGroups {
		for _, f := range rg.Flavors {
			for _, r := range f.Resources {
				if r.Name == "nvidia.com/gpu" {
					return r.NominalQuota.Value()
				}
			}
		}
	}
	t.Fatalf("ClusterQueue %s has no GPU quota", cq.Name)
	return 0
}

func TestSyncCreatesMemberships(t *testing.T) {
	e := newEnv(t)
	if after := e.sync(); after != time.Minute {
		t.Errorf("requeue after %v while the new executors are not available, want 1m", after)
	}

	cqs := e.clusterQueues()
	var names []string
	for n := range cqs {
		names = append(names, n)
	}
	sort.Strings(names)
	if !reflect.DeepEqual(names, []string{"hpc-a-proj42", "hpc-a-proj99"}) {
		t.Fatalf("ClusterQueues %v, want hpc-a-proj42 and hpc-a-proj99 only", names)
	}
	cq := cqs["hpc-a-proj42"]
	if cq.Spec.CohortName != "hpc-a" || cq.Spec.QueueingStrategy != kueue.BestEffortFIFO {
		t.Errorf("cohort %q strategy %q", cq.Spec.CohortName, cq.Spec.QueueingStrategy)
	}
	if got := cq.Spec.NamespaceSelector.MatchLabels; !reflect.DeepEqual(got, map[string]string{"hpc.interlink.eu/hpc-a-proj42": "true"}) {
		t.Errorf("namespaceSelector %v", got)
	}
	if p := cq.Spec.Preemption; p == nil || p.WithinClusterQueue != kueue.PreemptionPolicyNever || p.ReclaimWithinCohort != kueue.PreemptionPolicyNever {
		t.Errorf("preemption %+v", p)
	}
	if s := cq.Spec.AdmissionChecksStrategy; s == nil || len(s.AdmissionChecks) != 1 || s.AdmissionChecks[0].Name != "hpc-a" {
		t.Errorf("admissionChecksStrategy %+v", s)
	}
	if cq.Labels[ManagedByLabel] != ManagedByValue {
		t.Errorf("ClusterQueue labels %v", cq.Labels)
	}
	if g := gpuQuota(t, cq); g != 64 {
		t.Errorf("proj42 GPU quota %d, want 64 from its ProjectQuota", g)
	}
	if g := gpuQuota(t, cqs["hpc-a-proj99"]); g != 8 {
		t.Errorf("proj99 GPU quota %d, want 8 from defaultProjectQuota", g)
	}
	cohort := &kueue.Cohort{}
	if !e.get(types.NamespacedName{Name: "hpc-a"}, cohort) || cohort.Labels[ManagedByLabel] != ManagedByValue {
		t.Errorf("Cohort hpc-a missing or unlabelled")
	}

	alice, bob := e.namespace("alice"), e.namespace("bob")
	for k, want := range map[string]string{TenantLabel: "true", "hpc.interlink.eu/hpc-a-proj42": "true", "hpc.interlink.eu/hpc-a-proj99": "true"} {
		if alice.Labels[k] != want {
			t.Errorf("alice label %s = %q", k, alice.Labels[k])
		}
	}
	if bob.Labels["hpc.interlink.eu/hpc-a-proj42"] != "true" || hasKey(bob.Labels, "hpc.interlink.eu/hpc-a-proj99") {
		t.Errorf("bob labels %v", bob.Labels)
	}
	wantLQ := map[string]string{
		"alice/hpc-a-proj42": "hpc-a-proj42",
		"alice/hpc-a-proj99": "hpc-a-proj99",
		"bob/hpc-a-proj42":   "hpc-a-proj42",
	}
	if got := e.localQueues(); !reflect.DeepEqual(got, wantLQ) {
		t.Errorf("LocalQueues %v, want %v", got, wantLQ)
	}

	// The template is rendered once per user, with that user's accounts.
	cms := &corev1.ConfigMapList{}
	if err := e.c.List(context.Background(), cms, client.MatchingLabels{ManagedByLabel: ManagedByValue}); err != nil {
		t.Fatal(err)
	}
	var rendered []string
	for _, cm := range cms.Items {
		rendered = append(rendered, cm.Namespace+"/"+cm.Name)
	}
	sort.Strings(rendered)
	if !reflect.DeepEqual(rendered, []string{"alice/kueue-offload-executor", "bob/kueue-offload-executor"}) {
		t.Errorf("rendered ConfigMaps %v", rendered)
	}
	for ns, want := range map[string][]string{"alice": {"proj42", "proj99"}, "bob": {"proj42"}} {
		c := e.executorConfig(ns)
		got := c.Sites["hpc-a"]
		if got.User != ns || !reflect.DeepEqual(got.AllowedAccounts, want) {
			t.Errorf("%s executor config: user %q accounts %v, want %v", ns, got.User, got.AllowedAccounts, want)
		}
	}
	d := &appsv1.Deployment{}
	if !e.get(types.NamespacedName{Namespace: "alice", Name: "kueue-offload-executor"}, d) {
		t.Fatal("executor Deployment missing")
	}
	if d.Labels[ManagedByLabel] != ManagedByValue || d.Labels[SiteLabel] != testSite || d.Spec.Template.Spec.Containers[0].Image != "executor:1" {
		t.Errorf("executor Deployment labels %v image %q", d.Labels, d.Spec.Template.Spec.Containers[0].Image)
	}

	crb := &rbacv1.ClusterRoleBinding{}
	if !e.get(types.NamespacedName{Name: "kueue-offload-executor.hpc-a.alice"}, crb) {
		t.Fatal("token review binding missing")
	}
	wantSubj := []rbacv1.Subject{{Kind: "ServiceAccount", Name: "kueue-offload-executor", Namespace: "alice"}}
	if crb.RoleRef.Name != "system:auth-delegator" || !reflect.DeepEqual(crb.Subjects, wantSubj) {
		t.Errorf("token review binding %+v %+v", crb.RoleRef, crb.Subjects)
	}

	// The user without identity and the other cluster's row are ignored.
	if e.get(types.NamespacedName{Name: "carol"}, &corev1.Namespace{}) {
		t.Errorf("namespace carol created for an unmapped user")
	}
	if hasKey(alice.Labels, "hpc.interlink.eu/hpc-a-proj55") || hasKey(alice.Labels, "hpc.interlink.eu/hpc-b-proj55") {
		t.Errorf("row of another Slurm cluster applied: %v", alice.Labels)
	}
	if e.secretCalls != 0 {
		t.Errorf("%d Secret calls", e.secretCalls)
	}

	// A second pass writes the same state, and with the executors available it
	// waits for the default syncInterval.
	e.setDeploymentAvailable("alice", true)
	e.setDeploymentAvailable("bob", true)
	if after := e.sync(); after != 15*time.Minute {
		t.Errorf("requeue after %v, want the default syncInterval", after)
	}
	if got := e.localQueues(); !reflect.DeepEqual(got, wantLQ) {
		t.Errorf("LocalQueues after resync %v", got)
	}
}

func TestRemovedAssociationRevokesOnlyThatMembership(t *testing.T) {
	alice := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "alice", Labels: map[string]string{
		"team": "ml",
		// Set by hand before the controller ran: never recorded, never removed.
		"hpc.interlink.eu/hpc-a-proj77": "true",
	}, Annotations: map[string]string{"owner": "alice@example.org"}}}
	e := newEnv(t, alice)
	e.sync()

	e.setAssociations(strings.Replace(fixture, "hpc-a|proj99|alice\n", "", 1))
	e.sync()

	ns := e.namespace("alice")
	if hasKey(ns.Labels, "hpc.interlink.eu/hpc-a-proj99") {
		t.Errorf("proj99 label kept after its association was removed")
	}
	for k, want := range map[string]string{
		"team": "ml", "hpc.interlink.eu/hpc-a-proj77": "true", TenantLabel: "true", "hpc.interlink.eu/hpc-a-proj42": "true",
	} {
		if ns.Labels[k] != want {
			t.Errorf("label %s = %q, want %q", k, ns.Labels[k], want)
		}
	}
	if ns.Annotations["owner"] != "alice@example.org" || ns.Annotations[AccountsAnnotation(testSite)] != "proj42" {
		t.Errorf("annotations %v", ns.Annotations)
	}
	if hasKey(ns.Labels, ManagedByLabel) {
		t.Errorf("an existing namespace was marked as managed")
	}
	lqs := e.localQueues()
	if _, ok := lqs["alice/hpc-a-proj99"]; ok {
		t.Errorf("LocalQueue alice/hpc-a-proj99 kept")
	}
	if _, ok := lqs["alice/hpc-a-proj42"]; !ok {
		t.Errorf("LocalQueue alice/hpc-a-proj42 removed")
	}
	if !e.get(types.NamespacedName{Namespace: "alice", Name: "kueue-offload-executor"}, &appsv1.Deployment{}) {
		t.Errorf("executor Deployment removed")
	}
	if got := e.executorConfig("alice").Sites["hpc-a"].AllowedAccounts; !reflect.DeepEqual(got, []string{"proj42"}) {
		t.Errorf("executor accounts %v after removal", got)
	}
	// ClusterQueues are never deleted.
	if _, ok := e.clusterQueues()["hpc-a-proj99"]; !ok {
		t.Errorf("ClusterQueue hpc-a-proj99 deleted")
	}

	// alice leaves her last project: the namespace and the executor stay.
	e.setAssociations("hpc-a|proj42|bob\n")
	e.sync()
	ns = e.namespace("alice")
	if hasKey(ns.Labels, "hpc.interlink.eu/hpc-a-proj42") || hasKey(ns.Annotations, AccountsAnnotation(testSite)) {
		t.Errorf("last membership kept: labels %v annotations %v", ns.Labels, ns.Annotations)
	}
	if ns.Labels["team"] != "ml" || ns.Labels["hpc.interlink.eu/hpc-a-proj77"] != "true" {
		t.Errorf("foreign labels lost: %v", ns.Labels)
	}
	if _, ok := e.localQueues()["alice/hpc-a-proj42"]; ok {
		t.Errorf("LocalQueue alice/hpc-a-proj42 kept")
	}
	if !e.get(types.NamespacedName{Namespace: "alice", Name: "kueue-offload-executor"}, &appsv1.Deployment{}) {
		t.Errorf("executor Deployment removed")
	}
}

func TestUserMissingFromIdentityMapIgnored(t *testing.T) {
	e := newEnv(t, identityMap(v1alpha1.IdentityEntry{KubernetesUser: "alice", Site: testSite, HPCUser: "alice"}))
	e.sync()
	if e.get(types.NamespacedName{Name: "bob"}, &corev1.Namespace{}) {
		t.Errorf("namespace bob created without an identity")
	}
	nss := &corev1.NamespaceList{}
	if err := e.c.List(context.Background(), nss); err != nil {
		t.Fatal(err)
	}
	if len(nss.Items) != 1 || nss.Items[0].Name != "alice" {
		t.Errorf("namespaces %v, want alice only", nss.Items)
	}
	for k := range e.localQueues() {
		if !strings.HasPrefix(k, "alice/") {
			t.Errorf("LocalQueue %s outside alice", k)
		}
	}
	if e.executorConfigMap("bob") != nil {
		t.Errorf("executor rendered for bob")
	}
}

func TestNamespaceDroppedFromIdentityMapIsRevoked(t *testing.T) {
	e := newEnv(t)
	e.sync()
	im := &v1alpha1.IdentityMap{}
	e.get(types.NamespacedName{Name: "users"}, im)
	im.Spec.Entries = im.Spec.Entries[:1]
	if err := e.c.Update(context.Background(), im); err != nil {
		t.Fatal(err)
	}
	e.sync()
	ns := e.namespace("bob")
	if hasKey(ns.Labels, "hpc.interlink.eu/hpc-a-proj42") {
		t.Errorf("bob keeps proj42 after leaving the IdentityMap")
	}
	if _, ok := e.localQueues()["bob/hpc-a-proj42"]; ok {
		t.Errorf("bob keeps his LocalQueue")
	}
	if e.executorConfigMap("bob") == nil {
		t.Errorf("bob's executor removed")
	}
}

func TestOtherSlurmClusterIgnored(t *testing.T) {
	e := newEnv(t, assocConfigMap("hpc-a|proj42|alice\nhpc-b|proj55|alice\nhpc-b|proj42|bob\n"))
	e.sync()
	if _, ok := e.clusterQueues()["hpc-a-proj55"]; ok {
		t.Errorf("ClusterQueue for a row of another cluster")
	}
	if got := e.localQueues(); !reflect.DeepEqual(got, map[string]string{"alice/hpc-a-proj42": "hpc-a-proj42"}) {
		t.Errorf("LocalQueues %v", got)
	}
	if e.get(types.NamespacedName{Name: "bob"}, &corev1.Namespace{}) {
		t.Errorf("bob's row of another cluster created his namespace")
	}

	// Associations with no row of the site's cluster stop the sync instead
	// of revoking every membership.
	e.setAssociations("hpc-b|proj55|alice\n")
	if _, err := e.r.Sync(context.Background()); err == nil || !strings.Contains(err.Error(), "no user association") {
		t.Fatalf("sync without rows of the cluster: %v", err)
	}
	if e.namespace("alice").Labels["hpc.interlink.eu/hpc-a-proj42"] != "true" {
		t.Errorf("membership revoked from an empty association list")
	}
}

func TestTemplateRefusals(t *testing.T) {
	cm := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kueue-offload-executor\ndata:\n  accounts: {{ join \",\" .Accounts | quote }}\n"
	cases := map[string]string{
		"cluster scoped":  cm + "---\napiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata:\n  name: executor-{{ .Namespace }}\nrules: []\n",
		"other namespace": cm + "---\napiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: executor\n  namespace: kube-system\n",
		"secret":          cm + "---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: key\nstringData: {k: v}\n",
		"no name":         cm + "---\napiVersion: v1\nkind: ServiceAccount\nmetadata: {}\n",
		"unknown field":   "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Nope }}\n",
	}
	for name, tmpl := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, templateConfigMap(tmpl))
			_, err := e.r.Sync(context.Background())
			if err == nil {
				t.Fatal("sync accepted the template")
			}
			// Nothing of the template is applied, not even its valid objects.
			for _, ns := range []string{"alice", "bob"} {
				if e.executorConfigMap(ns) != nil {
					t.Errorf("ConfigMap applied in %s from a refused template", ns)
				}
			}
			if e.get(types.NamespacedName{Name: "executor-alice"}, &rbacv1.ClusterRole{}) {
				t.Errorf("cluster-scoped object applied")
			}
			if e.get(types.NamespacedName{Namespace: "kube-system", Name: "executor"}, &corev1.ServiceAccount{}) {
				t.Errorf("object applied in another namespace")
			}
			if e.secretCalls != 0 {
				t.Errorf("%d Secret calls", e.secretCalls)
			}
			refused := false
			for _, ev := range e.drainEvents() {
				refused = refused || strings.Contains(ev, EventTemplateRefused)
			}
			if !refused {
				t.Errorf("no %s event", EventTemplateRefused)
			}
			// Memberships do not depend on the template.
			if _, ok := e.localQueues()["alice/hpc-a-proj99"]; !ok {
				t.Errorf("LocalQueues not applied")
			}
		})
	}
}

func TestIdentityConflictSkipsNamespace(t *testing.T) {
	e := newEnv(t,
		assocConfigMap("hpc-a|proj42|alice\nhpc-a|proj99|alice2\nhpc-a|proj42|bob\n"),
		identityMap(
			v1alpha1.IdentityEntry{KubernetesUser: "alice", Site: testSite, HPCUser: "alice"},
			v1alpha1.IdentityEntry{KubernetesUser: "alice-admin", Namespace: "alice", Site: testSite, HPCUser: "alice2"},
			v1alpha1.IdentityEntry{KubernetesUser: "bob", Site: testSite, HPCUser: "bob"},
			v1alpha1.IdentityEntry{KubernetesUser: "root", Namespace: "kube-system", Site: testSite, HPCUser: "bob"},
			v1alpha1.IdentityEntry{KubernetesUser: "eve@example.org", Site: testSite, HPCUser: "bob"},
		))
	e.sync()
	if e.get(types.NamespacedName{Name: "alice"}, &corev1.Namespace{}) {
		t.Errorf("namespace mapped to two HPC users was set up")
	}
	if got := e.localQueues(); !reflect.DeepEqual(got, map[string]string{"bob/hpc-a-proj42": "hpc-a-proj42"}) {
		t.Errorf("LocalQueues %v", got)
	}
	var skipped []string
	for _, ev := range e.drainEvents() {
		if strings.Contains(ev, EventSkipped) {
			skipped = append(skipped, ev)
		}
	}
	if len(skipped) != 3 {
		t.Errorf("skipped events %v, want the conflict, the reserved and the invalid namespace", skipped)
	}
}

func TestConfigMapOutsideOwnNamespaceRefused(t *testing.T) {
	sc := offloadConfig()
	sc.Spec.Membership.Associations.Namespace = "alice"
	e := newEnv(t, sc, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "assoc", Namespace: "alice"},
		Data: map[string]string{"associations": fixture}})
	if _, err := e.r.Sync(context.Background()); err == nil {
		t.Fatal("associations read from a tenant namespace")
	}
	if len(e.clusterQueues()) != 0 || len(e.localQueues()) != 0 {
		t.Errorf("objects written from a refused source")
	}
}

func TestQueueNamesFollowAccountRule(t *testing.T) {
	sc := offloadConfig()
	sc.Spec.AccountRule.StripPrefix = new(string)
	sc.Spec.Membership.Cohort = "pool"
	sc.Spec.Membership.AdmissionCheck = "offload-a"
	e := newEnv(t, sc, assocConfigMap("hpc-a|proj42|alice\nhpc-a|Bad_Account|alice\n"))
	e.sync()
	cq, ok := e.clusterQueues()["proj42"]
	if !ok || cq.Spec.CohortName != "pool" || cq.Spec.AdmissionChecksStrategy.AdmissionChecks[0].Name != "offload-a" {
		t.Fatalf("ClusterQueues %v", e.clusterQueues())
	}
	if len(e.clusterQueues()) != 1 {
		t.Errorf("an account that is not a DNS label got a ClusterQueue")
	}
	if got := e.localQueues(); !reflect.DeepEqual(got, map[string]string{"alice/proj42": "proj42"}) {
		t.Errorf("LocalQueues %v", got)
	}
	// The label always names the site, whatever the queue prefix.
	if e.namespace("alice").Labels["hpc.interlink.eu/hpc-a-proj42"] != "true" {
		t.Errorf("project label missing")
	}
}

func TestParseAssociations(t *testing.T) {
	rows, bad := ParseAssociations("hpc-a|proj42|\r\nhpc-a|proj42|alice\n\nhpc-a|proj99|bob|\nbroken line\n|proj1|x\n")
	want := []Association{{"hpc-a", "proj42", "alice"}, {"hpc-a", "proj99", "bob"}}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("rows %v, want %v", rows, want)
	}
	if !reflect.DeepEqual(bad, []string{"broken line", "|proj1|x"}) {
		t.Errorf("bad %v", bad)
	}
}

func TestSampleTemplateRenders(t *testing.T) {
	tmpl, err := ParseTemplate(sampleTemplate(t))
	if err != nil {
		t.Fatal(err)
	}
	objs, err := renderObjects(tmpl, TemplateData{Namespace: "alice", Site: "hpc-a", HPCUser: "alice",
		Accounts: []string{"proj42", "proj99"}, Image: "executor:1", ExecutorService: "kueue-offload-executor", ExecutorPort: 8000})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, o := range objs {
		kinds = append(kinds, o.GetKind())
	}
	want := []string{"ServiceAccount", "RoleBinding", "ConfigMap", "Deployment", "Service", "NetworkPolicy"}
	if !reflect.DeepEqual(kinds, want) {
		t.Errorf("kinds %v, want %v", kinds, want)
	}
	e := newEnv(t)
	if err := e.r.prepareObjects(objs, site{name: "hpc-a"}, "alice"); err != nil {
		t.Errorf("sample template refused: %v", err)
	}
	for _, o := range objs {
		if o.GetKind() == "RoleBinding" {
			ref, _, _ := unstructured.NestedString(o.Object, "roleRef", "name")
			if ref != "kueue-offload-executor" {
				t.Errorf("RoleBinding points at %q", ref)
			}
		}
	}
}
