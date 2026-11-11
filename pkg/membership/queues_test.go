package membership

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

func (e *env) cohorts() map[string]kueue.Cohort {
	e.t.Helper()
	l := &kueue.CohortList{}
	if err := e.c.List(context.Background(), l); err != nil {
		e.t.Fatal(err)
	}
	out := map[string]kueue.Cohort{}
	for _, c := range l.Items {
		out[c.Name] = c
	}
	return out
}

// cohortNames names the Cohorts, sorted.
func (e *env) cohortNames() string {
	e.t.Helper()
	var names []string
	for n := range e.cohorts() {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

func (e *env) localQueueLabels(ns, name string) map[string]string {
	e.t.Helper()
	lq := &kueue.LocalQueue{}
	if !e.get(types.NamespacedName{Namespace: ns, Name: name}, lq) {
		e.t.Fatalf("LocalQueue %s/%s missing", ns, name)
	}
	return lq.Labels
}

// queueQuota has two groups: GPUs on the gpu and debug flavors, memory on the
// mem flavor.
func queueQuota(gpus int64) []kueue.ResourceGroup {
	return []kueue.ResourceGroup{
		{
			CoveredResources: []corev1.ResourceName{corev1.ResourceCPU, "nvidia.com/gpu"},
			Flavors: []kueue.FlavorQuotas{
				{Name: "hpc-a-gpu", Resources: []kueue.ResourceQuota{
					{Name: corev1.ResourceCPU, NominalQuota: resource.MustParse("64")},
					{Name: "nvidia.com/gpu", NominalQuota: *resource.NewQuantity(gpus, resource.DecimalSI), BorrowingLimit: ptr.To(resource.MustParse("0"))},
				}},
				{Name: "hpc-a-debug", Resources: []kueue.ResourceQuota{
					{Name: corev1.ResourceCPU, NominalQuota: resource.MustParse("8")},
					{Name: "nvidia.com/gpu", NominalQuota: resource.MustParse("1")},
				}},
			},
		},
		{
			CoveredResources: []corev1.ResourceName{corev1.ResourceMemory},
			Flavors: []kueue.FlavorQuotas{{Name: "hpc-a-mem", Resources: []kueue.ResourceQuota{
				{Name: corev1.ResourceMemory, NominalQuota: resource.MustParse("1Ti")},
			}}},
		},
	}
}

// queueConfig is the site with queues debug (the debug flavor) and gpu (the
// gpu and mem flavors).
func queueConfig() *v1alpha1.OffloadConfig {
	sc := offloadConfig()
	sc.Spec.Flavors = map[string]v1alpha1.FlavorTarget{"hpc-a-gpu": {}, "hpc-a-debug": {}, "hpc-a-mem": {}}
	sc.Spec.Membership.DefaultProjectQuota = queueQuota(8)
	sc.Spec.Membership.Queues = []v1alpha1.MembershipQueue{
		{Name: "debug", Flavors: []kueue.ResourceFlavorReference{"hpc-a-debug"}},
		{Name: "gpu", Flavors: []kueue.ResourceFlavorReference{"hpc-a-gpu", "hpc-a-mem"}},
	}
	return sc
}

func queueEnv(t *testing.T, objs ...client.Object) *env {
	t.Helper()
	base := []client.Object{queueConfig(), &v1alpha1.ProjectQuota{ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-proj42"},
		Spec: v1alpha1.ProjectQuotaSpec{Site: testSite, Account: "proj42", ResourceGroups: queueQuota(64)}}}
	return newEnv(t, append(base, objs...)...)
}

func (e *env) setConfig(edit func(m *v1alpha1.Membership)) {
	e.t.Helper()
	sc := &v1alpha1.OffloadConfig{}
	e.get(types.NamespacedName{Name: testSite}, sc)
	edit(sc.Spec.Membership)
	if err := e.c.Update(context.Background(), sc); err != nil {
		e.t.Fatal(err)
	}
}

func flavorNames(groups []kueue.ResourceGroup) [][]kueue.ResourceFlavorReference {
	var out [][]kueue.ResourceFlavorReference
	for _, g := range groups {
		var names []kueue.ResourceFlavorReference
		for _, f := range g.Flavors {
			names = append(names, f.Name)
		}
		out = append(out, names)
	}
	return out
}

// The project's quota is on its Cohort, under the site's, and never lent.
func TestProjectCohortHoldsTheQuota(t *testing.T) {
	e := queueEnv(t)
	e.sync()
	cohorts := e.cohorts()
	if got := e.cohortNames(); got != "hpc-a,hpc-a-proj42-project,hpc-a-proj99-project" {
		t.Fatalf("Cohorts %q", got)
	}
	site := cohorts["hpc-a"]
	if site.Spec.ParentName != "" || len(site.Spec.ResourceGroups) != 0 || site.Labels[AccountLabel] != "" {
		t.Errorf("site Cohort %+v", site)
	}
	for account, gpus := range map[string]int64{"proj42": 64, "proj99": 8} {
		c := cohorts["hpc-a-"+account+"-project"]
		if c.Spec.ParentName != "hpc-a" {
			t.Errorf("%s: parent %q, want the site's Cohort", account, c.Spec.ParentName)
		}
		if want := (map[string]string{ManagedByLabel: ManagedByValue, SiteLabel: testSite, AccountLabel: account}); !reflect.DeepEqual(c.Labels, want) {
			t.Errorf("%s: labels %v", account, c.Labels)
		}
		if g := gpuQuota(t, c); g != gpus {
			t.Errorf("%s: GPU quota %d, want %d", account, g, gpus)
		}
		want := queueQuota(gpus)
		for _, g := range want {
			for i := range g.Flavors {
				for j := range g.Flavors[i].Resources {
					g.Flavors[i].Resources[j].LendingLimit = ptr.To(resource.MustParse("0"))
				}
			}
		}
		if got := c.Spec.ResourceGroups; !resourceGroupsEqual(got, want) {
			t.Errorf("%s: resourceGroups %+v, want the quota with lendingLimit 0 everywhere and borrowingLimit kept: %+v", account, got, want)
		}
	}
}

// resourceGroupsEqual compares quantities by value.
func resourceGroupsEqual(a, b []kueue.ResourceGroup) bool {
	norm := func(groups []kueue.ResourceGroup) string {
		var sb strings.Builder
		for _, g := range groups {
			sb.WriteString("|")
			for _, r := range g.CoveredResources {
				sb.WriteString(string(r) + ",")
			}
			for _, f := range g.Flavors {
				sb.WriteString(" " + string(f.Name) + ":")
				for _, r := range f.Resources {
					q := func(v *resource.Quantity) string {
						if v == nil {
							return "-"
						}
						return v.String()
					}
					sb.WriteString(string(r.Name) + "=" + r.NominalQuota.String() + "/" + q(r.BorrowingLimit) + "/" + q(r.LendingLimit) + ";")
				}
			}
		}
		return sb.String()
	}
	return norm(a) == norm(b)
}

// Each queue's ClusterQueue has only its flavors, at nominal 0, and borrows
// from the project's Cohort.
func TestQueueClusterQueues(t *testing.T) {
	e := queueEnv(t)
	e.sync()
	if got := e.queueNames(); got != "hpc-a-proj42,hpc-a-proj42-debug,hpc-a-proj42-gpu,hpc-a-proj99,hpc-a-proj99-debug,hpc-a-proj99-gpu" {
		t.Fatalf("ClusterQueues %q", got)
	}
	want := map[string]struct {
		queue   string
		flavors [][]kueue.ResourceFlavorReference
	}{
		"hpc-a-proj42":       {"", [][]kueue.ResourceFlavorReference{{"hpc-a-gpu", "hpc-a-debug"}, {"hpc-a-mem"}}},
		"hpc-a-proj42-debug": {"debug", [][]kueue.ResourceFlavorReference{{"hpc-a-debug"}}},
		"hpc-a-proj42-gpu":   {"gpu", [][]kueue.ResourceFlavorReference{{"hpc-a-gpu"}, {"hpc-a-mem"}}},
	}
	cqs := e.clusterQueues()
	for name, w := range want {
		cq := cqs[name]
		if got := flavorNames(cq.Spec.ResourceGroups); !reflect.DeepEqual(got, w.flavors) {
			t.Errorf("%s: flavors %v, want %v", name, got, w.flavors)
		}
		for _, g := range cq.Spec.ResourceGroups {
			for _, f := range g.Flavors {
				for _, r := range f.Resources {
					if !r.NominalQuota.IsZero() || r.BorrowingLimit != nil || r.LendingLimit != nil {
						t.Errorf("%s: %s %s nominal %s borrowingLimit %v lendingLimit %v, want nominal 0 and no limits",
							name, f.Name, r.Name, r.NominalQuota.String(), r.BorrowingLimit, r.LendingLimit)
					}
				}
			}
		}
		labels := map[string]string{ManagedByLabel: ManagedByValue, SiteLabel: testSite, AccountLabel: "proj42"}
		if w.queue != "" {
			labels[QueueLabel] = w.queue
		}
		if !reflect.DeepEqual(cq.Labels, labels) {
			t.Errorf("%s: labels %v, want %v", name, cq.Labels, labels)
		}
		if cq.Spec.CohortName != "hpc-a-proj42-project" {
			t.Errorf("%s: cohort %q", name, cq.Spec.CohortName)
		}
		if got := cq.Spec.NamespaceSelector.MatchLabels; !reflect.DeepEqual(got, map[string]string{"hpc.interlink.eu/hpc-a-proj42": "true"}) {
			t.Errorf("%s: namespaceSelector %v", name, got)
		}
		if s := cq.Spec.AdmissionChecksStrategy; s == nil || len(s.AdmissionChecks) != 1 || s.AdmissionChecks[0].Name != "hpc-a" {
			t.Errorf("%s: admissionChecksStrategy %+v", name, s)
		}
		if cq.Spec.AdmissionScope != nil {
			t.Errorf("%s: admissionScope %+v without admissionFairSharing", name, cq.Spec.AdmissionScope)
		}
	}
}

// A project whose quota lacks some of a queue's flavors gets the queue with the
// groups it has. A queue none of whose flavors it has is skipped for that
// project alone, and the one it had before goes.
func TestQueueClusterQueueWithoutTheQueueFlavors(t *testing.T) {
	noDebug := queueQuota(8)
	noDebug[0].Flavors = noDebug[0].Flavors[:1]
	cases := map[string]struct {
		quota []kueue.ResourceGroup
		gpu   [][]kueue.ResourceFlavorReference
	}{
		"mem group only":  {queueQuota(8)[1:], [][]kueue.ResourceFlavorReference{{"hpc-a-mem"}}},
		"no debug flavor": {noDebug, [][]kueue.ResourceFlavorReference{{"hpc-a-gpu"}, {"hpc-a-mem"}}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			e := queueEnv(t, queuesOfTemplate())
			e.sync()
			if _, ok := e.clusterQueues()["hpc-a-proj99-debug"]; !ok {
				t.Fatal("proj99 has no queue debug on the default quota")
			}
			pq := &v1alpha1.ProjectQuota{ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-proj99"},
				Spec: v1alpha1.ProjectQuotaSpec{Site: testSite, Account: "proj99", ResourceGroups: c.quota}}
			if err := e.c.Create(context.Background(), pq); err != nil {
				t.Fatal(err)
			}
			e.drainEvents()
			e.sync()
			cqs := e.clusterQueues()
			if got := flavorNames(cqs["hpc-a-proj99-gpu"].Spec.ResourceGroups); !reflect.DeepEqual(got, c.gpu) {
				t.Errorf("proj99 gpu queue flavors %v, want %v", got, c.gpu)
			}
			if _, ok := cqs["hpc-a-proj99-debug"]; ok {
				t.Errorf("ClusterQueue hpc-a-proj99-debug kept, with no quota on its flavor")
			}
			if _, ok := cqs["hpc-a-proj42-debug"]; !ok {
				t.Errorf("proj42 lost queue debug")
			}
			if _, ok := e.localQueues()["alice/hpc-a-proj99-debug"]; ok {
				t.Errorf("LocalQueue alice/hpc-a-proj99-debug kept")
			}
			if got, want := e.renderedQueues("alice"), map[string][]string{"proj42": {"debug", "gpu"}, "proj99": {"gpu"}}; !reflect.DeepEqual(got, want) {
				t.Errorf("queuesOf %v, want %v", got, want)
			}
			want := `account "proj99" queue "debug": flavor hpc-a-debug has no quota in ProjectQuota hpc-a-proj99, the queue is skipped`
			if evs := e.drainEvents(); !hasEvent(evs, EventSkipped, want) {
				t.Errorf("no %s event with %q: %v", EventSkipped, want, evs)
			}
		})
	}
}

func TestAdmissionFairSharingScope(t *testing.T) {
	sc := queueConfig()
	sc.Spec.Membership.AdmissionFairSharing = true
	e := queueEnv(t, sc)
	e.sync()
	for name, cq := range e.clusterQueues() {
		if s := cq.Spec.AdmissionScope; s == nil || s.AdmissionMode != kueue.UsageBasedAdmissionFairSharing {
			t.Errorf("%s: admissionScope %+v, want UsageBasedAdmissionFairSharing", name, s)
		}
	}
	e.setConfig(func(m *v1alpha1.Membership) { m.AdmissionFairSharing = false })
	e.sync()
	for name, cq := range e.clusterQueues() {
		if cq.Spec.AdmissionScope != nil {
			t.Errorf("%s: admissionScope %+v kept after admissionFairSharing was turned off", name, cq.Spec.AdmissionScope)
		}
	}
}

func TestLocalQueuePerQueue(t *testing.T) {
	e := queueEnv(t)
	e.sync()
	want := map[string]string{}
	for ns, accounts := range map[string][]string{"alice": {"proj42", "proj99"}, "bob": {"proj42"}} {
		for _, a := range accounts {
			for _, q := range []string{"", "-debug", "-gpu"} {
				name := "hpc-a-" + a + q
				want[ns+"/"+name] = name
			}
		}
	}
	if got := e.localQueues(); !reflect.DeepEqual(got, want) {
		t.Fatalf("LocalQueues %v, want %v", got, want)
	}
	if l := e.localQueueLabels("bob", "hpc-a-proj42-debug"); l[QueueLabel] != "debug" || l[AccountLabel] != "proj42" || l[ManagedByLabel] != ManagedByValue {
		t.Errorf("labels of bob/hpc-a-proj42-debug %v", l)
	}
	if l := e.localQueueLabels("bob", "hpc-a-proj42"); hasKey(l, QueueLabel) || l[AccountLabel] != "proj42" {
		t.Errorf("labels of bob/hpc-a-proj42 %v", l)
	}
}

// The executor template gets the site's queue names in their order.
func TestTemplateGetsQueues(t *testing.T) {
	sc := queueConfig()
	slices.Reverse(sc.Spec.Membership.Queues)
	e := queueEnv(t, sc, templateConfigMap("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kueue-offload-executor\ndata:\n"+
		"  queues: {{ join \",\" .Queues | quote }}\n"+
		"  localQueues: \"{{ range $a := .Accounts }}{{ range $.Queues }}{{ $.Site }}-{{ $a }}-{{ . }} {{ end }}{{ end }}\"\n"))
	e.sync()
	cm := e.executorConfigMap("alice")
	if cm == nil {
		t.Fatal("executor ConfigMap missing")
	}
	if cm.Data["queues"] != "gpu,debug" {
		t.Errorf("queues %q, want the config order gpu,debug", cm.Data["queues"])
	}
	for _, name := range strings.Fields(cm.Data["localQueues"]) {
		if _, ok := e.localQueues()["alice/"+name]; !ok {
			t.Errorf("template names LocalQueue %s, which does not exist", name)
		}
	}
}

// queuesOfTemplate renders .Queues, .QueuesOf and, as <account>=<name>, the
// names a deployment's templates give their PodDefaults and LocalQueue
// references: <site>-<account> per account, <site>-<account>-<queue> per queue
// of .QueuesOf.
func queuesOfTemplate() *corev1.ConfigMap {
	return templateConfigMap("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kueue-offload-executor\ndata:\n" +
		"  queues: {{ join \",\" .Queues | quote }}\n" +
		"  queuesOf: {{ toJson .QueuesOf | quote }}\n" +
		"  names: \"{{ range $a := .Accounts }}{{ $a }}={{ $.Site }}-{{ $a }} " +
		"{{ range index $.QueuesOf $a }}{{ $a }}={{ $.Site }}-{{ $a }}-{{ . }} {{ end }}{{ end }}\"\n")
}

// renderedQueues returns the .QueuesOf queuesOfTemplate rendered in namespace
// ns, and fails the test if it rendered a name twice or a name that is not a
// LocalQueue of that account in ns.
func (e *env) renderedQueues(ns string) map[string][]string {
	e.t.Helper()
	cm := e.executorConfigMap(ns)
	if cm == nil {
		e.t.Fatalf("executor ConfigMap missing in %s", ns)
	}
	var queuesOf map[string][]string
	if err := json.Unmarshal([]byte(cm.Data["queuesOf"]), &queuesOf); err != nil {
		e.t.Fatalf("queuesOf %q: %v", cm.Data["queuesOf"], err)
	}
	seen := map[string]bool{}
	for _, f := range strings.Fields(cm.Data["names"]) {
		a, name, _ := strings.Cut(f, "=")
		if seen[name] {
			e.t.Errorf("%s: template renders %s twice", ns, name)
		}
		seen[name] = true
		lq := &kueue.LocalQueue{}
		if !e.get(types.NamespacedName{Namespace: ns, Name: name}, lq) || lq.Labels[AccountLabel] != a {
			e.t.Errorf("%s: template names LocalQueue %s for account %s, which has no such LocalQueue", ns, name, a)
		}
	}
	return queuesOf
}

// .QueuesOf leaves out, for one account, the queue whose name another account
// holds; .Queues still lists it.
func TestTemplateQueuesOfLeavesSkippedQueuesOut(t *testing.T) {
	sc := queueConfig()
	sc.Spec.Membership.Queues = nil
	e := queueEnv(t, sc, assocConfigMap("hpc-a|proj42|alice\nhpc-a|proj42-debug|alice\n"), queuesOfTemplate())
	e.sync()
	// proj42's queue debug would be hpc-a-proj42-debug, which proj42-debug's
	// queue of every flavor holds.
	e.setConfig(func(m *v1alpha1.Membership) {
		m.Queues = []v1alpha1.MembershipQueue{{Name: "debug", Flavors: []kueue.ResourceFlavorReference{"hpc-a-debug"}}}
	})
	e.sync()
	got := e.renderedQueues("alice")
	if q := e.executorConfigMap("alice").Data["queues"]; q != "debug" {
		t.Errorf("queues %q, want the site's debug", q)
	}
	if want := (map[string][]string{"proj42": {}, "proj42-debug": {"debug"}}); !reflect.DeepEqual(got, want) {
		t.Errorf("queuesOf %v, want %v: proj42's debug queue is skipped", got, want)
	}
}

func TestRemovedQueuePruned(t *testing.T) {
	e := queueEnv(t)
	e.sync()
	e.setConfig(func(m *v1alpha1.Membership) { m.Queues = m.Queues[:1] })
	e.sync()
	if got := e.queueNames(); got != "hpc-a-proj42,hpc-a-proj42-debug,hpc-a-proj99,hpc-a-proj99-debug" {
		t.Errorf("ClusterQueues %q once queue gpu is gone", got)
	}
	for k := range e.localQueues() {
		if strings.HasSuffix(k, "-gpu") {
			t.Errorf("LocalQueue %s kept", k)
		}
	}
	if _, ok := e.localQueues()["bob/hpc-a-proj42-debug"]; !ok {
		t.Errorf("LocalQueue bob/hpc-a-proj42-debug removed")
	}
	if got := e.cohortNames(); got != "hpc-a,hpc-a-proj42-project,hpc-a-proj99-project" {
		t.Errorf("Cohorts %q", got)
	}
}

func TestRemovedProjectPrunedWithCohort(t *testing.T) {
	e := queueEnv(t)
	e.sync()
	e.setAssociations(strings.Replace(fixture, "hpc-a|proj99|alice\n", "", 1))
	e.sync()
	if got := e.queueNames(); got != "hpc-a-proj42,hpc-a-proj42-debug,hpc-a-proj42-gpu" {
		t.Errorf("ClusterQueues %q once proj99 has no member", got)
	}
	if got := e.cohortNames(); got != "hpc-a,hpc-a-proj42-project" {
		t.Errorf("Cohorts %q once proj99 has no member", got)
	}
	for k := range e.localQueues() {
		if strings.Contains(k, "proj99") {
			t.Errorf("LocalQueue %s kept", k)
		}
	}
}

// Account proj42-debug renders hpc-a-proj42-debug, the name of proj42's queue
// debug: the queue of the account that does not hold the name is skipped.
func TestQueueNameCollisionSkipsTheQueue(t *testing.T) {
	collide := "hpc-a|proj42|alice\nhpc-a|proj42-debug|bob\n"

	// Between new accounts, every account's queue of every flavor gets its
	// name before any other queue: proj42's queue debug is skipped, not
	// proj42-debug's queue of every flavor, which templates name for every
	// account.
	t.Run("both new", func(t *testing.T) {
		cases := map[string]struct {
			assoc    string
			queuesOf map[string]map[string][]string
		}{
			"one namespace": {"hpc-a|proj42|alice\nhpc-a|proj42-debug|alice\n", map[string]map[string][]string{
				"alice": {"proj42": {"gpu"}, "proj42-debug": {"debug", "gpu"}}}},
			"two namespaces": {collide, map[string]map[string][]string{
				"alice": {"proj42": {"gpu"}}, "bob": {"proj42-debug": {"debug", "gpu"}}}},
		}
		for name, c := range cases {
			t.Run(name, func(t *testing.T) {
				e := queueEnv(t, assocConfigMap(c.assoc), queuesOfTemplate())
				e.sync()
				if l := e.clusterQueues()["hpc-a-proj42-debug"].Labels; l[AccountLabel] != "proj42-debug" || hasKey(l, QueueLabel) {
					t.Errorf("hpc-a-proj42-debug labels %v, want proj42-debug's queue of every flavor", l)
				}
				if got := e.queueNames(); got != "hpc-a-proj42,hpc-a-proj42-debug,hpc-a-proj42-debug-debug,hpc-a-proj42-debug-gpu,hpc-a-proj42-gpu" {
					t.Errorf("ClusterQueues %q", got)
				}
				for ns, want := range c.queuesOf {
					if got := e.renderedQueues(ns); !reflect.DeepEqual(got, want) {
						t.Errorf("%s: queuesOf %v, want %v", ns, got, want)
					}
				}
				if !hasEvent(e.drainEvents(), EventSkipped, `account "proj42" queue "debug": name hpc-a-proj42-debug is account proj42-debug's, the queue is skipped`) {
					t.Errorf("no Warning event for the skipped queue")
				}
			})
		}
	})

	// proj42's queue debug holds hpc-a-proj42-debug, the name of the queue of
	// every flavor of the new account proj42-debug: proj42-debug is left out.
	t.Run("taken name of the queue of every flavor skips the account", func(t *testing.T) {
		e := queueEnv(t, assocConfigMap("hpc-a|proj42|alice\n"), queuesOfTemplate())
		e.sync()
		e.setAssociations("hpc-a|proj42|alice\nhpc-a|proj42-debug|alice\nhpc-a|proj42-debug|bob\n")
		e.drainEvents()
		e.sync()
		if l := e.clusterQueues()["hpc-a-proj42-debug"].Labels; l[AccountLabel] != "proj42" || l[QueueLabel] != "debug" {
			t.Errorf("hpc-a-proj42-debug labels %v, want still proj42's queue debug", l)
		}
		if got := e.queueNames(); got != "hpc-a-proj42,hpc-a-proj42-debug,hpc-a-proj42-gpu" {
			t.Errorf("ClusterQueues %q, want proj42's only", got)
		}
		if got := e.cohortNames(); got != "hpc-a,hpc-a-proj42-project" {
			t.Errorf("Cohorts %q, want none for proj42-debug", got)
		}
		if e.get(types.NamespacedName{Name: "bob"}, &corev1.Namespace{}) {
			t.Errorf("namespace of the skipped account set up")
		}
		if got := e.namespace("alice").Annotations[AccountsAnnotation(testSite)]; got != "proj42" {
			t.Errorf("alice's accounts %q, want proj42", got)
		}
		if got, want := e.renderedQueues("alice"), map[string][]string{"proj42": {"debug", "gpu"}}; !reflect.DeepEqual(got, want) {
			t.Errorf("queuesOf %v, want %v", got, want)
		}
		if !hasEvent(e.drainEvents(), EventSkipped, `account "proj42-debug": name hpc-a-proj42-debug is account proj42's`) {
			t.Errorf("no Warning event for the skipped account")
		}
	})

	// The review's case: both accounts hold their names, then queue debug
	// is added and proj42's ClusterQueue for it would be proj42-debug's.
	t.Run("owner keeps its name", func(t *testing.T) {
		sc := queueConfig()
		sc.Spec.Membership.Queues = nil
		e := queueEnv(t, sc, assocConfigMap(collide))
		e.sync()
		e.setConfig(func(m *v1alpha1.Membership) {
			m.Queues = []v1alpha1.MembershipQueue{{Name: "debug", Flavors: []kueue.ResourceFlavorReference{"hpc-a-debug"}}}
		})
		e.drainEvents()
		e.sync()
		cq := e.clusterQueues()["hpc-a-proj42-debug"]
		if cq.Labels[AccountLabel] != "proj42-debug" || hasKey(cq.Labels, QueueLabel) || cq.Spec.CohortName != "hpc-a-proj42-debug-project" {
			t.Errorf("hpc-a-proj42-debug labels %v cohort %q, want still proj42-debug's queue of every flavor", cq.Labels, cq.Spec.CohortName)
		}
		want := map[string]string{"alice/hpc-a-proj42": "hpc-a-proj42", "bob/hpc-a-proj42-debug": "hpc-a-proj42-debug",
			"bob/hpc-a-proj42-debug-debug": "hpc-a-proj42-debug-debug"}
		if got := e.localQueues(); !reflect.DeepEqual(got, want) {
			t.Errorf("LocalQueues %v, want %v", got, want)
		}
		if got := e.queueNames(); got != "hpc-a-proj42,hpc-a-proj42-debug,hpc-a-proj42-debug-debug" {
			t.Errorf("ClusterQueues %q", got)
		}
		if !hasEvent(e.drainEvents(), EventSkipped, `account "proj42" queue "debug": name hpc-a-proj42-debug is account proj42-debug's, the queue is skipped`) {
			t.Errorf("no Warning event for the skipped queue")
		}
	})

	// A Cohort labelled with proj99 holds the name of proj42's Cohort, which
	// all of proj42's queues need: proj42 is left out.
	t.Run("taken Cohort name skips the account", func(t *testing.T) {
		e := queueEnv(t, &kueue.Cohort{ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-proj42-project", Labels: accountLabels(testSite, "proj99")}})
		e.sync()
		if got := e.queueNames(); got != "hpc-a-proj99,hpc-a-proj99-debug,hpc-a-proj99-gpu" {
			t.Errorf("ClusterQueues %q, want proj99's only", got)
		}
		if got := e.namespace("alice").Annotations[AccountsAnnotation(testSite)]; got != "proj99" {
			t.Errorf("alice's accounts %q, want proj99", got)
		}
		if e.get(types.NamespacedName{Name: "bob"}, &corev1.Namespace{}) {
			t.Errorf("namespace of the skipped account set up")
		}
		if !hasEvent(e.drainEvents(), EventSkipped, `account "proj42": name hpc-a-proj42-project is account proj99's`) {
			t.Errorf("no Warning event for the skipped account")
		}
	})
}

// Kueue keeps Cohort and ClusterQueue names apart, so a Cohort and a
// ClusterQueue can share a name: account x's Cohort and account x-project's
// queue of every flavor are both hpc-a-x-project, and every account keeps
// its memberships.
func TestCohortAndClusterQueueShareAName(t *testing.T) {
	pair := "hpc-a|x|alice\nhpc-a|x-project|bob\n"
	check := func(t *testing.T, e *env) {
		t.Helper()
		for ns, want := range map[string]string{"alice": "x", "bob": "x-project"} {
			if got := e.namespace(ns).Annotations[AccountsAnnotation(testSite)]; got != want {
				t.Errorf("%s's accounts %q, want %q", ns, got, want)
			}
		}
		want := map[string]string{"alice/hpc-a-x": "hpc-a-x", "bob/hpc-a-x-project": "hpc-a-x-project"}
		if got := e.localQueues(); !reflect.DeepEqual(got, want) {
			t.Errorf("LocalQueues %v, want %v", got, want)
		}
		cqs := e.clusterQueues()
		if cq := cqs["hpc-a-x"]; cq.Labels[AccountLabel] != "x" || cq.Spec.CohortName != "hpc-a-x-project" {
			t.Errorf("hpc-a-x labels %v cohort %q, want x's in Cohort hpc-a-x-project", cq.Labels, cq.Spec.CohortName)
		}
		if cq := cqs["hpc-a-x-project"]; cq.Labels[AccountLabel] != "x-project" || cq.Spec.CohortName != "hpc-a-x-project-project" {
			t.Errorf("hpc-a-x-project labels %v cohort %q, want x-project's in Cohort hpc-a-x-project-project", cq.Labels, cq.Spec.CohortName)
		}
		if got := e.cohortNames(); got != "hpc-a,hpc-a-x-project,hpc-a-x-project-project" {
			t.Errorf("Cohorts %q", got)
		}
		if l := e.cohorts()["hpc-a-x-project"].Labels; l[AccountLabel] != "x" {
			t.Errorf("Cohort hpc-a-x-project labels %v, want x's", l)
		}
		if evs := e.drainEvents(); hasEvent(evs, EventSkipped, "") {
			t.Errorf("Warning events %v, want none", evs)
		}
	}

	t.Run("both new", func(t *testing.T) {
		e := newEnv(t, assocConfigMap(pair))
		e.sync()
		check(t, e)
	})

	// The queues of every flavor of x and x-project, as a controller made
	// them before project Cohorts, with alice already a member of x.
	t.Run("upgrade revokes nothing", func(t *testing.T) {
		cq := func(name, account string) *kueue.ClusterQueue {
			return &kueue.ClusterQueue{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: accountLabels(testSite, account)},
				Spec: kueue.ClusterQueueSpec{CohortName: "hpc-a"}}
		}
		alice := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "alice",
			Labels:      map[string]string{ProjectLabel(testSite, "x"): "true", TenantLabel: "true"},
			Annotations: map[string]string{AccountsAnnotation(testSite): "x", UserAnnotation(testSite): "alice"}}}
		e := newEnv(t, assocConfigMap(pair), cq("hpc-a-x", "x"), cq("hpc-a-x-project", "x-project"), alice,
			&kueue.LocalQueue{ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-x", Namespace: "alice", Labels: queueLabels(testSite, "x", "")},
				Spec: kueue.LocalQueueSpec{ClusterQueue: "hpc-a-x"}})
		e.sync()
		if a := e.namespace("alice").Annotations; a[AccountsAnnotation(testSite)] != "x" || hasKey(a, LeftAnnotation(testSite)) {
			t.Errorf("alice's annotations %v after the first pass, want x kept", a)
		}
		e.sync()
		check(t, e)
	})

	t.Run("site's Cohort named like a ClusterQueue", func(t *testing.T) {
		sc := offloadConfig()
		sc.Spec.Membership.Cohort = "hpc-a-proj42"
		e := newEnv(t, sc)
		e.sync()
		for ns, want := range map[string]string{"alice": "proj42,proj99", "bob": "proj42"} {
			if got := e.namespace(ns).Annotations[AccountsAnnotation(testSite)]; got != want {
				t.Errorf("%s's accounts %q, want %q", ns, got, want)
			}
		}
		if cq := e.clusterQueues()["hpc-a-proj42"]; cq.Labels[AccountLabel] != "proj42" || cq.Spec.CohortName != "hpc-a-proj42-project" {
			t.Errorf("hpc-a-proj42 labels %v cohort %q, want proj42's", cq.Labels, cq.Spec.CohortName)
		}
		if c := e.cohorts()["hpc-a-proj42-project"]; c.Spec.ParentName != "hpc-a-proj42" {
			t.Errorf("Cohort hpc-a-proj42-project parent %q, want the site's Cohort hpc-a-proj42", c.Spec.ParentName)
		}
		if evs := e.drainEvents(); hasEvent(evs, EventSkipped, `account "proj42"`) {
			t.Errorf("proj42 skipped: %v", evs)
		}
	})
}

// A queue whose name is too long for one account is skipped for that account
// alone, which keeps its other queues; accounts with shorter names get it.
func TestQueueNameTooLongSkipsTheQueue(t *testing.T) {
	long := "long" + strings.Repeat("q", 46) // hpc-a-proj42-<long> is 63 characters
	e := queueEnv(t, assocConfigMap(fixture+"hpc-a|proj4242|bob\n"),
		templateConfigMap("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kueue-offload-executor\ndata:\n"+
			"  queuesOf: {{ toJson .QueuesOf | quote }}\n"))
	e.sync()
	e.setConfig(func(m *v1alpha1.Membership) {
		m.Queues = append(m.Queues, v1alpha1.MembershipQueue{Name: long, Flavors: []kueue.ResourceFlavorReference{"hpc-a-gpu"}})
	})
	e.drainEvents()
	e.sync()
	want := map[string]string{}
	for ns, accounts := range map[string][]string{"alice": {"proj42", "proj99"}, "bob": {"proj42", "proj4242"}} {
		for _, a := range accounts {
			for _, q := range []string{"", "-debug", "-gpu", "-" + long} {
				if name := "hpc-a-" + a + q; name != "hpc-a-proj4242-"+long {
					want[ns+"/"+name] = name
				}
			}
		}
	}
	if got := e.localQueues(); !reflect.DeepEqual(got, want) {
		t.Errorf("LocalQueues %v, want %v", got, want)
	}
	cqs := e.clusterQueues()
	if _, ok := cqs["hpc-a-proj4242-gpu"]; !ok {
		t.Errorf("proj4242 lost its other queues")
	}
	if _, ok := cqs["hpc-a-proj42-"+long]; !ok {
		t.Errorf("proj42 did not get queue %s", long)
	}
	if got := e.namespace("bob").Annotations[AccountsAnnotation(testSite)]; got != "proj42,proj4242" {
		t.Errorf("bob's accounts %q, want proj4242 kept", got)
	}
	cm := e.executorConfigMap("bob")
	if cm == nil {
		t.Fatal("executor ConfigMap missing")
	}
	var queuesOf map[string][]string
	if err := json.Unmarshal([]byte(cm.Data["queuesOf"]), &queuesOf); err != nil {
		t.Fatalf("queuesOf %q: %v", cm.Data["queuesOf"], err)
	}
	if want := (map[string][]string{"proj42": {"debug", "gpu", long}, "proj4242": {"debug", "gpu"}}); !reflect.DeepEqual(queuesOf, want) {
		t.Errorf("bob's queuesOf %v, want %v", queuesOf, want)
	}
	evs := e.drainEvents()
	if !hasEvent(evs, EventSkipped, `account "proj4242" queue "`+long+`": name hpc-a-proj4242-`+long+` is longer than 63 characters, the queue is skipped`) {
		t.Errorf("no Warning event for the skipped queue: %v", evs)
	}
	if hasEvent(evs, EventSkipped, `account "proj4242": `) {
		t.Errorf("account proj4242 skipped: %v", evs)
	}
}

// An account whose Cohort name is too long and that holds no ClusterQueue or
// Cohort is skipped, whether its <site>-<account> fits or not, and the rest of
// the pass goes on, a revocation included.
func TestCohortNameTooLongSkipsTheAccount(t *testing.T) {
	for name, long := range map[string]string{
		"queue of every flavor fits": "proj" + strings.Repeat("9", 48), // hpc-a-<long> is 58 characters
		"no name fits":               "proj" + strings.Repeat("9", 56), // hpc-a-<long> is 66 characters
	} {
		t.Run(name, func(t *testing.T) {
			e := queueEnv(t)
			e.sync()
			e.setAssociations(strings.Replace(fixture, "hpc-a|proj99|alice\n", "", 1) + "hpc-a|" + long + "|bob\n")
			e.drainEvents()
			if _, err := e.r.Sync(context.Background()); err != nil {
				t.Fatalf("sync: %v", err)
			}
			for ns, want := range map[string]string{"alice": "proj42", "bob": "proj42"} {
				if got := e.namespace(ns).Annotations[AccountsAnnotation(testSite)]; got != want {
					t.Errorf("%s's accounts %q, want %q", ns, got, want)
				}
			}
			for k := range e.localQueues() {
				if strings.Contains(k, "proj99") || strings.Contains(k, long) {
					t.Errorf("LocalQueue %s", k)
				}
			}
			if got := e.queueNames(); got != "hpc-a-proj42,hpc-a-proj42-debug,hpc-a-proj42-gpu" {
				t.Errorf("ClusterQueues %q, want proj99 revoked and nothing for the long account", got)
			}
			want := `account "` + long + `": Cohort name hpc-a-` + long + `-project is longer than 63 characters`
			if evs := e.drainEvents(); !hasEvent(evs, EventSkipped, want) {
				t.Errorf("no %s event with %q: %v", EventSkipped, want, evs)
			}
		})
	}
}

// A project Cohort cannot be the site's Cohort, its own parent: an account
// whose Cohort would be named membership.cohort is skipped when it holds
// nothing, and stops the pass, revoking nothing, when an earlier controller
// gave it queues.
func TestCohortNamedLikeTheSiteCohort(t *testing.T) {
	cq := func(name, a, cohort string) *kueue.ClusterQueue {
		return &kueue.ClusterQueue{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: accountLabels(testSite, a)},
			Spec: kueue.ClusterQueueSpec{CohortName: kueue.CohortReference(cohort)}}
	}
	lq := func(ns, name, a string) *kueue.LocalQueue {
		return &kueue.LocalQueue{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: accountLabels(testSite, a)},
			Spec: kueue.LocalQueueSpec{ClusterQueue: kueue.ClusterQueueReference(name)}}
	}
	member := func(n, accts string, labels ...string) *corev1.Namespace {
		l := map[string]string{TenantLabel: "true"}
		for _, a := range labels {
			l[ProjectLabel(testSite, a)] = "true"
		}
		return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n, Labels: l,
			Annotations: map[string]string{AccountsAnnotation(testSite): accts, UserAnnotation(testSite): n}}}
	}
	want := `account "proj42": Cohort name hpc-a-proj42-project is the site's Cohort`
	t.Run("never granted", func(t *testing.T) {
		sc := offloadConfig()
		sc.Spec.Membership.Cohort = "hpc-a-proj42-project"
		e := newEnv(t, sc)
		if _, err := e.r.Sync(context.Background()); err != nil {
			t.Fatalf("sync: %v", err)
		}
		if got := e.namespace("alice").Annotations[AccountsAnnotation(testSite)]; got != "proj99" {
			t.Errorf("alice's accounts %q, want proj99", got)
		}
		if evs := e.drainEvents(); !hasEvent(evs, EventSkipped, want) {
			t.Errorf("no %s event with %q: %v", EventSkipped, want, evs)
		}
	})
	t.Run("granted before project Cohorts", func(t *testing.T) {
		sc := offloadConfig()
		sc.Spec.Membership.Cohort = "hpc-a-proj42-project"
		e := newEnv(t, sc, cq("hpc-a-proj42", "proj42", "hpc-a-proj42-project"), cq("hpc-a-proj99", "proj99", "hpc-a-proj42-project"),
			member("alice", "proj42,proj99", "proj42", "proj99"), member("bob", "proj42", "proj42"),
			lq("alice", "hpc-a-proj42", "proj42"), lq("alice", "hpc-a-proj99", "proj99"), lq("bob", "hpc-a-proj42", "proj42"))
		lqs, cqs := e.localQueues(), e.queueNames()
		_, err := e.r.Sync(context.Background())
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("sync: %v, want an error with %q", err, want)
		}
		if got := e.localQueues(); !reflect.DeepEqual(got, lqs) {
			t.Errorf("LocalQueues %v, want %v unchanged", got, lqs)
		}
		if got := e.queueNames(); got != cqs {
			t.Errorf("ClusterQueues %q, want %q unchanged", got, cqs)
		}
		for ns, accts := range map[string]string{"alice": "proj42,proj99", "bob": "proj42"} {
			if got := e.namespace(ns).Annotations[AccountsAnnotation(testSite)]; got != accts {
				t.Errorf("%s's accounts %q, want %q unchanged", ns, got, accts)
			}
		}
	})
}

// An account whose Cohort name is too long, but whose <site>-<account> fits,
// may hold queues from before project Cohorts: when the site has a ClusterQueue
// of it, the pass stops and revokes nothing, not even a membership that
// disappeared.
func TestCohortNameTooLongStopsTheSync(t *testing.T) {
	long := "proj" + strings.Repeat("9", 48) // hpc-a-<long> is 58 characters, its Cohort name 66
	e := queueEnv(t)
	e.sync()
	// What an earlier controller granted bob for the account.
	ns := e.namespace("bob")
	ns.Labels[ProjectLabel(testSite, long)] = "true"
	ns.Annotations[AccountsAnnotation(testSite)] = "proj42," + long
	if err := e.c.Update(context.Background(), ns); err != nil {
		t.Fatal(err)
	}
	for _, o := range []client.Object{
		&kueue.ClusterQueue{ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-" + long, Labels: accountLabels(testSite, long)},
			Spec: kueue.ClusterQueueSpec{CohortName: "hpc-a"}},
		&kueue.LocalQueue{ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-" + long, Namespace: "bob", Labels: queueLabels(testSite, long, "")},
			Spec: kueue.LocalQueueSpec{ClusterQueue: kueue.ClusterQueueReference("hpc-a-" + long)}},
	} {
		if err := e.c.Create(context.Background(), o); err != nil {
			t.Fatal(err)
		}
	}
	lqs, cqs := e.localQueues(), e.queueNames()
	e.setAssociations(strings.Replace(fixture, "hpc-a|proj99|alice\n", "", 1) + "hpc-a|" + long + "|bob\n")
	e.drainEvents()
	_, err := e.r.Sync(context.Background())
	want := `account "` + long + `": Cohort name hpc-a-` + long + `-project is longer than 63 characters`
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("sync: %v, want an error with %q", err, want)
	}
	if !hasEvent(e.drainEvents(), EventSyncFailed, want) {
		t.Errorf("no %s event", EventSyncFailed)
	}
	if got := e.localQueues(); !reflect.DeepEqual(got, lqs) {
		t.Errorf("LocalQueues %v, want %v unchanged", got, lqs)
	}
	if got := e.queueNames(); got != cqs {
		t.Errorf("ClusterQueues %q, want %q unchanged", got, cqs)
	}
	for ns, want := range map[string]string{"alice": "proj42,proj99", "bob": "proj42," + long} {
		if got := e.namespace(ns).Annotations[AccountsAnnotation(testSite)]; got != want {
			t.Errorf("%s's accounts %q, want %q unchanged", ns, got, want)
		}
	}
}

func hasEvent(events []string, reason, text string) bool {
	for _, ev := range events {
		if strings.Contains(ev, "Warning") && strings.Contains(ev, reason) && strings.Contains(ev, text) {
			return true
		}
	}
	return false
}

// A queue whose name is invalid, reserved or listed twice, or whose flavors
// are missing, listed twice or not in flavors, refuses the whole OffloadConfig.
func TestInvalidQueuesRefused(t *testing.T) {
	flavor := func(names ...kueue.ResourceFlavorReference) []kueue.ResourceFlavorReference { return names }
	cases := map[string]struct {
		queues []v1alpha1.MembershipQueue
		want   string
	}{
		"reserved all":      {[]v1alpha1.MembershipQueue{{Name: "all", Flavors: flavor("hpc-a-gpu")}}, "reserved"},
		"reserved project":  {[]v1alpha1.MembershipQueue{{Name: "project", Flavors: flavor("hpc-a-gpu")}}, "reserved"},
		"not a DNS label":   {[]v1alpha1.MembershipQueue{{Name: "Debug", Flavors: flavor("hpc-a-gpu")}}, "RFC 1123"},
		"listed twice":      {[]v1alpha1.MembershipQueue{{Name: "a", Flavors: flavor("hpc-a-gpu")}, {Name: "a", Flavors: flavor("hpc-a-debug")}}, "listed twice"},
		"no flavor":         {[]v1alpha1.MembershipQueue{{Name: "a"}}, "no flavor"},
		"flavor twice":      {[]v1alpha1.MembershipQueue{{Name: "a", Flavors: flavor("hpc-a-gpu", "hpc-a-gpu")}}, "listed twice"},
		"flavor not listed": {[]v1alpha1.MembershipQueue{{Name: "a", Flavors: flavor("hpc-a-nope")}}, "is not in flavors"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			sc := queueConfig()
			sc.Spec.Membership.Queues = c.queues
			e := queueEnv(t, sc)
			_, err := e.r.Sync(context.Background())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("sync: %v, want an error with %q", err, c.want)
			}
			if len(e.clusterQueues()) != 0 || len(e.localQueues()) != 0 || e.cohortNames() != "" {
				t.Errorf("objects written from a refused OffloadConfig")
			}
			if !hasEvent(e.drainEvents(), EventSyncFailed, c.want) {
				t.Errorf("no %s event", EventSyncFailed)
			}
		})
	}

	// A flavor with quota in a ProjectQuota only is the site's quota too.
	sc := queueConfig()
	sc.Spec.Membership.DefaultProjectQuota = quota(8)
	e := queueEnv(t, sc)
	e.sync()
	if _, ok := e.clusterQueues()["hpc-a-proj42-debug"]; !ok {
		t.Errorf("queue of a flavor only a ProjectQuota has refused")
	}
}

// Once the ProjectQuota that held the only quota of the debug and mem flavors
// is gone, queue debug, none of whose flavors has quota, is skipped and queue
// gpu admits on its gpu flavor; the rest of the site syncs, a revocation
// included.
func TestQueueWithoutQuotaSkipped(t *testing.T) {
	sc := queueConfig()
	sc.Spec.Membership.DefaultProjectQuota = quota(8)
	e := queueEnv(t, sc)
	e.sync()
	if _, ok := e.localQueues()["alice/hpc-a-proj42-debug"]; !ok {
		t.Fatal("queue debug not rendered while a ProjectQuota has its flavor")
	}
	pq := &v1alpha1.ProjectQuota{}
	e.get(types.NamespacedName{Name: "hpc-a-proj42"}, pq)
	if err := e.c.Delete(context.Background(), pq); err != nil {
		t.Fatal(err)
	}
	e.setAssociations(strings.Replace(fixture, "hpc-a|proj99|alice\n", "", 1))
	e.drainEvents()
	if _, err := e.r.Sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if got := e.queueNames(); got != "hpc-a-proj42,hpc-a-proj42-gpu" {
		t.Errorf("ClusterQueues %q, want queue debug skipped and proj99 revoked", got)
	}
	if got := flavorNames(e.clusterQueues()["hpc-a-proj42-gpu"].Spec.ResourceGroups); !reflect.DeepEqual(got, [][]kueue.ResourceFlavorReference{{"hpc-a-gpu"}}) {
		t.Errorf("hpc-a-proj42-gpu flavors %v", got)
	}
	want := map[string]string{"alice/hpc-a-proj42": "hpc-a-proj42", "alice/hpc-a-proj42-gpu": "hpc-a-proj42-gpu",
		"bob/hpc-a-proj42": "hpc-a-proj42", "bob/hpc-a-proj42-gpu": "hpc-a-proj42-gpu"}
	if got := e.localQueues(); !reflect.DeepEqual(got, want) {
		t.Errorf("LocalQueues %v, want %v", got, want)
	}
	if got := e.namespace("alice").Annotations[AccountsAnnotation(testSite)]; got != "proj42" {
		t.Errorf("alice's accounts %q, want proj99 revoked", got)
	}
	evs := e.drainEvents()
	if !hasEvent(evs, EventSkipped, `membership.queues "debug": flavor hpc-a-debug has no quota in defaultProjectQuota or a ProjectQuota of the site, the queue is skipped`) {
		t.Errorf("no Warning event for the skipped queue: %v", evs)
	}
	if !hasEvent(evs, EventSkipped, `membership.queues "gpu": flavor hpc-a-mem has no quota in defaultProjectQuota or a ProjectQuota of the site, the queue admits on its other flavors`) {
		t.Errorf("no Warning event for the flavor without quota: %v", evs)
	}
}

// A project's Cohort stays while Kueue keeps one of its ClusterQueues under
// its finalizer for the Workloads it admitted.
func TestProjectCohortWaitsForItsClusterQueues(t *testing.T) {
	e := queueEnv(t)
	e.sync()
	proj99 := []string{"hpc-a-proj99", "hpc-a-proj99-debug", "hpc-a-proj99-gpu"}
	setFinalizers := func(fin []string) {
		t.Helper()
		for _, name := range proj99 {
			cq := &kueue.ClusterQueue{}
			if !e.get(types.NamespacedName{Name: name}, cq) {
				t.Fatalf("ClusterQueue %s missing", name)
			}
			cq.Finalizers = fin
			if err := e.c.Update(context.Background(), cq); err != nil {
				t.Fatal(err)
			}
		}
	}
	setFinalizers([]string{kueue.ResourceInUseFinalizerName})
	e.setAssociations(strings.Replace(fixture, "hpc-a|proj99|alice\n", "", 1))
	for range 2 {
		e.sync()
		for _, name := range proj99 {
			if cq := e.clusterQueues()[name]; cq.DeletionTimestamp.IsZero() {
				t.Fatalf("ClusterQueue %s not deleted", name)
			}
		}
		if c, ok := e.cohorts()["hpc-a-proj99-project"]; !ok || !c.DeletionTimestamp.IsZero() {
			t.Fatalf("Cohort hpc-a-proj99-project deleted while its ClusterQueues are left")
		}
	}
	setFinalizers(nil)
	e.sync()
	if got := e.cohortNames(); got != "hpc-a,hpc-a-proj42-project" {
		t.Errorf("Cohorts %q once proj99's ClusterQueues are gone", got)
	}
}

// Objects the executor template no longer renders in a namespace, those of a
// dropped queue and of a revoked account, are deleted; objects without the
// site's labels are not.
func TestObjectsNoLongerRenderedDeleted(t *testing.T) {
	const text = `apiVersion: v1
kind: ConfigMap
metadata:
  name: kueue-offload-executor
data: {user: {{ quote .HPCUser }}}
{{- range $a := .Accounts }}{{ range $.Queues }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ $.Site }}-{{ $a }}-{{ . }}
{{- end }}{{ end }}
`
	sa := func(name string, labels map[string]string) *corev1.ServiceAccount {
		return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "alice", Labels: labels}}
	}
	e := queueEnv(t, templateConfigMap(text),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "alice"}},
		sa("users-own", nil),
		sa("hpc-a-proj99-x", map[string]string{ManagedByLabel: ManagedByValue}),
		sa("hpc-b-proj99-debug", map[string]string{ManagedByLabel: ManagedByValue, SiteLabel: "hpc-b"}))
	names := func(ns string) string {
		t.Helper()
		l := &corev1.ServiceAccountList{}
		if err := e.c.List(context.Background(), l, client.InNamespace(ns)); err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, a := range l.Items {
			out = append(out, a.Name)
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	e.sync()
	if got := names("alice"); got != "hpc-a-proj42-debug,hpc-a-proj42-gpu,hpc-a-proj99-debug,hpc-a-proj99-gpu,hpc-a-proj99-x,hpc-b-proj99-debug,users-own" {
		t.Fatalf("alice's ServiceAccounts %q", got)
	}
	e.setConfig(func(m *v1alpha1.Membership) { m.Queues = m.Queues[1:] })
	e.setAssociations(strings.Replace(fixture, "hpc-a|proj99|alice\n", "", 1))
	e.sync()
	if got := names("alice"); got != "hpc-a-proj42-gpu,hpc-a-proj99-x,hpc-b-proj99-debug,users-own" {
		t.Errorf("alice's ServiceAccounts %q, want those of queue debug and of proj99 deleted, the others kept", got)
	}
	if got := names("bob"); got != "hpc-a-proj42-gpu" {
		t.Errorf("bob's ServiceAccounts %q", got)
	}
	if e.executorConfigMap("alice") == nil {
		t.Errorf("executor ConfigMap deleted")
	}
}

func associationsConfigMap(name, ns, site, text string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{AssociationsLabel: site}},
		Data:       map[string]string{AssociationsKey: text},
	}
}

func TestExtraAssociationsConfigMapsMerged(t *testing.T) {
	e := newEnv(t,
		associationsConfigMap("extra", ownNS, testSite, "hpc-a|proj55|bob\nhpc-a|proj42|bob\n"),
		associationsConfigMap("other-site", ownNS, "hpc-b", "hpc-a|proj66|bob\n"),
		associationsConfigMap("elsewhere", "alice", testSite, "hpc-a|proj77|bob\n"),
	)
	e.sync()
	if got := e.namespace("bob").Annotations[AccountsAnnotation(testSite)]; got != "proj42,proj55" {
		t.Errorf("bob's accounts %q, want proj42 and proj55 from the labelled ConfigMap", got)
	}
	if got := e.queueNames(); got != "hpc-a-proj42,hpc-a-proj55,hpc-a-proj99" {
		t.Errorf("ClusterQueues %q", got)
	}
	s, err := resolveSite(offloadConfig())
	if err != nil {
		t.Fatal(err)
	}
	rows, _, err := e.r.associations(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range rows {
		if r == (Association{"hpc-a", "proj42", "bob"}) {
			n++
		}
	}
	if n != 1 {
		t.Errorf("row hpc-a|proj42|bob read %d times, want once", n)
	}

	// A labelled ConfigMap without the key stops the sync instead of revoking
	// the memberships it held.
	cm := &corev1.ConfigMap{}
	e.get(types.NamespacedName{Namespace: ownNS, Name: "extra"}, cm)
	cm.Data = map[string]string{"rows": "hpc-a|proj55|bob\n"}
	if err := e.c.Update(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
	if _, err := e.r.Sync(context.Background()); err == nil || !strings.Contains(err.Error(), `has no key "associations"`) {
		t.Fatalf("sync with a labelled ConfigMap without the key: %v", err)
	}
	if _, ok := e.localQueues()["bob/hpc-a-proj55"]; !ok {
		t.Errorf("membership of the labelled ConfigMap revoked")
	}
}
