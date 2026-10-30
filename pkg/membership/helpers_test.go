package membership

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/yaml"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

const (
	testSite = "hpc-a"
	ownNS    = "kueue-offload-membership"
)

// fixture: alice in proj42 and proj99, bob in proj42, carol has no identity,
// and alice also has a row on another Slurm cluster.
const fixture = `hpc-a|proj42|
hpc-a|proj42|alice
hpc-a|proj42|bob
hpc-a|proj99|
hpc-a|proj99|alice
hpc-a|proj77|carol
hpc-b|proj55|alice
`

func testScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, kueue.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// testMapper knows which kinds are cluster scoped, as discovery would. The
// fake client's default mapper calls every kind namespaced.
func testMapper(s *runtime.Scheme) meta.RESTMapper {
	root := map[schema.GroupKind]bool{
		{Kind: "Namespace"}: true, {Kind: "Node"}: true, {Kind: "PersistentVolume"}: true,
		{Group: rbacv1.GroupName, Kind: "ClusterRole"}: true, {Group: rbacv1.GroupName, Kind: "ClusterRoleBinding"}: true,
		{Group: kueue.GroupVersion.Group, Kind: "ClusterQueue"}: true, {Group: kueue.GroupVersion.Group, Kind: "Cohort"}: true,
		{Group: kueue.GroupVersion.Group, Kind: "ResourceFlavor"}: true, {Group: kueue.GroupVersion.Group, Kind: "AdmissionCheck"}: true,
		{Group: v1alpha1.GroupVersion.Group, Kind: "OffloadConfig"}: true, {Group: v1alpha1.GroupVersion.Group, Kind: "IdentityMap"}: true,
		{Group: v1alpha1.GroupVersion.Group, Kind: "ProjectQuota"}: true,
	}
	m := meta.NewDefaultRESTMapper(nil)
	for gvk := range s.AllKnownTypes() {
		scope := meta.RESTScopeNamespace
		if root[gvk.GroupKind()] {
			scope = meta.RESTScopeRoot
		}
		m.Add(gvk, scope)
	}
	return m
}

func quota(gpus int64) []kueue.ResourceGroup {
	return []kueue.ResourceGroup{{
		CoveredResources: []corev1.ResourceName{corev1.ResourceCPU, "nvidia.com/gpu"},
		Flavors: []kueue.FlavorQuotas{{Name: "hpc-a-gpu", Resources: []kueue.ResourceQuota{
			{Name: corev1.ResourceCPU, NominalQuota: resource.MustParse("64")},
			{Name: "nvidia.com/gpu", NominalQuota: *resource.NewQuantity(gpus, resource.DecimalSI)},
		}}},
	}}
}

func offloadConfig() *v1alpha1.OffloadConfig {
	return &v1alpha1.OffloadConfig{
		ObjectMeta: metav1.ObjectMeta{Name: testSite},
		Spec: v1alpha1.OffloadConfigSpec{Membership: &v1alpha1.Membership{
			SlurmCluster:        "hpc-a",
			Associations:        v1alpha1.ConfigMapKeyRef{Namespace: ownNS, Name: "assoc", Key: "associations"},
			ExecutorTemplate:    &v1alpha1.ConfigMapKeyRef{Namespace: ownNS, Name: "hpc-a-executor-template", Key: "template.yaml"},
			ExecutorImage:       "executor:1",
			DefaultProjectQuota: quota(8),
		}},
	}
}

func assocConfigMap(text string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "assoc", Namespace: ownNS},
		Data:       map[string]string{"associations": text},
	}
}

func templateConfigMap(text string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-executor-template", Namespace: ownNS},
		Data:       map[string]string{"template.yaml": text},
	}
}

func identityMap(entries ...v1alpha1.IdentityEntry) *v1alpha1.IdentityMap {
	return &v1alpha1.IdentityMap{ObjectMeta: metav1.ObjectMeta{Name: "users"}, Spec: v1alpha1.IdentityMapSpec{Entries: entries}}
}

func defaultIdentities() *v1alpha1.IdentityMap {
	return identityMap(
		v1alpha1.IdentityEntry{KubernetesUser: "alice", Site: testSite, HPCUser: "alice"},
		v1alpha1.IdentityEntry{KubernetesUser: "bob@example.org", Namespace: "bob", Site: testSite, HPCUser: "bob"},
		v1alpha1.IdentityEntry{KubernetesUser: "alice", Site: "hpc-b", HPCUser: "alice"},
	)
}

// sampleTemplate is the template shipped in config/samples.
func sampleTemplate(t *testing.T) string {
	data, err := os.ReadFile("../../config/samples/membership/executor-template.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{}
	if err := yaml.Unmarshal(data, cm); err != nil {
		t.Fatal(err)
	}
	if cm.Data["template.yaml"] == "" {
		t.Fatal("sample template is empty")
	}
	return cm.Data["template.yaml"]
}

type env struct {
	t      *testing.T
	c      client.Client
	r      *Reconciler
	events *events.FakeRecorder

	mu          sync.Mutex
	secretCalls int
}

// newEnv builds a fake cluster with the default fixture; objs are added or
// replace fixture objects of the same name. Every read or write of a Secret is
// counted.
func newEnv(t *testing.T, objs ...client.Object) *env {
	e := &env{t: t, events: events.NewFakeRecorder(200)}
	base := map[string]client.Object{}
	order := []string{}
	add := func(o client.Object) {
		k := fmt.Sprintf("%T/%s/%s", o, o.GetNamespace(), o.GetName())
		if _, ok := base[k]; !ok {
			order = append(order, k)
		}
		base[k] = o
	}
	for _, o := range []client.Object{offloadConfig(), assocConfigMap(fixture), templateConfigMap(sampleTemplate(t)), defaultIdentities(),
		&v1alpha1.ProjectQuota{ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-proj42"},
			Spec: v1alpha1.ProjectQuotaSpec{Site: testSite, Account: "proj42", ResourceGroups: quota(64)}}} {
		add(o)
	}
	for _, o := range objs {
		add(o)
	}
	list := make([]client.Object, 0, len(order))
	for _, k := range order {
		list = append(list, base[k])
	}
	s := testScheme(t)
	count := func(obj runtime.Object) {
		switch obj.(type) {
		case *corev1.Secret, *corev1.SecretList:
			e.mu.Lock()
			e.secretCalls++
			e.mu.Unlock()
		}
	}
	// The client-go type converter of the fake client cannot apply a
	// NetworkPolicy ("expected objects with types from the same schema"), so
	// apply merges with the deduced converter, which treats lists as atomic.
	e.c = fake.NewClientBuilder().WithScheme(s).WithRESTMapper(testMapper(s)).WithObjects(list...).
		WithTypeConverters(managedfields.NewDeducedTypeConverter()).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				count(obj)
				return c.Get(ctx, key, obj, opts...)
			},
			List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, opts ...client.ListOption) error {
				count(l)
				return c.List(ctx, l, opts...)
			},
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				count(obj)
				return c.Create(ctx, obj, opts...)
			},
		}).Build()
	e.r = &Reconciler{Client: e.c, APIReader: e.c, Recorder: e.events, Site: testSite, Namespace: ownNS,
		Now: func() time.Time { return time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC) }}
	return e
}

func (e *env) sync() time.Duration {
	e.t.Helper()
	after, err := e.r.Sync(context.Background())
	if err != nil {
		e.t.Fatalf("sync: %v", err)
	}
	return after
}

func (e *env) get(key types.NamespacedName, obj client.Object) bool {
	e.t.Helper()
	err := e.c.Get(context.Background(), key, obj)
	if err == nil {
		return true
	}
	if client.IgnoreNotFound(err) != nil {
		e.t.Fatal(err)
	}
	return false
}

func (e *env) namespace(name string) *corev1.Namespace {
	e.t.Helper()
	ns := &corev1.Namespace{}
	if !e.get(types.NamespacedName{Name: name}, ns) {
		e.t.Fatalf("namespace %s missing", name)
	}
	return ns
}

func (e *env) localQueues() map[string]string {
	e.t.Helper()
	l := &kueue.LocalQueueList{}
	if err := e.c.List(context.Background(), l); err != nil {
		e.t.Fatal(err)
	}
	out := map[string]string{}
	for _, q := range l.Items {
		out[q.Namespace+"/"+q.Name] = string(q.Spec.ClusterQueue)
	}
	return out
}

func (e *env) clusterQueues() map[string]kueue.ClusterQueue {
	e.t.Helper()
	l := &kueue.ClusterQueueList{}
	if err := e.c.List(context.Background(), l); err != nil {
		e.t.Fatal(err)
	}
	out := map[string]kueue.ClusterQueue{}
	for _, q := range l.Items {
		out[q.Name] = q
	}
	return out
}

func (e *env) executorConfigMap(ns string) *corev1.ConfigMap {
	e.t.Helper()
	cm := &corev1.ConfigMap{}
	if !e.get(types.NamespacedName{Namespace: ns, Name: "kueue-offload-executor"}, cm) {
		return nil
	}
	return cm
}

func (e *env) setAssociations(text string) {
	e.t.Helper()
	cm := &corev1.ConfigMap{}
	e.get(types.NamespacedName{Namespace: ownNS, Name: "assoc"}, cm)
	cm.Data["associations"] = text
	if err := e.c.Update(context.Background(), cm); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) setDeploymentAvailable(ns string, ok bool) {
	e.t.Helper()
	d := &appsv1.Deployment{}
	if !e.get(types.NamespacedName{Namespace: ns, Name: "kueue-offload-executor"}, d) {
		e.t.Fatalf("executor Deployment missing in %s", ns)
	}
	st := corev1.ConditionFalse
	if ok {
		st = corev1.ConditionTrue
	}
	d.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: st}}
	if err := e.c.Status().Update(context.Background(), d); err != nil {
		e.t.Fatal(err)
	}
}

// drainEvents returns the events recorded so far.
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
