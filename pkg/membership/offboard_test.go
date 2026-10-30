package membership

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// offboardText removes the namespace's directory on the site.
const offboardText = `apiVersion: batch/v1
kind: Job
metadata:
  name: offboard
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: rm
        image: tools:1
        args: ["rm -rf /site/{{ .HPCUser }}/{{ .Namespace }}", {{ join "," .Accounts | quote }}]
`

func offboardConfig(labels map[string]string) *v1alpha1.OffloadConfig {
	sc := offloadConfig()
	sc.Spec.Membership.OffboardTemplate = &v1alpha1.ConfigMapKeyRef{Namespace: ownNS, Name: "hpc-a-offboard", Key: "offboard.yaml"}
	sc.Spec.Membership.NamespaceLabels = labels
	return sc
}

func offboardConfigMap(text string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-offboard", Namespace: ownNS},
		Data:       map[string]string{"offboard.yaml": text},
	}
}

// setIdentities replaces the entries of the IdentityMap users.
func (e *env) setIdentities(entries ...v1alpha1.IdentityEntry) {
	e.t.Helper()
	im := &v1alpha1.IdentityMap{}
	e.get(types.NamespacedName{Name: "users"}, im)
	im.Spec.Entries = entries
	if err := e.c.Update(context.Background(), im); err != nil {
		e.t.Fatal(err)
	}
}

// offloadRequest creates an OffloadRequest of worker in ns, held by the
// executor's finalizer, for the Workload of the same name.
func (e *env) offloadRequest(ns, name, worker string) *v1alpha1.OffloadRequest {
	e.t.Helper()
	q := &v1alpha1.OffloadRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{v1alpha1.SiteLabel: worker},
			Finalizers: []string{v1alpha1.ExecutorFinalizer}},
		Spec: v1alpha1.OffloadRequestSpec{Site: worker, Account: "proj42",
			Workload: v1alpha1.WorkloadReference{Name: name, UID: types.UID(name + "-uid")}},
	}
	if err := e.c.Create(context.Background(), q); err != nil {
		e.t.Fatal(err)
	}
	return q
}

// workload creates the Workload an OffloadRequest of the same name refers to.
func (e *env) workload(ns, name string, finished bool) {
	e.t.Helper()
	wl := &kueue.Workload{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(name + "-uid")}}
	if finished {
		wl.Status.Conditions = []metav1.Condition{{Type: kueue.WorkloadFinished, Status: metav1.ConditionTrue,
			Reason: kueue.WorkloadFinishedReasonSucceeded, LastTransitionTime: metav1.Now()}}
	}
	if err := e.c.Create(context.Background(), wl); err != nil {
		e.t.Fatal(err)
	}
}

// finishWorkload marks a Workload finished.
func (e *env) finishWorkload(ns, name string) {
	e.t.Helper()
	wl := &kueue.Workload{}
	e.get(types.NamespacedName{Namespace: ns, Name: name}, wl)
	wl.Status.Conditions = []metav1.Condition{{Type: kueue.WorkloadFinished, Status: metav1.ConditionTrue,
		Reason: kueue.WorkloadFinishedReasonSucceeded, LastTransitionTime: metav1.Now()}}
	if err := e.c.Update(context.Background(), wl); err != nil {
		e.t.Fatal(err)
	}
}

// finalize does what the executor does with a deleted OffloadRequest.
func (e *env) finalize(ns, name string) {
	e.t.Helper()
	q := &v1alpha1.OffloadRequest{}
	if !e.get(types.NamespacedName{Namespace: ns, Name: name}, q) {
		e.t.Fatalf("OffloadRequest %s/%s missing", ns, name)
	}
	if q.DeletionTimestamp.IsZero() {
		e.t.Fatalf("OffloadRequest %s/%s not deleted", ns, name)
	}
	q.Finalizers = nil
	if err := e.c.Update(context.Background(), q); err != nil {
		e.t.Fatal(err)
	}
}

// deleting reports whether the OffloadRequest exists and is being deleted.
func (e *env) deleting(ns, name string) bool {
	e.t.Helper()
	q := &v1alpha1.OffloadRequest{}
	return e.get(types.NamespacedName{Namespace: ns, Name: name}, q) && !q.DeletionTimestamp.IsZero()
}

func (e *env) endJob(ns, name string, end batchv1.JobConditionType) {
	e.t.Helper()
	job := &batchv1.Job{}
	if !e.get(types.NamespacedName{Namespace: ns, Name: name}, job) {
		e.t.Fatalf("Job %s/%s missing", ns, name)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: end, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"}}
	if err := e.c.Status().Update(context.Background(), job); err != nil {
		e.t.Fatal(err)
	}
}

// executorObjects names the objects of the sample template that exist in ns.
func (e *env) executorObjects(ns string) []string {
	e.t.Helper()
	var out []string
	for kind, obj := range map[string]client.Object{
		"ServiceAccount": &corev1.ServiceAccount{}, "RoleBinding": &rbacv1.RoleBinding{}, "ConfigMap": &corev1.ConfigMap{},
		"Deployment": &appsv1.Deployment{}, "Service": &corev1.Service{}, "NetworkPolicy": &networkingv1.NetworkPolicy{},
	} {
		if e.get(types.NamespacedName{Namespace: ns, Name: "kueue-offload-executor"}, obj) {
			out = append(out, kind)
		}
	}
	return out
}

var bobIdentity = v1alpha1.IdentityEntry{KubernetesUser: "bob@example.org", Namespace: "bob", Site: testSite, HPCUser: "bob"}

func TestOffboardingWaitsForRequestsThenCleansUp(t *testing.T) {
	keep := []client.Object{
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-submit-key", Namespace: "bob"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "notes", Namespace: "bob"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "other-site", Namespace: "bob",
			Labels: map[string]string{ManagedByLabel: ManagedByValue, SiteLabel: "hpc-b"}}},
	}
	e := newEnv(t, append(keep, offboardConfig(nil), offboardConfigMap(offboardText))...)
	e.sync()
	if got := e.namespace("bob").Annotations[UserAnnotation(testSite)]; got != "bob" {
		t.Fatalf("user annotation %q, want bob", got)
	}
	e.setDeploymentAvailable("alice", true)

	e.setIdentities(defaultIdentities().Spec.Entries[0])
	e.workload("bob", "train-1", false)
	e.offloadRequest("bob", "train-1", testSite)
	e.workload("bob", "done-1", true)
	e.offloadRequest("bob", "done-1", testSite)
	e.offloadRequest("bob", "gone-1", testSite)
	e.workload("bob", "other-1", false)
	e.offloadRequest("bob", "other-1", "hpc-b")
	// The first pass only revokes: quota reserved before may still get its
	// OffloadRequest.
	if after := e.sync(); after != offboardRecheck {
		t.Errorf("requeue after %v while bob leaves, want %v", after, offboardRecheck)
	}
	if _, ok := e.localQueues()["bob/hpc-a-proj42"]; ok {
		t.Errorf("LocalQueue kept")
	}
	if got := e.namespace("bob").Annotations[LeftAnnotation(testSite)]; got != "proj42" {
		t.Errorf("left annotation %q, want the accounts bob had", got)
	}
	if got := e.executorObjects("bob"); len(got) != 6 {
		t.Errorf("executor objects %v right after the membership went", got)
	}
	e.sync()
	if e.get(types.NamespacedName{Namespace: "bob", Name: "offboard"}, &batchv1.Job{}) || len(e.executorObjects("bob")) != 6 {
		t.Fatalf("offboarded while a job of the site runs")
	}
	if e.deleting("bob", "done-1") {
		t.Errorf("request of a finished Workload deleted while another job runs")
	}

	// A request whose Workload is gone waits for the garbage collector and the
	// executor.
	e.finishWorkload("bob", "train-1")
	e.sync()
	if e.deleting("bob", "train-1") || e.deleting("bob", "done-1") {
		t.Errorf("requests of finished Workloads deleted while a request of a deleted Workload is left")
	}
	if err := e.c.Delete(context.Background(), &v1alpha1.OffloadRequest{ObjectMeta: metav1.ObjectMeta{Namespace: "bob", Name: "gone-1"}}); err != nil {
		t.Fatal(err)
	}
	e.finalize("bob", "gone-1")

	// Once nothing runs, the requests of finished Workloads are deleted, for the
	// executor to clean up after them; the other site's request stays.
	if after := e.sync(); after != offboardRecheck {
		t.Errorf("requeue after %v while the requests of finished jobs go", after)
	}
	if !e.deleting("bob", "train-1") || !e.deleting("bob", "done-1") || e.deleting("bob", "other-1") {
		t.Errorf("deleted requests: train-1 %v, done-1 %v, other-1 %v", e.deleting("bob", "train-1"),
			e.deleting("bob", "done-1"), e.deleting("bob", "other-1"))
	}
	e.finalize("bob", "train-1")
	e.sync()
	if e.get(types.NamespacedName{Namespace: "bob", Name: "offboard"}, &batchv1.Job{}) || len(e.executorObjects("bob")) != 6 {
		t.Fatalf("offboarded while the executor finalizes a request")
	}

	// Only the site's requests hold the namespace.
	e.finalize("bob", "done-1")
	if after := e.sync(); after != offboardRecheck {
		t.Errorf("requeue after %v while the offboard Job runs", after)
	}
	job := &batchv1.Job{}
	if !e.get(types.NamespacedName{Namespace: "bob", Name: "offboard"}, job) {
		t.Fatal("offboard Job not created")
	}
	if got := job.Spec.Template.Spec.Containers[0].Args; !reflect.DeepEqual(got, []string{"rm -rf /site/bob/bob", "proj42"}) {
		t.Errorf("offboard Job args %v", got)
	}
	for k, v := range map[string]string{ManagedByLabel: ManagedByValue, SiteLabel: testSite, OffboardLabel: "true"} {
		if job.Labels[k] != v {
			t.Errorf("offboard Job label %s = %q", k, job.Labels[k])
		}
	}
	e.sync()
	if len(e.executorObjects("bob")) != 6 {
		t.Fatalf("executor objects deleted while the offboard Job runs")
	}

	e.endJob("bob", "offboard", batchv1.JobComplete)
	e.drainEvents()
	if after := e.sync(); after != 15*time.Minute {
		t.Errorf("requeue after %v once bob is offboarded, want the syncInterval", after)
	}
	if got := e.executorObjects("bob"); len(got) != 0 {
		t.Errorf("executor objects %v left", got)
	}
	if e.get(types.NamespacedName{Namespace: "bob", Name: "offboard"}, &batchv1.Job{}) {
		t.Errorf("offboard Job left")
	}
	if e.secretCalls != 0 || e.secretMetadataCalls == 0 {
		t.Errorf("%d Secret calls, %d Secret metadata reads", e.secretCalls, e.secretMetadataCalls)
	}
	for _, o := range keep {
		if !e.get(client.ObjectKeyFromObject(o), o) {
			t.Errorf("%T %s deleted", o, o.GetName())
		}
	}
	ns := e.namespace("bob")
	for k := range ns.Labels {
		if strings.HasPrefix(k, labelPrefix) && k != ManagedByLabel {
			t.Errorf("label %s left", k)
		}
	}
	for k := range ns.Annotations {
		if strings.HasPrefix(k, labelPrefix) {
			t.Errorf("annotation %s left", k)
		}
	}
	if ev := e.eventsWith(EventOffboarded); len(ev) != 1 {
		t.Errorf("offboarded events %v", ev)
	}

	// Added back, bob gets everything again.
	e.setIdentities(defaultIdentities().Spec.Entries...)
	e.sync()
	if got := e.executorObjects("bob"); len(got) != 6 {
		t.Errorf("executor objects %v after bob is back", got)
	}
	if _, ok := e.localQueues()["bob/hpc-a-proj42"]; !ok {
		t.Errorf("LocalQueue not back")
	}
	if ns := e.namespace("bob"); ns.Labels[TenantLabel] != "true" || ns.Annotations[UserAnnotation(testSite)] != "bob" {
		t.Errorf("namespace marks not back: %v %v", ns.Labels, ns.Annotations)
	}
}

func TestOffboardJobFailureDoesNotKeepTheTenant(t *testing.T) {
	e := newEnv(t, offboardConfig(nil), offboardConfigMap(offboardText))
	e.sync()
	e.setIdentities(defaultIdentities().Spec.Entries[0])
	e.sync()
	e.sync()
	e.endJob("bob", "offboard", batchv1.JobFailed)
	e.drainEvents()
	e.sync()
	if got := e.executorObjects("bob"); len(got) != 0 {
		t.Errorf("executor objects %v left after a failed offboard Job", got)
	}
	failed := e.eventsWith(EventOffboardFailed)
	if len(failed) != 1 || !strings.Contains(failed[0], "offboard Job offboard") || !strings.Contains(failed[0], "BackoffLimitExceeded") {
		t.Errorf("offboard failure events %v", failed)
	}
}

// The offboard Jobs run once: when deleting the objects fails after they
// ended, the next pass deletes the rest without running them again, as what
// they mount may be gone.
func TestOffboardJobsRunOnce(t *testing.T) {
	e := newEnv(t, offboardConfig(nil), offboardConfigMap(offboardText))
	e.sync()
	e.setDeploymentAvailable("alice", true)
	e.setIdentities(defaultIdentities().Spec.Entries[0])
	e.sync()
	e.sync()
	e.endJob("bob", "offboard", batchv1.JobComplete)
	e.failDeleteAllOf = "NetworkPolicy"
	if _, err := e.r.Sync(context.Background()); err == nil || !strings.Contains(err.Error(), "NetworkPolicy") {
		t.Fatalf("sync with NetworkPolicies not deleted: %v", err)
	}
	if got := e.namespace("bob").Annotations[OffboardedAnnotation(testSite)]; got != "true" {
		t.Errorf("offboarded annotation %q", got)
	}
	if e.get(types.NamespacedName{Namespace: "bob", Name: "offboard"}, &batchv1.Job{}) {
		t.Fatal("offboard Job kept")
	}
	e.failDeleteAllOf = ""
	if after := e.sync(); after != 15*time.Minute {
		t.Errorf("requeue after %v, want bob offboarded", after)
	}
	if e.get(types.NamespacedName{Namespace: "bob", Name: "offboard"}, &batchv1.Job{}) {
		t.Errorf("offboard Job created again")
	}
	if got := e.executorObjects("bob"); len(got) != 0 {
		t.Errorf("executor objects %v left", got)
	}
	ns := e.namespace("bob")
	if hasKey(ns.Annotations, OffboardedAnnotation(testSite)) || hasKey(ns.Annotations, LeftAnnotation(testSite)) {
		t.Errorf("annotations left: %v", ns.Annotations)
	}

	// The next offboarding runs its Jobs again.
	e.setIdentities(defaultIdentities().Spec.Entries...)
	e.sync()
	e.setIdentities(defaultIdentities().Spec.Entries[0])
	e.sync()
	e.sync()
	if !e.get(types.NamespacedName{Namespace: "bob", Name: "offboard"}, &batchv1.Job{}) {
		t.Errorf("offboard Job not created at the next offboarding")
	}
}

func TestOffboardingWithoutTemplate(t *testing.T) {
	e := newEnv(t)
	e.sync()
	e.setIdentities(defaultIdentities().Spec.Entries[0])
	e.sync()
	if len(e.executorObjects("bob")) != 6 {
		t.Fatalf("executor deleted in the pass that revoked the membership")
	}
	e.sync()
	if got := e.executorObjects("bob"); len(got) != 0 {
		t.Errorf("executor objects %v left", got)
	}
	jobs := &batchv1.JobList{}
	if err := e.c.List(context.Background(), jobs, client.InNamespace("bob")); err != nil || len(jobs.Items) != 0 {
		t.Errorf("Jobs %v, %v", jobs.Items, err)
	}
}

// A namespace that comes back while its offboard Job runs loses the Job: it
// would remove what the executor uses, and its completion would end the next
// offboarding before anything ran.
func TestBackDuringOffboardingDropsTheOffboardJob(t *testing.T) {
	e := newEnv(t, offboardConfig(nil), offboardConfigMap(offboardText))
	e.sync()
	e.setIdentities(defaultIdentities().Spec.Entries[0])
	e.sync()
	e.sync()
	if !e.get(types.NamespacedName{Namespace: "bob", Name: "offboard"}, &batchv1.Job{}) {
		t.Fatal("offboard Job not created")
	}
	e.setIdentities(defaultIdentities().Spec.Entries...)
	e.sync()
	if e.get(types.NamespacedName{Namespace: "bob", Name: "offboard"}, &batchv1.Job{}) {
		t.Errorf("offboard Job kept after bob came back")
	}
	if len(e.executorObjects("bob")) != 6 {
		t.Errorf("executor objects %v", e.executorObjects("bob"))
	}
}

func TestOffboardTemplateHoldsJobsOnly(t *testing.T) {
	e := newEnv(t, offboardConfig(nil), offboardConfigMap("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\n"))
	e.sync()
	e.setIdentities(defaultIdentities().Spec.Entries[0])
	e.sync()
	if _, err := e.r.Sync(context.Background()); err == nil || !strings.Contains(err.Error(), "batch/v1 Jobs only") {
		t.Fatalf("sync with a ConfigMap in the offboard template: %v", err)
	}
	if len(e.executorObjects("bob")) != 6 {
		t.Errorf("executor deleted although the offboard template was refused")
	}
	if len(e.eventsWith(EventTemplateRefused)) == 0 {
		t.Errorf("no %s event", EventTemplateRefused)
	}
}

func TestNamespaceLabels(t *testing.T) {
	const key = "kueue.example.org/managed"
	// alice had the label before: it is not the controller's.
	alice := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "alice", Labels: map[string]string{key: "true"}}}
	e := newEnv(t, alice, offboardConfig(map[string]string{key: "true", "team": "hpc"}), offboardConfigMap(offboardText))
	e.sync()
	a, b := e.namespace("alice"), e.namespace("bob")
	if a.Labels[key] != "true" || a.Labels["team"] != "hpc" || a.Annotations[LabelsAnnotation(testSite)] != "team" {
		t.Errorf("alice labels %v annotations %v", a.Labels, a.Annotations)
	}
	if b.Labels[key] != "true" || b.Annotations[LabelsAnnotation(testSite)] != key+",team" {
		t.Errorf("bob labels %v annotations %v", b.Labels, b.Annotations)
	}

	// A label the site no longer sets goes where the controller added it.
	sc := offboardConfig(map[string]string{key: "true"})
	cur := &v1alpha1.OffloadConfig{}
	e.get(types.NamespacedName{Name: testSite}, cur)
	cur.Spec = sc.Spec
	if err := e.c.Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	e.sync()
	if a := e.namespace("alice"); hasKey(a.Labels, "team") || hasKey(a.Annotations, LabelsAnnotation(testSite)) {
		t.Errorf("alice keeps team: %v %v", a.Labels, a.Annotations)
	}

	// Both leave: only the label the controller added goes, once offboarded.
	e.setIdentities()
	e.setAssociations("hpc-a|proj42|carol\n")
	e.sync()
	if e.namespace("bob").Labels[key] != "true" {
		t.Errorf("label removed before the namespace was offboarded")
	}
	e.sync()
	e.endJob("alice", "offboard", batchv1.JobComplete)
	e.endJob("bob", "offboard", batchv1.JobComplete)
	e.sync()
	if got := e.namespace("alice").Labels[key]; got != "true" {
		t.Errorf("alice lost %s, which she had before", key)
	}
	if hasKey(e.namespace("bob").Labels, key) {
		t.Errorf("bob keeps %s after offboarding", key)
	}
}

func TestNamespaceLabelsValidated(t *testing.T) {
	for name, labels := range map[string]map[string]string{
		"own prefix": {TenantLabel: "true"},
		"bad key":    {"not a key!": "true"},
		"bad value":  {"example.org/x": "a b"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := resolveSite(offboardConfig(labels)); err == nil {
				t.Errorf("namespaceLabels %v accepted", labels)
			}
		})
	}
}

// The tenant label stays while the namespace is a member of another site.
func TestOffboardKeepsTenantLabelOfOtherSite(t *testing.T) {
	e := newEnv(t)
	e.sync()
	if err := e.r.patchNamespace(context.Background(), "bob", nil, map[string]any{AccountsAnnotation("hpc-b"): "proj55"}); err != nil {
		t.Fatal(err)
	}
	e.setIdentities(defaultIdentities().Spec.Entries[0])
	e.sync()
	e.sync()
	if got := e.executorObjects("bob"); len(got) != 0 {
		t.Fatalf("executor objects %v left", got)
	}
	if e.namespace("bob").Labels[TenantLabel] != "true" {
		t.Errorf("tenant label removed while bob is a member of hpc-b")
	}
}

// Objects the executor template renders per account go too: the template is
// rendered with the accounts the namespace had.
func TestOffboardDeletesPerAccountObjects(t *testing.T) {
	const text = `apiVersion: v1
kind: ConfigMap
metadata:
  name: kueue-offload-executor
data: {user: {{ quote .HPCUser }}}
{{- range .Accounts }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: runner-{{ . }}
{{- end }}
`
	e := newEnv(t, templateConfigMap(text))
	e.sync()
	if !e.get(types.NamespacedName{Namespace: "alice", Name: "runner-proj99"}, &corev1.ServiceAccount{}) {
		t.Fatal("per-account ServiceAccount not applied")
	}
	e.setIdentities(defaultIdentities().Spec.Entries[1])
	e.sync()
	e.sync()
	for _, name := range []string{"runner-proj42", "runner-proj99"} {
		if e.get(types.NamespacedName{Namespace: "alice", Name: name}, &corev1.ServiceAccount{}) {
			t.Errorf("ServiceAccount %s left", name)
		}
	}
	if e.executorConfigMap("alice") != nil {
		t.Errorf("executor ConfigMap left")
	}
}
