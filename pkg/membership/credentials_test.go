package membership

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (e *env) executorPod(ns, name string, waiting *corev1.ContainerStateWaiting) {
	e.t.Helper()
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"app": "kueue-offload-executor"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "executor", Image: "executor:1"}}},
	}
	if err := e.c.Create(context.Background(), p); err != nil {
		e.t.Fatal(err)
	}
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "executor", State: corev1.ContainerState{Waiting: waiting}}}
	if err := e.c.Status().Update(context.Background(), p); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) podEvent(ns, pod, reason, msg string, last time.Time) {
	e.t.Helper()
	ev := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: pod + "." + reason, Namespace: ns},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: ns, Name: pod},
		Reason:         reason,
		Message:        msg,
		Type:           corev1.EventTypeWarning,
		FirstTimestamp: metav1.NewTime(last.Add(-time.Hour)),
		LastTimestamp:  metav1.NewTime(last),
	}
	if err := e.c.Create(context.Background(), ev); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) eventsWith(reason string) []string {
	var out []string
	for _, ev := range e.drainEvents() {
		if strings.Contains(ev, " "+reason+" ") {
			out = append(out, ev)
		}
	}
	return out
}

func TestMissingCredentialsFromExecutorPods(t *testing.T) {
	e := newEnv(t)
	e.sync()
	e.drainEvents()

	// Not available and no pod yet: no evidence, no annotation.
	if hasKey(e.namespace("alice").Annotations, ExecutorAnnotation(testSite)) {
		t.Fatal("annotation set without evidence")
	}

	e.executorPod("alice", "kueue-offload-executor-1", &corev1.ContainerStateWaiting{
		Reason: "CreateContainerConfigError", Message: `secret "hpc-a-submit-key" not found`,
	})
	e.executorPod("bob", "kueue-offload-executor-1", &corev1.ContainerStateWaiting{
		Reason: "ImagePullBackOff", Message: `Back-off pulling image "executor:1"`,
	})
	if after := e.sync(); after != time.Minute {
		t.Errorf("requeue after %v while executors are not available", after)
	}
	if got := e.namespace("alice").Annotations[ExecutorAnnotation(testSite)]; got != MissingCredentials {
		t.Errorf("alice annotation %q, want %q", got, MissingCredentials)
	}
	if hasKey(e.namespace("bob").Annotations, ExecutorAnnotation(testSite)) {
		t.Errorf("bob's image pull failure reported as missing credentials")
	}
	warn := e.eventsWith(EventMissingCredentials)
	if len(warn) != 1 || !strings.Contains(warn[0], "hpc-a-submit-key") || !strings.Contains(warn[0], "Deployment kueue-offload-executor") {
		t.Errorf("missing credentials events %v", warn)
	}

	// No new event while nothing changes.
	e.sync()
	if warn := e.eventsWith(EventMissingCredentials); len(warn) != 0 {
		t.Errorf("event repeated: %v", warn)
	}

	e.setDeploymentAvailable("alice", true)
	e.sync()
	if hasKey(e.namespace("alice").Annotations, ExecutorAnnotation(testSite)) {
		t.Errorf("annotation kept after the executor became available")
	}
	if ok := e.eventsWith(EventCredentialsPresent); len(ok) != 1 {
		t.Errorf("credentials present events %v", ok)
	}
	if e.secretCalls != 0 {
		t.Errorf("%d Secret calls", e.secretCalls)
	}
}

func TestMissingCredentialsFromMountEvents(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		age  time.Duration
		want bool
	}{
		{"recent missing secret", `MountVolume.SetUp failed for volume "submit-key" : secret "hpc-a-submit-key" not found`, time.Minute, true},
		{"missing key", `couldn't find key ssh-privatekey in Secret alice/hpc-a-submit-key`, time.Minute, true},
		{"stale event", `MountVolume.SetUp failed for volume "submit-key" : secret "hpc-a-submit-key" not found`, 30 * time.Minute, false},
		{"other mount failure", `MountVolume.SetUp failed for volume "jobs" : rpc error: code = DeadlineExceeded`, time.Minute, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.sync()
			e.executorPod("alice", "kueue-offload-executor-1", &corev1.ContainerStateWaiting{Reason: "ContainerCreating"})
			e.podEvent("alice", "kueue-offload-executor-1", "FailedMount", tc.msg, e.r.now().Add(-tc.age))
			// An event about another pod never counts.
			e.podEvent("alice", "notebook-0", "FailedMount", `secret "other" not found`, e.r.now())
			e.sync()
			got := e.namespace("alice").Annotations[ExecutorAnnotation(testSite)] == MissingCredentials
			if got != tc.want {
				t.Errorf("annotation set %v, want %v", got, tc.want)
			}
			for _, ev := range e.eventsWith(EventMissingCredentials) {
				if strings.Contains(ev, `"other"`) {
					t.Errorf("event of a foreign pod used: %s", ev)
				}
			}
			if e.secretCalls != 0 {
				t.Errorf("%d Secret calls", e.secretCalls)
			}
		})
	}
}

func (e *env) secretVersions(ns string) string {
	e.t.Helper()
	d := &appsv1.Deployment{}
	if !e.get(types.NamespacedName{Namespace: ns, Name: "kueue-offload-executor"}, d) {
		e.t.Fatalf("executor Deployment missing in %s", ns)
	}
	return d.Spec.Template.Annotations[SecretVersionsAnnotation]
}

// A replaced Secret changes the pod template of the executor that mounts it, so
// that the Deployment rolls; the Secret's data is never read.
func TestReplacedSecretRollsExecutor(t *testing.T) {
	key := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-submit-key", Namespace: "alice"},
		Data: map[string][]byte{"ssh-privatekey": []byte("old")}}
	e := newEnv(t, key)
	e.sync()
	cur := &corev1.Secret{}
	e.get(types.NamespacedName{Namespace: "alice", Name: "hpc-a-submit-key"}, cur)
	e.secretCalls = 0
	if got, want := e.secretVersions("alice"), "hpc-a-submit-key="+cur.ResourceVersion; got != want {
		t.Errorf("alice secret versions %q, want %q", got, want)
	}
	if got := e.secretVersions("bob"); got != "" {
		t.Errorf("bob, without the Secret, has secret versions %q", got)
	}

	cur.Data["ssh-privatekey"] = []byte("new")
	if err := e.c.Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	e.sync()
	if got, want := e.secretVersions("alice"), "hpc-a-submit-key="+cur.ResourceVersion; got != want {
		t.Errorf("secret versions %q after the key was replaced, want %q", got, want)
	}
	if e.secretCalls != 0 || e.secretMetadataCalls == 0 {
		t.Errorf("%d Secret calls, %d Secret metadata reads", e.secretCalls, e.secretMetadataCalls)
	}

	// A Secret that is missing for a while keeps the running executor as it is,
	// and rolls it once it is back.
	want := e.secretVersions("alice")
	if err := e.c.Delete(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	e.sync()
	if got := e.secretVersions("alice"); got != want {
		t.Errorf("secret versions %q while the key is missing, want %q kept", got, want)
	}
	back := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-submit-key", Namespace: "alice"}}
	if err := e.c.Create(context.Background(), back); err != nil {
		t.Fatal(err)
	}
	e.sync()
	if got := e.secretVersions("alice"); got != "hpc-a-submit-key="+back.ResourceVersion || got == want {
		t.Errorf("secret versions %q once the key is back, want hpc-a-submit-key=%s", got, back.ResourceVersion)
	}

	// A Secret created later rolls the executor that waited for it.
	if err := e.c.Create(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "hpc-a-submit-key", Namespace: "bob"}}); err != nil {
		t.Fatal(err)
	}
	e.sync()
	if got := e.secretVersions("bob"); !strings.HasPrefix(got, "hpc-a-submit-key=") {
		t.Errorf("bob secret versions %q once the Secret exists", got)
	}
}

func TestStampSecretVersions(t *testing.T) {
	const text = `apiVersion: batch/v1
kind: Job
metadata:
  name: remote
spec:
  template:
    metadata:
      annotations: {note: kept}
    spec:
      restartPolicy: Never
      initContainers:
      - name: init
        image: x
        envFrom: [{secretRef: {name: from-env}}]
      containers:
      - name: c
        image: x
        env: [{name: K, valueFrom: {secretKeyRef: {name: from-key, key: k}}}, {name: P, value: plain}]
      volumes:
      - {name: a, secret: {secretName: mounted}}
      - {name: b, projected: {sources: [{secret: {name: projected}}, {configMap: {name: not-a-secret}}]}}
      - {name: c, secret: {secretName: missing}}
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: nightly
spec:
  schedule: "0 0 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers: [{name: c, image: x}]
          volumes: [{name: a, secret: {secretName: mounted}}]
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: plain
`
	var objs []client.Object
	for _, n := range []string{"from-env", "from-key", "mounted", "projected"} {
		objs = append(objs, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: n, Namespace: "alice"}})
	}
	e := newEnv(t, objs...)
	tmpl, err := ParseTemplate(text)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := renderObjects(tmpl, TemplateData{})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.r.stampSecretVersions(context.Background(), rendered, "alice"); err != nil {
		t.Fatal(err)
	}
	rv := map[string]string{}
	for _, o := range objs {
		s := &corev1.Secret{}
		e.get(client.ObjectKeyFromObject(o), s)
		rv[o.GetName()] = s.ResourceVersion
	}
	job, _, _ := unstructured.NestedStringMap(rendered[0].Object, "spec", "template", "metadata", "annotations")
	want := "from-env=" + rv["from-env"] + ",from-key=" + rv["from-key"] + ",mounted=" + rv["mounted"] + ",projected=" + rv["projected"]
	if job[SecretVersionsAnnotation] != want || job["note"] != "kept" {
		t.Errorf("Job pod template annotations %v, want %s=%s", job, SecretVersionsAnnotation, want)
	}
	cron, _, _ := unstructured.NestedStringMap(rendered[1].Object, "spec", "jobTemplate", "spec", "template", "metadata", "annotations")
	if cron[SecretVersionsAnnotation] != "mounted="+rv["mounted"] {
		t.Errorf("CronJob pod template annotations %v", cron)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(rendered[2].Object, "spec"); found {
		t.Errorf("ConfigMap changed: %v", rendered[2].Object)
	}
	if e.secretMetadataCalls != 5 {
		t.Errorf("%d Secret metadata reads, want one per Secret", e.secretMetadataCalls)
	}
}
