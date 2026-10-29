package membership

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (e *env) agentPod(ns, name string, waiting *corev1.ContainerStateWaiting) {
	e.t.Helper()
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"app": "interlink-agent"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "agent", Image: "agent:1"}}},
	}
	if err := e.c.Create(context.Background(), p); err != nil {
		e.t.Fatal(err)
	}
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Waiting: waiting}}}
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

func TestMissingCredentialsFromAgentPods(t *testing.T) {
	e := newEnv(t)
	e.sync()
	e.drainEvents()

	// Not available and no pod yet: no evidence, no annotation.
	if hasKey(e.namespace("alice").Annotations, AgentAnnotation(testSite)) {
		t.Fatal("annotation set without evidence")
	}

	e.agentPod("alice", "interlink-agent-1", &corev1.ContainerStateWaiting{
		Reason: "CreateContainerConfigError", Message: `secret "hpc-a-submit-key" not found`,
	})
	e.agentPod("bob", "interlink-agent-1", &corev1.ContainerStateWaiting{
		Reason: "ImagePullBackOff", Message: `Back-off pulling image "agent:1"`,
	})
	if after := e.sync(); after != time.Minute {
		t.Errorf("requeue after %v while agents are not available", after)
	}
	if got := e.namespace("alice").Annotations[AgentAnnotation(testSite)]; got != MissingCredentials {
		t.Errorf("alice annotation %q, want %q", got, MissingCredentials)
	}
	if hasKey(e.namespace("bob").Annotations, AgentAnnotation(testSite)) {
		t.Errorf("bob's image pull failure reported as missing credentials")
	}
	warn := e.eventsWith(EventMissingCredentials)
	if len(warn) != 1 || !strings.Contains(warn[0], "hpc-a-submit-key") || !strings.Contains(warn[0], "Deployment interlink-agent") {
		t.Errorf("missing credentials events %v", warn)
	}

	// No new event while nothing changes.
	e.sync()
	if warn := e.eventsWith(EventMissingCredentials); len(warn) != 0 {
		t.Errorf("event repeated: %v", warn)
	}

	e.setDeploymentAvailable("alice", true)
	e.sync()
	if hasKey(e.namespace("alice").Annotations, AgentAnnotation(testSite)) {
		t.Errorf("annotation kept after the agent became available")
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
			e.agentPod("alice", "interlink-agent-1", &corev1.ContainerStateWaiting{Reason: "ContainerCreating"})
			e.podEvent("alice", "interlink-agent-1", "FailedMount", tc.msg, e.r.now().Add(-tc.age))
			// An event about another pod never counts.
			e.podEvent("alice", "notebook-0", "FailedMount", `secret "other" not found`, e.r.now())
			e.sync()
			got := e.namespace("alice").Annotations[AgentAnnotation(testSite)] == MissingCredentials
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
