package executor

import (
	"strings"
	"testing"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/executorapi"
	"github.com/matbun/kueue-offload/pkg/shadow"
)

// withLauncher returns a one-member request whose template names launcher.
func withLauncher(launcher string) *v1alpha1.OffloadRequest {
	q := testRequest(1)
	q.Spec.PodSets[0].Template.Annotations = map[string]string{LauncherAnnotation: launcher}
	return q
}

// A workload of one member that names a site launcher, from its template, its
// owner or its flavor, is a pod group of one, which the plugin starts through the
// launcher. Without a launcher, or with independent pods, it stays a single pod.
func TestLauncherMakesPodGroupOfOne(t *testing.T) {
	for _, tc := range []struct {
		name  string
		req   func() *v1alpha1.OffloadRequest
		group bool
	}{
		{"no launcher", func() *v1alpha1.OffloadRequest { return testRequest(1) }, false},
		{"launcher in the template", func() *v1alpha1.OffloadRequest { return withLauncher("torchrun") }, true},
		{"launcher on the owner", func() *v1alpha1.OffloadRequest {
			q := testRequest(1)
			q.Spec.OwnerAnnotations = map[string]string{LauncherAnnotation: "ray"}
			return q
		}, true},
		{"launcher from the flavor", func() *v1alpha1.OffloadRequest {
			q := testRequest(1)
			q.Spec.PodSets[0].Annotations[LauncherAnnotation] = "srun"
			return q
		}, true},
		{"independent pods", func() *v1alpha1.OffloadRequest {
			q := withLauncher("torchrun")
			q.Spec.PodSets[0].Template.Annotations[executorapi.IndependentPodsAnnotation] = "true"
			return q
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			q := e.submit(tc.req())
			pods := e.plugin.createdPods()
			if len(pods) != 1 || q.Status.JobID != executorJobID {
				t.Fatalf("%d /create calls, job %q", len(pods), q.Status.JobID)
			}
			pod := pods[0]
			grouped := pod.Labels[PodGroupNameLabel] == executorWorkload && pod.Annotations[PodGroupTotalCountAnnotation] == "1" &&
				pod.Annotations[GangRankAnnotation] == "0" && pod.Annotations[GangRoleAnnotation] == GangRoleHead
			keys := pod.Labels[PodGroupNameLabel] != "" || pod.Annotations[PodGroupTotalCountAnnotation] != "" || pod.Annotations[GangRoleAnnotation] != ""
			if tc.group && !grouped || !tc.group && keys {
				t.Fatalf("pod group %v, labels %v, annotations %v", tc.group, pod.Labels, pod.Annotations)
			}
			if pod.Annotations[GangEndAnnotation] != "" {
				t.Errorf("gang-end %q: a launcher ends the gang", pod.Annotations[GangEndAnnotation])
			}
		})
	}
}

// A plugin that lists pod-group-create gets the pod group of one in one
// /createPodGroup call.
func TestPodGroupOfOneInOneCreatePodGroup(t *testing.T) {
	e := newExecutorEnv(t)
	e.listPodGroupCreate()
	q := e.submit(withLauncher("torchrun"))
	calls := e.plugin.podGroupCalls()
	if q.Status.JobID != executorJobID || len(e.plugin.createdPods()) != 0 || len(calls) != 1 || len(calls[0].Pods) != 1 || calls[0].GroupUID != executorWorkload {
		t.Fatalf("job %q, %d /create and %d /createPodGroup calls", q.Status.JobID, len(e.plugin.createdPods()), len(calls))
	}
}

// A plugin that runs no pod groups cannot start a launcher: the workload is
// refused, naming both.
func TestLauncherNeedsPodGroupCapability(t *testing.T) {
	e := newExecutorEnv(t)
	e.plugin.with(func(p *fakePlugin) { p.capabilities = nil })
	q := e.take(withLauncher("torchrun"))
	e.refusedWith(q, `site launcher "torchrun"`)
	e.refusedWith(q, `capability "pod-group"`)
	if n := len(e.plugin.createdPods()); n != 0 {
		t.Fatalf("%d /create calls for a refused workload", n)
	}
}

// One member with a launcher keeps its reverse tunnel: the plugin sets it up in
// the batch shell of the one-node gang, whose job is named after the group.
func TestReverseOneNodeLauncher(t *testing.T) {
	e := reverseEnv(t, shadow.ReverseConfig{Enabled: true})
	req := reverseRequest(1, "8443:"+reverseTarget+":443")
	req.Spec.PodSets[0].Template.Annotations[LauncherAnnotation] = "torchrun"
	rt, aerr := e.d.reverseTunnel(jobRequestOf(req))
	if aerr != nil || rt.JobName != executorWorkload {
		t.Fatalf("tunnel %+v, %v", rt, aerr)
	}
	e.submit(req)
	pod := e.plugin.createdPods()[0]
	if pod.Labels[PodGroupNameLabel] != executorWorkload || !strings.Contains(pod.Annotations[preExecAnnotation], "interLink reverse tunnel") {
		t.Fatalf("labels %v, pre-exec\n%s", pod.Labels, pod.Annotations[preExecAnnotation])
	}
}
