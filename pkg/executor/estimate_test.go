package executor

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// estimateRoute sends /estimate straight to the plugin at host, in the place of
// an API server without that route (interLink before feat/estimate), as
// apiClient does for /createPodGroup.
type estimateRoute struct {
	host string
	next http.RoundTripper
}

func (r estimateRoute) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path == "/estimate" {
		req = req.Clone(req.Context())
		req.URL.Host, req.Host = r.host, r.host
	}
	return r.next.RoundTrip(req)
}

// withEstimates makes the plugin list capability estimate and answer start,
// through the API server when it has /estimate.
func (e *executorEnv) withEstimates(start time.Time) {
	e.t.Helper()
	e.plugin.with(func(p *fakePlugin) {
		p.capabilities = append(p.capabilities, CapabilityEstimate)
		p.estimateStart = start
	})
	resp, err := http.Post(e.d.InterLink.URL+"/estimate", "application/json", strings.NewReader("{}"))
	if err != nil {
		e.t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		return
	}
	plugin, err := url.Parse(e.plugin.srv.URL)
	if err != nil {
		e.t.Fatal(err)
	}
	next := e.d.InterLink.HTTP.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	e.d.InterLink.HTTP = &http.Client{Transport: estimateRoute{host: plugin.Host, next: next}}
}

// The first pass of an attempt asks the site when it would start the job and
// records it, without creating anything; the next one submits without asking
// again.
func TestEstimateBeforeSubmit(t *testing.T) {
	e := newExecutorEnv(t)
	start := time.Now().Add(90 * time.Second).UTC().Truncate(time.Second)
	e.withEstimates(start)
	req := testRequest(1)
	req.Spec.MaximumExecutionTimeSeconds = ptr.To[int32](5401)
	req.Spec.PodSets[0].Template.Spec.Containers[0].Resources.Limits = v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("20"), "nvidia.com/gpu": resource.MustParse("1"),
	}

	before := time.Now().Truncate(time.Second)
	q := e.take(req)
	if q.Status.JobID != "" || len(e.plugin.createdPods()) != 0 || state(q) != "waiting" {
		t.Fatalf("submitted before the estimate was read: %s %+v", state(q), q.Status)
	}
	if q.Status.LastSyncTime == nil || q.Status.Reason != "" || q.Status.Message != "" {
		t.Errorf("status %+v, want a heartbeat and no reason", q.Status)
	}
	est := q.Status.Estimate
	if est == nil || est.Nodes != 1 || est.StartTime == nil || !est.StartTime.Time.Equal(start) || est.Reason != "" ||
		est.ProbeTime.Time.Before(before) {
		t.Fatalf("estimate %+v, want 1 node at %s asked after %s", est, start, before)
	}
	calls := e.plugin.estimateCalls()
	want := EstimateRequest{Nodes: 1, GPUsPerNode: 1, CPUsPerNode: 20, Time: "01:31:00", Flags: flavorFlags}
	if len(calls) != 1 || calls[0] != want {
		t.Fatalf("/estimate calls %+v, want %+v", calls, want)
	}

	q = e.poll()
	if q.Status.JobID != executorJobID || state(q) != "queued" || len(e.plugin.createdPods()) != 1 {
		t.Fatalf("not submitted after the estimate: %s %+v", state(q), q.Status)
	}
	if len(e.plugin.estimateCalls()) != 1 || q.Status.Estimate == nil || !q.Status.Estimate.StartTime.Time.Equal(start) {
		t.Errorf("the estimate was asked again or lost: %d calls, %+v", len(e.plugin.estimateCalls()), q.Status.Estimate)
	}
}

// A pod group asks for one node per member, the most GPUs and CPUs of its
// PodSets, and the flavor's flags after the template's own.
func TestEstimateShapeOfPodGroup(t *testing.T) {
	e := newExecutorEnv(t)
	e.withEstimates(time.Now().UTC().Truncate(time.Second))
	req := testRequest(2)
	head := &req.Spec.PodSets[0]
	head.Template.Annotations = map[string]string{flagsKey: "--constraint=highmem"}
	head.Template.Spec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceCPU: resource.MustParse("40"), "nvidia.com/gpu": resource.MustParse("2")}
	worker := head.DeepCopy()
	worker.Name, worker.Count = "worker", 1
	worker.Template.Spec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceCPU: resource.MustParse("2500m"), "nvidia.com/gpu": resource.MustParse("4")}
	req.Spec.PodSets = append(req.Spec.PodSets, *worker)

	q := e.take(req)
	if q.Status.Estimate == nil || q.Status.Estimate.Nodes != 3 {
		t.Fatalf("estimate %+v, want 3 nodes", q.Status.Estimate)
	}
	want := EstimateRequest{Nodes: 3, GPUsPerNode: 4, CPUsPerNode: 40, Flags: "--constraint=highmem " + flavorFlags}
	if calls := e.plugin.estimateCalls(); len(calls) != 1 || calls[0] != want {
		t.Fatalf("/estimate calls %+v, want %+v", calls, want)
	}
}

// With site Resources, GPUs count under their aliases too, and the CPUs are the
// ones the executor gives the member.
func TestEstimateShapeSized(t *testing.T) {
	res := &SiteResources{GPUResource: "nvidia.com/gpu", GPUAliases: []string{"example.org/hpc-gpu"}, CPUsPerGPU: 20, GPUsPerMember: 4}
	ps := v1alpha1.PodSet{Name: "main", Count: 1, Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
		Name: "main", Resources: v1.ResourceRequirements{Limits: v1.ResourceList{"example.org/hpc-gpu": resource.MustParse("2")}},
	}}}}}
	withSiteResources(&ps, res)
	req := jobRequest{WorkloadUID: executorWorkload, Owner: v1alpha1.OwnerReference{Name: "train"}, PodSets: []v1alpha1.PodSet{ps}}
	got := estimateShape(req, res, ptr.To[int32](60))
	want := EstimateRequest{Nodes: 1, GPUsPerNode: 2, CPUsPerNode: 40, Time: "00:01:00"}
	if got != want {
		t.Errorf("shape %+v, want %+v", got, want)
	}
	// Without Resources, only <vendor>/gpu resources are GPUs.
	ps.Template.Spec.Containers[0].Resources = v1.ResourceRequirements{Requests: v1.ResourceList{"amd.com/gpu": resource.MustParse("1"), "example.org/hpc-gpu": resource.MustParse("3")}}
	req.PodSets = []v1alpha1.PodSet{ps}
	if got := estimateShape(req, nil, nil); got.GPUsPerNode != 1 || got.Time != "" || got.CPUsPerNode != 0 {
		t.Errorf("shape %+v, want 1 GPU, no time, no CPU", got)
	}
}

// A site that takes no further job of the user now gives no start time: the
// estimate records why, and nothing is created on that pass.
func TestEstimateSubmitLimit(t *testing.T) {
	e := newExecutorEnv(t)
	e.withEstimates(time.Time{})
	text := `sbatch --test-only: exit status 1: sbatch: error: QOSMaxSubmitJobPerUserLimit
sbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits) "quoted"`
	e.plugin.with(func(p *fakePlugin) { p.estimateCode, p.estimateText = http.StatusBadGateway, text })

	q := e.take(testRequest(1))
	est := q.Status.Estimate
	if q.Status.JobID != "" || len(e.plugin.createdPods()) != 0 || est == nil || est.StartTime != nil || est.Nodes != 1 {
		t.Fatalf("status %+v, estimate %+v, want an estimate without a start time and no job", q.Status, est)
	}
	if est.Reason != "QOSMaxSubmitJobPerUserLimit" || !strings.Contains(est.Message, `job submit limit, user's size and/or time limits) "quoted"`) {
		t.Errorf("estimate reason %q message %q", est.Reason, est.Message)
	}
}

// Any other failure to answer leaves the job without an estimate: it is
// submitted on the same pass, as on a plugin without the capability.
func TestEstimateFailureSubmits(t *testing.T) {
	for _, tt := range []struct {
		name string
		code int
		text string
	}{
		{"no route", http.StatusNotFound, "404 page not found"},
		{"invalid flags", http.StatusBadRequest, `flags: "a&b" has characters other than ...`},
		{"sbatch refuses", http.StatusBadGateway, "sbatch --test-only: exit status 1: sbatch: error: invalid account"},
		{"time zone", http.StatusInternalServerError, "SiteTimeZone: unknown time zone Mars/Olympus"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			e.withEstimates(time.Time{})
			e.plugin.with(func(p *fakePlugin) { p.estimateCode, p.estimateText = tt.code, tt.text })
			q := e.submit(testRequest(1))
			if q.Status.Estimate != nil || len(e.plugin.estimateCalls()) != 1 {
				t.Errorf("estimate %+v after %d calls, want none after one", q.Status.Estimate, len(e.plugin.estimateCalls()))
			}
		})
	}
	t.Run("start missing", func(t *testing.T) {
		e := newExecutorEnv(t)
		e.withEstimates(time.Time{})
		if q := e.submit(testRequest(1)); q.Status.Estimate != nil {
			t.Errorf("estimate %+v from an answer without a start", q.Status.Estimate)
		}
	})
}

// A failure before the stage-in leaves the attempt without an estimate: the
// site is not asked again while the stage-in runs, nor once it ended.
func TestEstimateFailureBeforeStageIn(t *testing.T) {
	e := newExecutorEnv(t)
	e.enableStaging()
	e.withEstimates(time.Time{})
	e.plugin.with(func(p *fakePlugin) {
		p.estimateCode, p.estimateText = http.StatusBadGateway, "sbatch --test-only: exit status 255: ssh: connection reset"
	})

	e.add(stagedRequest(StageInAnnotation, "/shared/alice/data"))
	q := e.poll()
	if state(q) != "staging in" || q.Status.Estimate != nil {
		t.Fatalf("after the failed estimate: %s %+v", state(q), q.Status)
	}
	e.plugin.with(func(p *fakePlugin) { p.estimateCode, p.estimateStart = 0, time.Now().Add(2*time.Hour) })
	for range 2 {
		if q = e.poll(); state(q) != "staging in" || q.Status.Estimate != nil {
			t.Fatalf("while staging: %s %+v", state(q), q.Status)
		}
	}
	e.endStageJob(stageInJob, batchv1.JobComplete, "")
	if q = e.poll(); q.Status.JobID != executorJobID || q.Status.Estimate != nil {
		t.Fatalf("after the stage-in: %s %+v", state(q), q.Status)
	}
	if n := len(e.plugin.estimateCalls()); n != 1 {
		t.Errorf("%d /estimate calls, want 1", n)
	}
}

// A plugin that does not list the capability is never asked.
func TestEstimateNeedsTheCapability(t *testing.T) {
	e := newExecutorEnv(t)
	e.plugin.with(func(p *fakePlugin) { p.estimateStart = time.Now() })
	if q := e.submit(testRequest(1)); q.Status.Estimate != nil || len(e.plugin.estimateCalls()) != 0 {
		t.Errorf("estimate %+v, %d calls, from a plugin without the capability", q.Status.Estimate, len(e.plugin.estimateCalls()))
	}
}

// Once the offload controller sets the check to Retry over the estimate, the
// next pass submits nothing: it waits for the request to be deleted.
func TestEstimateDeferredIsNotSubmitted(t *testing.T) {
	e := newExecutorEnv(t)
	e.withEstimates(time.Now().Add(2 * time.Hour))
	q := e.take(testRequest(1))
	if q.Status.Estimate == nil || q.Status.JobID != "" {
		t.Fatalf("status %+v, want an estimate and no job", q.Status)
	}
	wl := &kueue.Workload{}
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: executorNS, Name: workloadName}, wl); err != nil {
		t.Fatal(err)
	}
	wl.Status.AdmissionChecks[0].State = kueue.CheckStateRetry
	if err := e.c.Status().Update(context.Background(), wl); err != nil {
		t.Fatal(err)
	}
	q = e.poll()
	waitingWith(t, q, "is Retry, not Pending", true)
	if len(e.plugin.createdPods()) != 0 || len(e.plugin.estimateCalls()) != 1 {
		t.Fatalf("%d /create and %d /estimate calls after the Retry", len(e.plugin.createdPods()), len(e.plugin.estimateCalls()))
	}
}
