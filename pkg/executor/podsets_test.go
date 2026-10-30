package executor

import (
	"net/http"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

const workerCtn = "worker"

// twoPodSetRequest is a workload of a PodSet of drivers and one of workers, each
// with its own container.
func twoPodSetRequest(drivers, workers int32) *v1alpha1.OffloadRequest {
	req := testRequest(drivers)
	req.Spec.PodSets[0].Name = "driver"
	req.Spec.PodSets[0].Template.Spec.Containers[0].Command = []string{"sh", "-c", "echo driver"}
	req.Spec.PodSets = append(req.Spec.PodSets, v1alpha1.PodSet{
		Name:        "worker",
		Flavors:     testFlavors(),
		Count:       workers,
		Annotations: map[string]string{flagsKey: flavorFlags},
		Template: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{
			Name:    workerCtn,
			Image:   "docker://alpine",
			Command: []string{"sh", "-c", "echo worker"},
		}}}},
	})
	return req
}

func TestSeveralPodSetsAreOnePodGroup(t *testing.T) {
	e := newExecutorEnv(t)
	req := twoPodSetRequest(2, 1)
	if q := e.submit(req); q.Status.JobID != executorJobID {
		t.Fatalf("jobID = %q", q.Status.JobID)
	}

	pods := e.plugin.createdPods()
	if len(pods) != 3 {
		t.Fatalf("%d /create calls, want 3", len(pods))
	}
	for i, want := range []struct{ podSet, role, command string }{
		{"driver", GangRoleHead, "echo driver"}, {"driver", GangRoleWorker, "echo driver"}, {"worker", GangRoleWorker, "echo worker"},
	} {
		m := pods[i]
		if m.Labels[JobCompletionIndexLabel] != string(rune('0'+i)) || m.Annotations[PodSetAnnotation] != want.podSet ||
			m.Labels[PodGroupNameLabel] != executorWorkload || m.Annotations[PodGroupTotalCountAnnotation] != "3" ||
			m.Annotations[GangRoleAnnotation] != want.role || string(m.UID) != memberUID(executorWorkload, i) {
			t.Errorf("member %d: labels %v annotations %v", i, m.Labels, m.Annotations)
		}
		if got := strings.Join(m.Spec.Containers[0].Command, " "); !strings.Contains(got, want.command) {
			t.Errorf("member %d runs %q, want %q", i, got, want.command)
		}
	}
	if env := envOf(pods[2].Spec.Containers[0]); env[RankEnv] != "2" || env[WorldSizeEnv] != "3" {
		t.Errorf("member 2 env %v", env)
	}

	if q := e.poll(); len(q.Status.Ranks) != 3 || q.Status.Ranks[2].PodUID != memberUID(executorWorkload, 2) {
		t.Fatalf("ranks: %+v", q.Status.Ranks)
	}

	// Each member's log is its own container's.
	e.setLog(0, executorCtn, "zero\n")
	e.setLog(1, executorCtn, "one\n")
	e.setLog(2, workerCtn, "two\n")
	rec := e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload)
	if want := "[rank 0] zero\n[rank 1] one\n[rank 2] two\n"; rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Fatalf("logs = %d %q, want %q", rec.Code, rec.Body.String(), want)
	}
	rec = e.do(http.MethodGet, "/getLogs?workloadUID="+executorWorkload+"&container="+workerCtn)
	if want := "[rank 2] two\n"; rec.Body.String() != want {
		t.Fatalf("worker logs = %q, want %q", rec.Body.String(), want)
	}

	if err := e.remove(); err != nil {
		t.Fatal(err)
	}
	if n := len(e.plugin.deletedUIDs()); n != 3 {
		t.Errorf("%d members deleted, want 3", n)
	}
}

// Every member keeps its own PodSet's resources; sizing the one allocation is the
// plugin's.
func TestPodSetsKeepTheirResources(t *testing.T) {
	e := newExecutorEnv(t)
	req := twoPodSetRequest(1, 1)
	req.Spec.PodSets[0].Template.Spec.Containers[0].Resources.Limits = v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("2"), v1.ResourceMemory: resource.MustParse("8Gi"),
	}
	req.Spec.PodSets[1].Template.Spec.Containers[0].Resources.Limits = v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("16"), v1.ResourceMemory: resource.MustParse("4Gi"), "nvidia.com/gpu": resource.MustParse("2"),
	}
	e.submit(req)
	pods := e.plugin.createdPods()
	if got := pods[0].Spec.Containers[0].Resources.Limits.Cpu().String(); got != "2" {
		t.Errorf("driver CPUs %s", got)
	}
	if got := pods[1].Spec.Containers[0].Resources.Limits.Name("nvidia.com/gpu", resource.DecimalSI).String(); got != "2" {
		t.Errorf("worker GPUs %s", got)
	}
}

func TestPodSetsRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1alpha1.OffloadRequest)
		want   string
	}{
		{"different queues", func(r *v1alpha1.OffloadRequest) {
			r.Spec.PodSets[1].Annotations = map[string]string{flagsKey: "--account=proj42 --partition=gpu --qos=prod"}
		}, "annotations differ from PodSet driver's"},
		{"launcher with different images", func(r *v1alpha1.OffloadRequest) {
			r.Spec.PodSets[0].Template.Spec.Containers[0].Command = r.Spec.PodSets[1].Template.Spec.Containers[0].Command
			r.Spec.PodSets[1].Template.Annotations = map[string]string{LauncherAnnotation: "mpi"}
		}, "same image and command"},
		{"launcher with different commands", func(r *v1alpha1.OffloadRequest) {
			r.Spec.PodSets[1].Template.Spec.Containers[0].Image = r.Spec.PodSets[0].Template.Spec.Containers[0].Image
			r.Spec.PodSets[0].Template.Annotations = map[string]string{LauncherAnnotation: "mpi"}
		}, "same image and command"},
		{"too many members", func(r *v1alpha1.OffloadRequest) {
			r.Spec.PodSets[0].Count, r.Spec.PodSets[1].Count = maxWorkloadMembers, 1
		}, "more than"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			req := twoPodSetRequest(1, 1)
			tt.mutate(req)
			e.refusedWith(e.take(req), tt.want)
			if n := len(e.plugin.createdPods()); n != 0 {
				t.Fatalf("the plugin got %d pods", n)
			}
		})
	}

	// The same container in every PodSet runs under a launcher.
	req := twoPodSetRequest(1, 1)
	req.Spec.PodSets[1].Template.Spec.Containers[0] = *req.Spec.PodSets[0].Template.Spec.Containers[0].DeepCopy()
	req.Spec.PodSets[1].Template.ObjectMeta = metav1.ObjectMeta{Annotations: map[string]string{LauncherAnnotation: "mpi"}}
	if aerr := validatePodSetsShareAllocation(req.Spec.PodSets); aerr != nil {
		t.Fatalf("same container refused: %s", aerr.msg)
	}
}

// Each PodSet's references are read and resolved for its own members.
func TestPodSetsResolveTheirOwnReferences(t *testing.T) {
	e := newExecutorEnv(t, &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "hf-token", Namespace: executorNS}, Data: map[string][]byte{"token": []byte("s3cr3t")}})
	req := twoPodSetRequest(1, 1)
	ps := &req.Spec.PodSets[1]
	ps.SecretRefs = []string{"hf-token"}
	ps.Template.Spec.Containers[0].Env = []v1.EnvVar{{Name: "HF_TOKEN", ValueFrom: &v1.EnvVarSource{SecretKeyRef: &v1.SecretKeySelector{
		LocalObjectReference: v1.LocalObjectReference{Name: "hf-token"}, Key: "token",
	}}}}
	e.submit(req)
	pods := e.plugin.createdPods()
	if env := envOf(pods[1].Spec.Containers[0]); env["HF_TOKEN"] != "s3cr3t" {
		t.Fatalf("worker env %v", env)
	}
	if _, ok := envOf(pods[0].Spec.Containers[0])["HF_TOKEN"]; ok {
		t.Fatalf("the driver got the worker's Secret")
	}
}
