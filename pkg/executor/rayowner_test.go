package executor

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/offload"
)

// A Ray owner's request carries PodSets the offload controller prepared: the
// launcher and the head's dashboard and client ports, without a RayJob's
// submitter, which runs in the cluster. The executor prepares the Workload's
// the same way and takes the request only when the two are equal.
func TestWorkloadCheckRayOwners(t *testing.T) {
	for _, kind := range []string{"RayCluster", "RayJob"} {
		t.Run(kind, func(t *testing.T) {
			e := newExecutorEnv(t)
			ctx := context.Background()

			req := testRequest(1)
			main := req.Spec.PodSets[0]
			head, worker := *main.DeepCopy(), *main.DeepCopy()
			head.Name, worker.Name = "head", "worker"
			req.Spec.PodSets = []v1alpha1.PodSet{head, worker}
			wl := workloadOf(req)
			wl.OwnerReferences = []metav1.OwnerReference{{APIVersion: "ray.io/v1", Kind: kind, Name: "ray", UID: types.UID("ray-uid"), Controller: ptr.To(true)}}
			if kind == "RayJob" {
				submitter := kueue.PodSet{Name: "submitter", Count: 1, Template: *main.Template.DeepCopy()}
				wl.Spec.PodSets = append(wl.Spec.PodSets, submitter)
				wl.Status.Admission.PodSetAssignments = append(wl.Status.Admission.PodSetAssignments,
					kueue.PodSetAssignment{Name: "submitter", Flavors: main.Flavors, Count: ptr.To[int32](1)})
			}
			if err := e.c.Create(ctx, wl); err != nil {
				t.Fatal(err)
			}

			// As the Workload holds them, the templates are not what the controller sends
			if r := e.d.checkWorkload(ctx, req); r == nil {
				t.Fatal("a request with the Workload's own templates was taken")
			}
			want, _ := offload.RequestPodSets(wl)
			for i := range req.Spec.PodSets {
				req.Spec.PodSets[i].Template = *want[i].Template.DeepCopy()
			}
			if r := e.d.checkWorkload(ctx, req); r != nil {
				t.Fatalf("the controller's request was refused: %s", r.msg)
			}
			// a head without the ports the shadow forwards
			ctr := &req.Spec.PodSets[0].Template.Spec.Containers[0]
			ctr.Ports = ctr.Ports[:len(ctr.Ports)-1]
			if r := e.d.checkWorkload(ctx, req); r == nil || !strings.Contains(r.msg, "PodSet head: the template is not the Workload's") {
				t.Fatalf("a head without the client port: %+v", r)
			}
			if kind != "RayJob" {
				return
			}
			// the submitter sent along
			req.Spec.PodSets[0].Template = *want[0].Template.DeepCopy()
			req.Spec.PodSets = append(req.Spec.PodSets, v1alpha1.PodSet{Name: "submitter", Count: 1, Flavors: main.Flavors, Template: *main.Template.DeepCopy()})
			if r := e.d.checkWorkload(ctx, req); r == nil || !strings.Contains(r.msg, "3 PodSets, the Workload has 2") {
				t.Fatalf("a request with the submitter: %+v", r)
			}
		})
	}
}
