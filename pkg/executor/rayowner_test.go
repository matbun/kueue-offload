package executor

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/offload"
)

// A Ray owner's request carries PodSets the offload controller prepared: the
// launcher, a RayCluster head's ports and labels, a RayJob's entrypoint in place
// of its submitter. The executor prepares the Workload's the same way, reading
// the RayJob itself, and takes the request only when the two are equal.
func TestWorkloadCheckRayOwners(t *testing.T) {
	for _, kind := range []string{"RayCluster", "RayJob"} {
		t.Run(kind, func(t *testing.T) {
			e := newExecutorEnv(t)
			ctx := context.Background()
			owner := &unstructured.Unstructured{}
			owner.SetAPIVersion("ray.io/v1")
			owner.SetKind(kind)
			owner.SetNamespace(executorNS)
			owner.SetName("ray")
			owner.SetUID(types.UID("ray-uid"))
			if kind == "RayJob" {
				if err := unstructured.SetNestedField(owner.Object, "python train.py", "spec", "entrypoint"); err != nil {
					t.Fatal(err)
				}
			}
			if err := e.c.Create(ctx, owner); err != nil {
				t.Fatal(err)
			}

			req := testRequest(1)
			main := req.Spec.PodSets[0]
			head, worker := *main.DeepCopy(), *main.DeepCopy()
			head.Name, worker.Name = "head", "worker"
			req.Spec.PodSets = []v1alpha1.PodSet{head, worker}
			wl := workloadOf(req)
			wl.OwnerReferences = []metav1.OwnerReference{{APIVersion: "ray.io/v1", Kind: kind, Name: "ray", UID: owner.GetUID(), Controller: ptr.To(true)}}
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
			want, _, err := offload.RequestPodSets(wl, func(*metav1.OwnerReference) (metav1.Object, error) { return owner, nil })
			if err != nil {
				t.Fatal(err)
			}
			for i := range req.Spec.PodSets {
				req.Spec.PodSets[i].Template = want[i].Template
			}
			if r := e.d.checkWorkload(ctx, req); r != nil {
				t.Fatalf("the controller's request was refused: %s", r.msg)
			}
			if kind != "RayJob" {
				return
			}
			// another entrypoint than the RayJob's
			cmd := req.Spec.PodSets[0].Template.Spec.Containers[0].Command
			cmd[len(cmd)-1] = strings.Replace(cmd[len(cmd)-1], "train.py", "other.py", 1)
			if r := e.d.checkWorkload(ctx, req); r == nil || !strings.Contains(r.msg, "PodSet head: the template is not the Workload's") {
				t.Fatalf("another entrypoint: %+v", r)
			}
		})
	}
}
