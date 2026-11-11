package offload

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

var customKinds = []ownerKind{jobSetKind, trainJobKind}

func TestCustomOwnerSubmit(t *testing.T) {
	for _, k := range customKinds {
		t.Run(k.gvk.Kind, func(t *testing.T) {
			e := newEnv(t, customOwned(k))
			e.reconcile()
			sp := e.request(1).Spec
			want := v1alpha1.OwnerReference{APIVersion: k.gvk.GroupVersion().String(), Kind: k.gvk.Kind, Name: "train", UID: customUID}
			if sp.Owner != want {
				t.Errorf("owner = %+v, want %+v", sp.Owner, want)
			}
			if want := map[string]string{"slurm-job.vk.io/flags": "--time=02:00:00", "interlink.eu/mode": "owner"}; !reflect.DeepEqual(sp.OwnerAnnotations, want) {
				t.Errorf("ownerAnnotations = %v, want %v", sp.OwnerAnnotations, want)
			}
			if conds, _, _ := unstructured.NestedSlice(e.custom(k).Object, "status", "conditions"); len(conds) != 0 {
				t.Errorf("owner patched while queued: %v", conds)
			}
		})
	}
}

// A JobSet's or a TrainJob's job ends on the check, Ready: Kueue's integration
// finishes the owner from its pods, which the node ends.
func TestCustomOwnerTerminal(t *testing.T) {
	cases := []struct {
		name    string
		failed  bool
		detail  string
		message string
	}{
		{"completed", false, "", "job 123 completed"},
		{"failed", true, "rank 1 exited 3", "job 123 failed: rank 1 exited 3"},
	}
	for _, k := range customKinds {
		for _, tc := range cases {
			t.Run(k.gvk.Kind+"/"+tc.name, func(t *testing.T) {
				e := newEnv(t, customOwned(k))
				e.submitted()
				e.report(1, e.finished(!tc.failed, tc.detail))
				e.reconcile()
				if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != tc.message {
					t.Errorf("check = %s %q, want Ready %q", st.State, st.Message, tc.message)
				}
				if st, ok := e.custom(k).Object["status"]; ok {
					t.Errorf("owner status written: %v", st)
				}
				e.readyWrites()
			})
		}
	}
}

func TestCustomOwnerFails(t *testing.T) {
	const msg = "sbatch: error: Invalid account or account/partition combination specified"
	for _, k := range customKinds {
		t.Run(k.gvk.Kind, func(t *testing.T) {
			e := newEnv(t, customOwned(k))
			e.reconcile()
			e.report(1, func(st *v1alpha1.OffloadRequestStatus) {
				e.condition(st, v1alpha1.OffloadRequestAccepted, metav1.ConditionFalse, "Refused", msg)
			})
			e.reconcile()
			if st := e.check("hpc-a"); st.State != kueue.CheckStateReady || st.Message != msg || e.failed() != msg {
				t.Errorf("check = %s %q, failed %q", st.State, st.Message, e.failed())
			}
			if st, ok := e.custom(k).Object["status"]; ok {
				t.Errorf("owner status written: %v", st)
			}
		})
	}
}
