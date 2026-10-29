package offload

import (
	"net/http"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/pkg/executorapi"
)

var customKinds = []ownerKind{jobSetKind, trainJobKind}

// terminalTypes are the condition types Kueue's integrations read, completed
// then failed.
var terminalTypes = map[string][2]string{"JobSet": {"Completed", "Failed"}, "TrainJob": {"Complete", "Failed"}}

// condition returns the condition of type ty on an unstructured owner.
func condition(t *testing.T, u *unstructured.Unstructured, ty string) map[string]any {
	t.Helper()
	conds, _, err := unstructured.NestedSlice(u.Object, "status", "conditions")
	if err != nil {
		t.Fatal(err)
	}
	var found map[string]any
	for _, raw := range conds {
		if c, _ := raw.(map[string]any); c["type"] == ty {
			if found != nil {
				t.Errorf("condition %s set twice", ty)
			}
			found = c
		}
	}
	return found
}

func TestCustomOwnerSubmit(t *testing.T) {
	for _, k := range customKinds {
		t.Run(k.gvk.Kind, func(t *testing.T) {
			e := newEnv(t, customOwned(k))
			e.reconcile()
			if s, _, _ := e.executor.counts(); s != 1 {
				t.Fatalf("submits = %d, want 1", s)
			}
			req := e.executor.submits[0]
			want := executorapi.OwnerReference{APIVersion: k.gvk.GroupVersion().String(), Kind: k.gvk.Kind, Name: "train", UID: string(customUID)}
			if req.Owner != want {
				t.Errorf("owner = %+v, want %+v", req.Owner, want)
			}
			ann := req.PodSets[0].Template.Annotations
			if ann["slurm-job.vk.io/flags"] != "--time=02:00:00" {
				t.Errorf("owner annotations not merged: %v", ann)
			}
			if ann["interlink.eu/mode"] != "template" {
				t.Errorf("template annotation must win over the owner's, got %q", ann["interlink.eu/mode"])
			}
			if _, ok := ann["example.org/unrelated"]; ok {
				t.Errorf("annotation outside the forwarded prefixes was merged")
			}
			if conds, _, _ := unstructured.NestedSlice(e.custom(k).Object, "status", "conditions"); len(conds) != 0 {
				t.Errorf("owner patched while queued: %v", conds)
			}
		})
	}
}

func TestCustomOwnerTerminal(t *testing.T) {
	cases := []struct {
		name     string
		job      executorapi.JobStatus
		failed   bool
		terminal string
		message  string
	}{
		{"completed", executorapi.JobStatus{State: executorapi.StateCompleted}, false, "Completed", "Slurm job 123 completed"},
		{"failed", executorapi.JobStatus{State: executorapi.StateFailed, Message: "rank 1 exited 3"}, true, "Failed", "Slurm job 123 failed: rank 1 exited 3"},
	}
	for _, k := range customKinds {
		for _, tc := range cases {
			t.Run(k.gvk.Kind+"/"+tc.name, func(t *testing.T) {
				e := newEnv(t, customOwned(k))
				e.reconcile()
				tc.job.APIVersion, tc.job.JobID = executorapi.APIVersion, "123"
				e.executor.set(func(a *fakeExecutor) { a.job = tc.job })
				e.reconcile()
				u := e.custom(k)
				types := terminalTypes[k.gvk.Kind]
				ty, other := types[0], types[1]
				if tc.failed {
					ty, other = other, ty
				}
				c := condition(t, u, ty)
				if c == nil || c["status"] != "True" || c["reason"] != ReasonOffload || c["message"] != tc.message || c["lastTransitionTime"] == nil {
					t.Errorf("condition %s = %v, want True %s %q", ty, c, ReasonOffload, tc.message)
				}
				if condition(t, u, other) != nil {
					t.Errorf("condition %s also set", other)
				}
				state, _, _ := unstructured.NestedString(u.Object, "status", "terminalState")
				if k.terminalState && state != tc.terminal {
					t.Errorf("terminalState = %q, want %q", state, tc.terminal)
				}
				if !k.terminalState && state != "" {
					t.Errorf("%s got terminalState %q", k.gvk.Kind, state)
				}
				if st := e.check("hpc-a"); st.State != kueue.CheckStatePending {
					t.Errorf("check = %s, want Pending (D1)", st.State)
				}
				if len(e.job().Status.Conditions) != 0 {
					t.Errorf("an unrelated Job was patched")
				}
			})
		}
	}
}

func TestCustomOwnerRejected(t *testing.T) {
	const msg = "sbatch: error: Invalid account or account/partition combination specified"
	for _, k := range customKinds {
		t.Run(k.gvk.Kind, func(t *testing.T) {
			e := newEnv(t, customOwned(k))
			e.executor.set(func(a *fakeExecutor) {
				a.submitCode = http.StatusBadRequest
				a.submitBody = executorapi.ErrorResponse{APIVersion: executorapi.APIVersion, Permanent: true, Message: msg}
			})
			e.reconcile()
			if st := e.check("hpc-a"); st.State != kueue.CheckStateRejected || st.Message != msg {
				t.Errorf("check = %s %q", st.State, st.Message)
			}
			c := condition(t, e.custom(k), "Failed")
			if c == nil || c["status"] != "True" || c["reason"] != ReasonRejected || c["message"] != msg {
				t.Errorf("condition Failed = %v, want True %s", c, ReasonRejected)
			}
		})
	}
}
