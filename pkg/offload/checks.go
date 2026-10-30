package offload

import (
	"context"
	"encoding/json"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// FieldManager owns the offload controller's entry in status.admissionChecks.
const FieldManager = "kueue-offload"

// CheckWriter writes the state of one admission check on a Workload.
type CheckWriter interface {
	WriteCheck(ctx context.Context, wl *kueue.Workload, st kueue.AdmissionCheckState) error
}

// SSACheckWriter writes the check with a server-side apply patch on
// workloads/status that carries only this check's entry. status.admissionChecks
// is a list keyed by name, so the entries of other controllers are kept.
type SSACheckWriter struct {
	Client client.Client
}

type checkApply struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Metadata   checkApplyMeta   `json:"metadata"`
	Status     checkApplyStatus `json:"status"`
}

type checkApplyMeta struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type checkApplyStatus struct {
	AdmissionChecks []checkApplyEntry `json:"admissionChecks"`
}

type checkApplyEntry struct {
	Name                string `json:"name"`
	State               string `json:"state"`
	Message             string `json:"message"`
	LastTransitionTime  string `json:"lastTransitionTime"`
	RequeueAfterSeconds *int32 `json:"requeueAfterSeconds,omitempty"`
}

// checkApplyBody is the apply configuration for one check entry. It leaves out
// retryCount, which Kueue owns.
func checkApplyBody(wl *kueue.Workload, st kueue.AdmissionCheckState) ([]byte, error) {
	return json.Marshal(checkApply{
		APIVersion: kueue.GroupVersion.String(),
		Kind:       "Workload",
		Metadata:   checkApplyMeta{Name: wl.Name, Namespace: wl.Namespace},
		Status: checkApplyStatus{AdmissionChecks: []checkApplyEntry{{
			Name:                string(st.Name),
			State:               string(st.State),
			Message:             st.Message,
			LastTransitionTime:  st.LastTransitionTime.UTC().Format("2006-01-02T15:04:05Z"),
			RequeueAfterSeconds: st.RequeueAfterSeconds,
		}}},
	})
}

func (w *SSACheckWriter) WriteCheck(ctx context.Context, wl *kueue.Workload, st kueue.AdmissionCheckState) error {
	body, err := checkApplyBody(wl, st)
	if err != nil {
		return err
	}
	target := &kueue.Workload{}
	target.Name, target.Namespace = wl.Name, wl.Namespace
	return w.Client.Status().Patch(ctx, target, client.RawPatch(types.ApplyPatchType, body),
		client.FieldOwner(FieldManager), client.ForceOwnership)
}
