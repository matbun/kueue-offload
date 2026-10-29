package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkloadReference names the Kueue Workload a OffloadRecord belongs to.
type WorkloadReference struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// OffloadRecordSpec is fixed when the record is created, before the first
// submission.
type OffloadRecordSpec struct {
	Workload WorkloadReference `json:"workload"`
	Site     string            `json:"site"`
	Account  string            `json:"account,omitempty"`
	// executorEndpoint is the base URL of the executor the job was submitted to.
	ExecutorEndpoint string `json:"executorEndpoint"`
}

// OffloadRecordStatus is the offload controller's view of the job.
type OffloadRecordStatus struct {
	// jobID is the Slurm job ID. Empty when no job is held for the Workload.
	// +optional
	JobID string `json:"jobID,omitempty"`
	// state is the last state the executor reported, or the offload controller's own
	// outcome (submitted, rejected, lost).
	// +optional
	State string `json:"state,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	NodeName string `json:"nodeName,omitempty"`
	// retries counts resubmissions after the job was cancelled on the site or
	// the executor was lost.
	// +optional
	Retries int32 `json:"retries,omitempty"`
	// +optional
	LastPoll *metav1.Time `json:"lastPoll,omitempty"`
	// firstUnreachable is when the executor stopped answering. Unset while it
	// answers.
	// +optional
	FirstUnreachable *metav1.Time `json:"firstUnreachable,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Workload",type=string,JSONPath=`.spec.workload.name`
// +kubebuilder:printcolumn:name="Namespace",type=string,JSONPath=`.spec.workload.namespace`
// +kubebuilder:printcolumn:name="Job",type=string,JSONPath=`.status.jobID`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.nodeName`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// OffloadRecord is the offload controller's durable record of a submission. Its name is
// the Workload UID, so a restarted offload controller does not submit twice.
type OffloadRecord struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OffloadRecordSpec   `json:"spec,omitempty"`
	Status OffloadRecordStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// OffloadRecordList is a list of OffloadRecord.
type OffloadRecordList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OffloadRecord `json:"items"`
}

func init() {
	SchemeBuilder.Register(&OffloadRecord{}, &OffloadRecordList{})
}
