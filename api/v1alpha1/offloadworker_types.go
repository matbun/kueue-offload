package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// OffloadWorkerActive is the condition the AdmissionChecks of the worker's
// OffloadConfigs follow, as MultiKueueCluster's Active.
const OffloadWorkerActive = kueue.MultiKueueClusterActive

// OffloadWorkerSpec describes one site.
type OffloadWorkerSpec struct {
	// loginEndpoint is the host:port of the site's login service, which every
	// executor of the site reaches. The worker is Active while the offload
	// controller can open a TCP connection to it.
	// +kubebuilder:validation:MinLength=1
	LoginEndpoint string `json:"loginEndpoint"`
}

// OffloadWorkerStatus holds the worker's Active condition. It comes from
// site-level facts only, never from one tenant's executor, since an inactive
// check makes every ClusterQueue that uses it inactive.
type OffloadWorkerStatus struct {
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=16
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.spec.loginEndpoint`
// +kubebuilder:printcolumn:name="Active",type=string,JSONPath=`.status.conditions[?(@.type=="Active")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// OffloadWorker is one HPC site, named after it. It has MultiKueueCluster's
// role for a site with no Kubernetes API: OffloadConfigs name the workers their
// checks submit to, and a check is Active while one of them is.
type OffloadWorker struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OffloadWorkerSpec   `json:"spec,omitempty"`
	Status OffloadWorkerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// OffloadWorkerList is a list of OffloadWorker.
type OffloadWorkerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OffloadWorker `json:"items"`
}

func init() {
	SchemeBuilder.Register(&OffloadWorker{}, &OffloadWorkerList{})
}
