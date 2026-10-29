package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IdentityEntry maps one Kubernetes user to their HPC user on one site.
type IdentityEntry struct {
	// kubernetesUser is the Kubernetes identity, such as an OIDC subject.
	// +kubebuilder:validation:MinLength=1
	KubernetesUser string `json:"kubernetesUser"`
	// namespace is the user's namespace. Unset means kubernetesUser.
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// +kubebuilder:validation:MinLength=1
	Site string `json:"site"`
	// hpcUser is the user name on the site, as in the user column of the
	// Slurm associations.
	// +kubebuilder:validation:MinLength=1
	HPCUser string `json:"hpcUser"`
}

// IdentityMapSpec lists identity mappings. Entries of every IdentityMap are
// merged.
type IdentityMapSpec struct {
	// +listType=atomic
	// +optional
	Entries []IdentityEntry `json:"entries,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// IdentityMap maps Kubernetes users to HPC users. The membership controller
// reads it and never infers a mapping from names.
type IdentityMap struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec IdentityMapSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// IdentityMapList is a list of IdentityMap.
type IdentityMapList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []IdentityMap `json:"items"`
}

func init() {
	SchemeBuilder.Register(&IdentityMap{}, &IdentityMapList{})
}
