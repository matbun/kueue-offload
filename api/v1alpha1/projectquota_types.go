package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// ProjectQuotaSpec is the concurrent quota of one project on one site.
type ProjectQuotaSpec struct {
	// +kubebuilder:validation:MinLength=1
	Site string `json:"site"`
	// account is the Slurm account of the project.
	// +kubebuilder:validation:MinLength=1
	Account string `json:"account"`
	// resourceGroups become the resourceGroups of the project's ClusterQueue.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	// +optional
	ResourceGroups []kueue.ResourceGroup `json:"resourceGroups,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:validation:XValidation:rule="self.metadata.name == self.spec.site + '-' + self.spec.account",message="metadata.name must be <site>-<account>"
// +kubebuilder:printcolumn:name="Site",type=string,JSONPath=`.spec.site`
// +kubebuilder:printcolumn:name="Account",type=string,JSONPath=`.spec.account`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ProjectQuota overrides the site's default quota for one project. It is
// named <site>-<account>. Quota is an administrator's number and is never
// derived from Slurm.
type ProjectQuota struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ProjectQuotaSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// ProjectQuotaList is a list of ProjectQuota.
type ProjectQuotaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ProjectQuota `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ProjectQuota{}, &ProjectQuotaList{})
}
