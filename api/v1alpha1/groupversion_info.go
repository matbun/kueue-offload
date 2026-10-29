// Package v1alpha1 contains the interlink.eu/v1alpha1 API types of the offload controller.
// +kubebuilder:object:generate=true
// +groupName=interlink.eu
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group and version of these types.
	GroupVersion = schema.GroupVersion{Group: "interlink.eu", Version: "v1alpha1"}

	// SchemeBuilder registers the types with a scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
