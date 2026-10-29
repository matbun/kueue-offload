package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// SiteConfigSpec configures the dispatcher of one site. An AdmissionCheck with
// controllerName interlink.eu/dispatch selects it through its parameters.
type SiteConfigSpec struct {
	// agentService is the name of the agent Service in every tenant namespace.
	// +kubebuilder:default=interlink-agent
	// +optional
	AgentService string `json:"agentService,omitempty"`

	// agentPort is the port of the agent Service.
	// +kubebuilder:default=8000
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	AgentPort int32 `json:"agentPort,omitempty"`

	// pollInterval is how often the dispatcher asks the agent for the state of
	// a submitted job.
	// +kubebuilder:default="15s"
	// +optional
	PollInterval *metav1.Duration `json:"pollInterval,omitempty"`

	// agentLostTimeout is how long an agent may stay unreachable before the
	// check is set to Retry.
	// +kubebuilder:default="10m"
	// +optional
	AgentLostTimeout *metav1.Duration `json:"agentLostTimeout,omitempty"`

	// maxRetries bounds how many times a job cancelled on the site is
	// resubmitted before the check is Rejected.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxRetries *int32 `json:"maxRetries,omitempty"`

	// accountRule derives the Slurm account from the ClusterQueue name.
	// +optional
	AccountRule AccountRule `json:"accountRule,omitempty"`

	// flavorPartitions maps a ResourceFlavor name to a Slurm partition. A
	// flavor that is not listed gives no --partition flag.
	// +optional
	FlavorPartitions map[string]string `json:"flavorPartitions,omitempty"`

	// alwaysShadow asks the agents to render a shadow for every job, also for
	// jobs without exposed ports, so that logs are always reachable.
	// +optional
	AlwaysShadow bool `json:"alwaysShadow,omitempty"`

	// mirrorPodStatus mirrors per-container status onto the owner object.
	// +optional
	MirrorPodStatus bool `json:"mirrorPodStatus,omitempty"`

	// membership configures the membership sync controller for this site.
	// +optional
	Membership *Membership `json:"membership,omitempty"`
}

// AccountRule derives a Slurm account from a ClusterQueue name.
type AccountRule struct {
	// stripPrefix is removed from the ClusterQueue name to give the account.
	// Unset means "<site>-", where site is the SiteConfig name. A ClusterQueue
	// whose name does not start with the prefix is rejected.
	// +optional
	StripPrefix *string `json:"stripPrefix,omitempty"`
}

// Membership holds the settings of the membership sync controller.
type Membership struct {
	// slurmCluster is the value of the cluster column of the associations that
	// belong to this site. Rows of other clusters are ignored.
	// +kubebuilder:validation:MinLength=1
	SlurmCluster string `json:"slurmCluster"`

	// associations holds the output of
	// sacctmgr --parsable2 --noheader show assoc format=cluster,account,user.
	// Rows with an empty user are account rows and are skipped.
	Associations ConfigMapKeyRef `json:"associations"`

	// agentTemplate holds a Go text/template that renders the agent objects
	// of one tenant namespace as multi-document YAML. It may produce only
	// namespaced objects. Unset means no agent objects are rendered.
	// +optional
	AgentTemplate *ConfigMapKeyRef `json:"agentTemplate,omitempty"`

	// agentImage is passed to the agent template as .Image.
	// +optional
	AgentImage string `json:"agentImage,omitempty"`

	// agentTokenReview also binds every ServiceAccount the agent template
	// renders to system:auth-delegator, with one ClusterRoleBinding per tenant
	// namespace, so that the agent can validate the dispatcher's token.
	// +optional
	AgentTokenReview bool `json:"agentTokenReview,omitempty"`

	// cohort is the Kueue Cohort of the site's ClusterQueues. Unset means the
	// site name.
	// +optional
	Cohort string `json:"cohort,omitempty"`

	// admissionCheck is the AdmissionCheck every ClusterQueue of the site
	// carries. Unset means the site name.
	// +optional
	AdmissionCheck string `json:"admissionCheck,omitempty"`

	// defaultProjectQuota is the quota of a project that has no ProjectQuota.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	// +optional
	DefaultProjectQuota []kueue.ResourceGroup `json:"defaultProjectQuota,omitempty"`

	// syncInterval is how often the memberships are synchronised when
	// nothing changed.
	// +kubebuilder:default="15m"
	// +optional
	SyncInterval *metav1.Duration `json:"syncInterval,omitempty"`
}

// ConfigMapKeyRef selects one key of a ConfigMap. The membership controller
// reads ConfigMaps only in its own namespace.
type ConfigMapKeyRef struct {
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Agent",type=string,JSONPath=`.spec.agentService`
// +kubebuilder:printcolumn:name="Poll",type=string,JSONPath=`.spec.pollInterval`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SiteConfig is the configuration of one HPC site for the dispatcher.
type SiteConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec SiteConfigSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// SiteConfigList is a list of SiteConfig.
type SiteConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SiteConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SiteConfig{}, &SiteConfigList{})
}
