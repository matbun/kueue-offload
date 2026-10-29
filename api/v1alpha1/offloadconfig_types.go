package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// OffloadConfigSpec configures the offload controller of one site. An AdmissionCheck with
// controllerName interlink.eu/offload selects it through its parameters.
type OffloadConfigSpec struct {
	// executorService is the name of the executor Service in every tenant namespace.
	// +kubebuilder:default=kueue-offload-executor
	// +optional
	ExecutorService string `json:"executorService,omitempty"`

	// executorPort is the port of the executor Service.
	// +kubebuilder:default=8000
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	ExecutorPort int32 `json:"executorPort,omitempty"`

	// pollInterval is how often the offload controller asks the executor for the state of
	// a submitted job.
	// +kubebuilder:default="15s"
	// +optional
	PollInterval *metav1.Duration `json:"pollInterval,omitempty"`

	// executorLostTimeout is how long an executor may stay unreachable before the
	// check is set to Retry.
	// +kubebuilder:default="10m"
	// +optional
	ExecutorLostTimeout *metav1.Duration `json:"executorLostTimeout,omitempty"`

	// maxRetries bounds how many times a job cancelled on the site is
	// resubmitted before the check is Rejected.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxRetries *int32 `json:"maxRetries,omitempty"`

	// accountRule derives the Slurm account from the ClusterQueue name.
	// +optional
	AccountRule AccountRule `json:"accountRule,omitempty"`

	// flavors maps a ResourceFlavor name to the Slurm queue its quota stands for:
	// a partition and a QOS. A project with several queues on one machine (say a
	// debug, a production and an interactive QOS) is one ClusterQueue with one
	// flavor each, and the flavor Kueue assigns decides where the job is submitted.
	// A flavor that is not listed gives neither flag.
	// +optional
	Flavors map[string]FlavorTarget `json:"flavors,omitempty"`

	// alwaysShadow asks the executors to render a shadow for every job, also for
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

// FlavorTarget is the Slurm queue a ResourceFlavor stands for.
type FlavorTarget struct {
	// partition is passed as --partition; empty gives no flag.
	// +optional
	Partition string `json:"partition,omitempty"`
	// qos is passed as --qos; empty gives no flag.
	// +optional
	QOS string `json:"qos,omitempty"`
}

// AccountRule derives a Slurm account from a ClusterQueue name.
type AccountRule struct {
	// stripPrefix is removed from the ClusterQueue name to give the account.
	// Unset means "<site>-", where site is the OffloadConfig name. A ClusterQueue
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

	// executorTemplate holds a Go text/template that renders the executor objects
	// of one tenant namespace as multi-document YAML. It may produce only
	// namespaced objects. Unset means no executor objects are rendered.
	// +optional
	ExecutorTemplate *ConfigMapKeyRef `json:"executorTemplate,omitempty"`

	// executorImage is passed to the executor template as .Image.
	// +optional
	ExecutorImage string `json:"executorImage,omitempty"`

	// executorTokenReview also binds every ServiceAccount the executor template
	// renders to system:auth-delegator, with one ClusterRoleBinding per tenant
	// namespace, so that the executor can validate the offload controller's token.
	// +optional
	ExecutorTokenReview bool `json:"executorTokenReview,omitempty"`

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
// +kubebuilder:printcolumn:name="Executor",type=string,JSONPath=`.spec.executorService`
// +kubebuilder:printcolumn:name="Poll",type=string,JSONPath=`.spec.pollInterval`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// OffloadConfig is the configuration of one HPC site for the offload controller.
type OffloadConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec OffloadConfigSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// OffloadConfigList is a list of OffloadConfig.
type OffloadConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OffloadConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&OffloadConfig{}, &OffloadConfigList{})
}
