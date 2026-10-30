package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// AccountLabel on a ClusterQueue names the site account its Workloads are
// charged to. The membership controller sets it on the ClusterQueues it makes.
const AccountLabel = "hpc.interlink.eu/account"

// OffloadConfigSpec holds the parameters of the AdmissionChecks with
// controllerName interlink.eu/offload that name it. One offload controller
// serves every such check and reads each check's OffloadConfig separately.
type OffloadConfigSpec struct {
	// workers names the OffloadWorkers the check may submit to. The check is
	// Active while one of them is. A check submits to one site until a
	// dispatcher chooses among several, so the list holds one name.
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1
	Workers []string `json:"workers"`

	// executorService is the name of the executor Service in every tenant
	// namespace, which the shadows read job logs from. The membership controller
	// passes it to the executor template.
	// +kubebuilder:default=kueue-offload-executor
	// +optional
	ExecutorService string `json:"executorService,omitempty"`

	// executorPort is the port of the executor Service.
	// +kubebuilder:default=8000
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	ExecutorPort int32 `json:"executorPort,omitempty"`

	// pollInterval is how often each executor is expected to report on its
	// OffloadRequests (their lastSyncTime heartbeat), and how often the offload
	// controller looks at a Workload whose request did not change. A request
	// with no heartbeat for twice this long shows "no executor answering" in the
	// check message.
	// +kubebuilder:default="15s"
	// +optional
	PollInterval *metav1.Duration `json:"pollInterval,omitempty"`

	// executorLostTimeout is how long an OffloadRequest may go without an
	// executor heartbeat before the check is set to Retry, as MultiKueue's
	// workerLostTimeout.
	// +kubebuilder:default="10m"
	// +optional
	ExecutorLostTimeout *metav1.Duration `json:"executorLostTimeout,omitempty"`

	// maxQueueTime is how long a submitted job may wait in the site's queue
	// before the check is set to Retry, which cancels it and lets Kueue requeue
	// the Workload, possibly on another flavor. Kueue puts no timeout on a
	// Pending check. Unset: no limit.
	// +optional
	MaxQueueTime *metav1.Duration `json:"maxQueueTime,omitempty"`

	// retryStrategy bounds the new attempts after a job is lost: cancelled on
	// the site, behind an executor without a heartbeat for executorLostTimeout,
	// or queued for longer than maxQueueTime. Each loss sets the check to Retry
	// with requeueAfterSeconds from Kueue's backoff, backoffBaseSeconds doubled
	// per retry up to backoffMaxSeconds, and Kueue requeues the Workload after
	// that delay. The retries are counted by the OffloadRequest's attempt; once
	// backoffLimitCount retries were spent the check is Rejected, and 0 rejects
	// at the first loss. A job that ran and failed is not retried, and a submit
	// the executor refuses for now (a submit limit, an unreachable site) keeps
	// the check Pending with the quota. The type and its defaults are
	// ProvisioningRequestConfig's.
	// +kubebuilder:default={backoffLimitCount:3,backoffBaseSeconds:60,backoffMaxSeconds:1800}
	// +optional
	RetryStrategy *kueue.ProvisioningRequestRetryStrategy `json:"retryStrategy,omitempty"`

	// flavors maps a ResourceFlavor name to the batch queue its quota stands for,
	// as annotations the backend's plugin reads (for Slurm, the sbatch flags in
	// slurm-job.vk.io/flags). A project with several queues on one machine (say a
	// debug, a production and an interactive QOS) is one ClusterQueue with one
	// flavor each, and the flavor Kueue assigns decides where the job is submitted.
	// A PodSet none of whose admitted flavors is listed is rejected: its job would
	// carry no account.
	// +optional
	Flavors map[string]FlavorTarget `json:"flavors,omitempty"`

	// membership configures the membership sync controller for this site.
	// +optional
	Membership *Membership `json:"membership,omitempty"`
}

// FlavorTarget is the batch queue a ResourceFlavor stands for.
type FlavorTarget struct {
	// annotations are added to every member pod of a PodSet admitted on this
	// flavor. Each value is a Go text/template executed with .Account (the
	// ClusterQueue's account label), .MaxExecSeconds (the Workload's
	// maximumExecutionTimeSeconds, 0 when unset) and .MaxExecMinutes (the same in
	// minutes, rounded up). The executor appends each value to the template's own
	// value for that key, after a space, and never parses it, for example
	// slurm-job.vk.io/flags: "--account={{.Account}} --qos=prod{{with .MaxExecMinutes}} --time={{.}}{{end}}".
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
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
// +kubebuilder:printcolumn:name="Workers",type=string,JSONPath=`.spec.workers`
// +kubebuilder:printcolumn:name="Poll",type=string,JSONPath=`.spec.pollInterval`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// OffloadConfig holds the parameters of an offload AdmissionCheck and, for the
// membership controller of its site, the membership settings.
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
