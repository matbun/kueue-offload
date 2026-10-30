package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// Condition types of an OffloadRequest, written by the executor. QuotaReserved,
// Admitted, Finished and Evicted mean what they mean on a remote Workload of
// MultiKueue.
const (
	// OffloadRequestAccepted is True once the executor took the request, and
	// False when it refused it for good (an account it does not serve, a shape
	// the site cannot run, the site refusing the job at submission). False sets
	// the check to Rejected.
	OffloadRequestAccepted = "Accepted"
	// OffloadRequestStagingIn is True while the stage-in Job runs.
	OffloadRequestStagingIn = "StagingIn"
	// OffloadRequestQuotaReserved is True once the job is submitted and waits in
	// the site's queue. Its lastTransitionTime starts the maximum queue time.
	OffloadRequestQuotaReserved = kueue.WorkloadQuotaReserved
	// OffloadRequestAdmitted is True while the job runs, False while it waits in
	// the site's queue.
	OffloadRequestAdmitted = kueue.WorkloadAdmitted
	// OffloadRequestStagingOut is True while the stage-out Job of an ended job
	// runs.
	OffloadRequestStagingOut = "StagingOut"
	// OffloadRequestFinished is True once the job ended and staging out is done,
	// with reason Succeeded or Failed. A failed stage-out fails the job.
	OffloadRequestFinished = kueue.WorkloadFinished
	// OffloadRequestEvicted is True when the job was lost: cancelled outside the
	// executor, or gone from the site's status. It sets the check to Retry. It is
	// False with reason RankMissing while a rank is missing from the site's
	// status, since its lastTransitionTime.
	OffloadRequestEvicted = kueue.WorkloadEvicted
)

// Reasons of the Finished condition, Kueue's for a finished Workload.
const (
	OffloadRequestSucceeded = kueue.WorkloadFinishedReasonSucceeded
	OffloadRequestFailed    = kueue.WorkloadFinishedReasonFailed
)

// Rank states in status.ranks.
const (
	RankQueued    = "queued"
	RankRunning   = "running"
	RankCompleted = "completed"
	RankFailed    = "failed"
)

// ExecutorFinalizer is the executor's finalizer on an OffloadRequest. The
// executor adds it before it submits, and removes it once a deleted request's
// job is cancelled and its objects (shadow, stage Jobs, job directory) are gone.
const ExecutorFinalizer = "offload.interlink.eu/executor"

// AttemptFinalizer is the offload controller's finalizer on an OffloadRequest.
// It keeps a deleted request, and the attempt number it carries, until the
// controller made the check's next attempt or the Workload can have none.
const AttemptFinalizer = "offload.interlink.eu/attempt"

// SiteLabel on an OffloadRequest names its site, so that an executor can watch
// only the requests of its own site.
const SiteLabel = "offload.interlink.eu/site"

// WorkloadReference names a Workload in the OffloadRequest's namespace.
type WorkloadReference struct {
	// +kubebuilder:validation:MinLength=1
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

// OwnerReference identifies the object the Workload finishes through: its
// Job, JobSet or TrainJob, or for pods v1 Pod named after the pod group (the
// pod's name for a single pod) with the first owner pod's UID.
type OwnerReference struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Name       string    `json:"name"`
	UID        types.UID `json:"uid"`
}

// PodSet is one PodSet of the Workload with what Kueue admitted for it.
type PodSet struct {
	// name is the PodSet's name in the Workload.
	Name kueue.PodSetReference `json:"name"`
	// flavors are the ResourceFlavors Kueue assigned to the PodSet's resources.
	// +optional
	Flavors map[corev1.ResourceName]kueue.ResourceFlavorReference `json:"flavors,omitempty"`
	// count is the number of pods Kueue admitted.
	// +kubebuilder:validation:Minimum=0
	Count int32 `json:"count"`
	// annotations are those of the PodSet's flavors in the OffloadConfig,
	// expanded. The executor appends each value to the template's own value for
	// that key, after a space, and never parses it.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// template is the PodSet's pod template as the Workload holds it.
	Template corev1.PodTemplateSpec `json:"template"`
	// secretRefs and configMapRefs are every Secret and ConfigMap the template
	// names through volumes (secret, configMap, projected sources), envFrom and
	// env valueFrom. The executor fetches exactly these from its namespace.
	// +listType=set
	// +optional
	SecretRefs []string `json:"secretRefs,omitempty"`
	// +listType=set
	// +optional
	ConfigMapRefs []string `json:"configMapRefs,omitempty"`
}

// OffloadRequestSpec is one attempt at running a Workload whose quota Kueue
// reserved on a flavor the check covers. The offload controller writes it once.
type OffloadRequestSpec struct {
	// workload is the Workload this request runs. The Workload is also the
	// request's controller owner.
	Workload WorkloadReference `json:"workload"`
	// admissionCheck is the Workload's check this request is an attempt of.
	AdmissionCheck kueue.AdmissionCheckReference `json:"admissionCheck"`
	// attempt counts the check's attempts from 1. Retries spent are
	// attempt-1-deferrals.
	// It lives here because Kueue drops the state of a check that covers only
	// some flavors, retryCount included, when it evicts the Workload.
	// +kubebuilder:validation:Minimum=1
	Attempt int32 `json:"attempt"`
	// deferrals counts the check's earlier attempts set to Retry by the site's
	// start estimate (the OffloadConfig's maxStartDelay), which are not retries:
	// the previous attempt's deferrals, plus one when that attempt submitted no
	// job and its status.estimate has no startTime or one further than
	// maxStartDelay from its probeTime.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Deferrals int32 `json:"deferrals,omitempty"`
	// site is the OffloadWorker the job runs on.
	// +kubebuilder:validation:MinLength=1
	Site string `json:"site"`
	// account is the site account the job is charged to, from the
	// ClusterQueue's label.
	// +kubebuilder:validation:MinLength=1
	Account string `json:"account"`
	// owner is the object the Workload finishes through.
	Owner OwnerReference `json:"owner"`
	// ownerAnnotations are the owner's annotations under the prefixes
	// slurm-job.vk.io/, htcondor-job.vk.io/, interlink.eu/ and
	// offload.interlink.eu/. They apply to every PodSet template, whose own
	// value for a key wins.
	// +optional
	OwnerAnnotations map[string]string `json:"ownerAnnotations,omitempty"`
	// ownerLabels are, for a Workload of one plain pod, that pod's labels:
	// Kueue's PodSet template of a pod holds its spec only. The shadow carries
	// those a Service of the namespace selects the pod by, so that the Service
	// selects the shadow too. Empty for every other owner.
	// +optional
	OwnerLabels map[string]string `json:"ownerLabels,omitempty"`
	// podSets are the Workload's PodSets, in its order.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	PodSets []PodSet `json:"podSets"`
	// maximumExecutionTimeSeconds is the job's wall time: the Workload's, else
	// the longest interlink.eu/time-limit of its PodSets, a template's own value
	// winning over the owner's.
	// +optional
	MaximumExecutionTimeSeconds *int32 `json:"maximumExecutionTimeSeconds,omitempty"`
	// priority is the Workload's.
	// +optional
	Priority *int32 `json:"priority,omitempty"`
}

// RankStatus is one member of the job.
type RankStatus struct {
	Rank int32 `json:"rank"`
	// podUID is the UID the member has at the backend; empty for a rank of
	// independent pods that waits for the site to take it.
	// +optional
	PodUID string `json:"podUID,omitempty"`
	// jobID is the batch job the member runs in, its own for independent pods.
	// +optional
	JobID string `json:"jobID,omitempty"`
	// +kubebuilder:validation:Enum=queued;running;completed;failed
	State string `json:"state"`
	// +optional
	ExitCode int32 `json:"exitCode,omitempty"`
	// containers are the member's container statuses as the backend reports
	// them. The head rank's exit codes reach the owner pods.
	// +listType=atomic
	// +optional
	Containers []corev1.ContainerStatus `json:"containers,omitempty"`
}

// ShadowReference names the shadow objects the executor rendered for the job,
// and the state of the shadow's pod.
type ShadowReference struct {
	// +optional
	Deployment string `json:"deployment,omitempty"`
	// +optional
	Service string `json:"service,omitempty"`
	// podIP is the IP of the shadow's pod, a ready one if there is one.
	// +optional
	PodIP string `json:"podIP,omitempty"`
	// ready is the Ready condition of that pod: for a shadow that forwards
	// ports, its forward listens.
	// +optional
	Ready bool `json:"ready,omitempty"`
}

// StartEstimate is the site's answer, before an attempt is submitted, to when
// it would start the job.
type StartEstimate struct {
	// probeTime is when the executor asked.
	ProbeTime metav1.Time `json:"probeTime"`
	// nodes is the number of nodes the job asked for.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Nodes int32 `json:"nodes,omitempty"`
	// startTime is when the site expects to start the job; unset when it gave
	// none, as reason and message say.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// reason and message say why the site gave no start time, for example that
	// it takes no further job of the user now (for Slurm,
	// QOSMaxSubmitJobPerUserLimit).
	// +optional
	Reason string `json:"reason,omitempty"`
	// +kubebuilder:validation:MaxLength=4096
	// +optional
	Message string `json:"message,omitempty"`
}

// OffloadRequestStatus is the executor's account of the request and its job.
type OffloadRequestStatus struct {
	// conditions are Accepted, StagingIn, QuotaReserved, Admitted, StagingOut,
	// Finished and Evicted.
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=16
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// lastSyncTime is when the executor last read the request and the state of
	// its job: its heartbeat, written on every poll. Without one for the
	// OffloadConfig's executorLostTimeout the check goes to Retry.
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`
	// jobID is the batch job ID; for independent pods every rank's job,
	// comma-separated, or the first and the last past eight.
	// +optional
	JobID string `json:"jobID,omitempty"`
	// nodeName is the head rank's node, the one the shadow tunnels to.
	// +optional
	NodeName string `json:"nodeName,omitempty"`
	// nodes are every node of the allocation.
	// +listType=atomic
	// +optional
	Nodes []string `json:"nodes,omitempty"`
	// socketDir is the directory on the node where the job's tunnel sockets
	// live.
	// +optional
	SocketDir string `json:"socketDir,omitempty"`
	// startTime and completionTime are the job's, as the backend reports them.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	// ranks are the job's members in rank order.
	// +listType=atomic
	// +optional
	Ranks []RankStatus `json:"ranks,omitempty"`
	// shadow names the shadow objects of the job.
	// +optional
	Shadow *ShadowReference `json:"shadow,omitempty"`
	// estimate is the site's start estimate for the job, asked before it is
	// submitted, which the OffloadConfig's maxStartDelay is held against.
	// +optional
	Estimate *StartEstimate `json:"estimate,omitempty"`
	// reason and message say what the executor is doing or waiting for: the
	// site's reason for a queued job, why a submit is refused for now (a limit
	// on submitted jobs, a site that cannot be reached), why a job failed.
	// +optional
	Reason string `json:"reason,omitempty"`
	// +kubebuilder:validation:MaxLength=32768
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Workload",type=string,JSONPath=`.spec.workload.name`
// +kubebuilder:printcolumn:name="Attempt",type=integer,JSONPath=`.spec.attempt`
// +kubebuilder:printcolumn:name="Site",type=string,JSONPath=`.spec.site`
// +kubebuilder:printcolumn:name="Job",type=string,JSONPath=`.status.jobID`
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.nodeName`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.reason`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// OffloadRequest is one attempt at running a Workload at an HPC site. The
// offload controller creates it, named <workload>-<check>-<attempt>, in the
// Workload's namespace; the tenant's executor runs it as one batch job under
// the tenant's identity and reports on it in its status.
type OffloadRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OffloadRequestSpec   `json:"spec,omitempty"`
	Status OffloadRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// OffloadRequestList is a list of OffloadRequest.
type OffloadRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OffloadRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(&OffloadRequest{}, &OffloadRequestList{})
}
