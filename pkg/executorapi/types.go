// Package executorapi holds the names the executor shares with the offload
// controller and the interLink API server. The offload controller and the
// executor talk through OffloadRequests; the executor's own HTTP API is GET
// /getLogs, which the offload controller's virtual node reads, and GET
// /healthz.
package executorapi

// GetLogsPath is the executor's log route. It answers the logs of one
// container of one member of a Workload's live job, or of every member.
const GetLogsPath = "/getLogs"

// ExecPath is the executor's exec: a command in a running member's container,
// with the kubelet's exec protocol. Its query is ExecWorkloadUID, ExecRank and
// ExecContainer, then the kubelet's command, tty, input, output and error.
const ExecPath = "/exec"

const (
	ExecWorkloadUID = "workloadUID"
	ExecRank        = "rank"
	ExecContainer   = "container"
)

// Query parameters of GetLogsPath. workloadUID is required; container defaults
// to each member's first container; without rank, every member's lines, each
// prefixed "[rank N] " when there are more than one. follow, timestamps and
// previous are "true" or not; tail, sinceSeconds and limitBytes are counts;
// sinceTime is RFC 3339. previous is refused: an offloaded container has no
// previous instance.
const (
	LogsWorkloadUID  = "workloadUID"
	LogsContainer    = "container"
	LogsRank         = "rank"
	LogsFollow       = "follow"
	LogsTail         = "tail"
	LogsSinceSeconds = "sinceSeconds"
	LogsSinceTime    = "sinceTime"
	LogsLimitBytes   = "limitBytes"
	LogsTimestamps   = "timestamps"
	LogsPrevious     = "previous"
)

// SessionHeader carries the Workload UID on every call to the interLink API
// server, for tracing.
const SessionHeader = "InterLink-Http-Session"

// IndependentPodsAnnotation, set to "true" on the owner, runs a Workload of several
// pods as one job per pod instead of one allocation. The offload controller
// forwards it in the OffloadRequest's ownerAnnotations like the owner's other
// annotations.
const IndependentPodsAnnotation = "offload.interlink.eu/independent-pods"

// ServiceAccountTokenAnnotation, set to "true" on the owner, keeps the token of the
// API server's own kube-api-access-* volume, which a plain pod carries, for a job
// that calls the cluster's API (a pipeline step's launcher). Without it, and unless
// the executor's AutomountServiceAccountToken is set, that token never leaves the
// cluster.
const ServiceAccountTokenAnnotation = "offload.interlink.eu/service-account-token"
