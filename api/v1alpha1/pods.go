package v1alpha1

// SchedulerName is the schedulerName of the pods of an offloaded Workload. No
// scheduler serves it: the offload controller binds those pods to its virtual
// node.
const SchedulerName = "kueue-offload"

// NodeLabel is on the offload controller's virtual node, with the node's name
// as its value. Every pod of a Workload admitted through an offload check
// carries it from the check's podSetUpdates, with the same value in its
// nodeSelector.
const NodeLabel = "offload.interlink.eu/node"

// WorkloadAnnotation on a pod names the Workload the pod belongs to. It comes
// from the check's podSetUpdates.
const WorkloadAnnotation = "offload.interlink.eu/workload"

// RankAnnotation on a pod is the member of the job the pod shows, numbered as
// the executor numbers members. The offload controller writes it when it binds
// the pod and never changes it.
const RankAnnotation = "offload.interlink.eu/rank"

// FailedAnnotation on a Workload says why its job will never run, or why it
// was given up. The virtual node fails the Workload's pods with it.
const FailedAnnotation = "offload.interlink.eu/failed"

// NodeName is the name of the virtual node of an OffloadConfig.
func NodeName(offloadConfig string) string { return "kueue-offload-" + offloadConfig }
