// Package node is the virtual node of each OffloadConfig, run inside the
// offload controller. The pods of a Workload admitted through an offload check
// are bound to it, and it shows on each the state of the member of the job it
// stands for, as a kubelet shows a pod it runs, and serves its logs.
package node

// Patch and delete of nodes, and patch of nodes/status, the only writes the
// controller and the virtual-kubelet library make to a node it runs, are the
// deployment's to grant by name, one <OffloadConfig>-vk per site
// (resourceNames cannot name a node before it exists, and create takes none):
// the controller then cannot repoint a node it does not run, so a serving
// certificate for another node's name is of no use.
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=nodes/status,verbs=get
// +kubebuilder:rbac:groups=coordination.k8s.io,namespace=kube-node-lease,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create
// +kubebuilder:rbac:groups="",namespace=kube-system,resources=configmaps,resourceNames=extension-apiserver-authentication,verbs=get
// +kubebuilder:rbac:groups=certificates.k8s.io,resources=certificatesigningrequests,verbs=create;get;list;watch
// +kubebuilder:rbac:groups=certificates.k8s.io,resources=certificatesigningrequests/approval,verbs=update
// +kubebuilder:rbac:groups=certificates.k8s.io,resources=signers,resourceNames=kubernetes.io/kubelet-serving,verbs=approve
// +kubebuilder:rbac:groups=interlink.eu,resources=offloadconfigs;offloadrequests,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch
