# kueue-offload

Runs Kueue Workloads on batch systems outside the cluster through a Kueue admission check and a
per-user executor. Kueue decides whether and when a workload runs, the user's executor decides how
and as whom, and no cluster-scoped component holds a user's credential. Each site is one virtual
node, run inside the offload controller, on which the Workload's pods show the job:
`kubectl get pods` and `kubectl logs` work as on any cluster.

The executor drives an interLink plugin through a stock interLink API server, with the calls a
virtual kubelet makes, so any batch system reachable through an interLink plugin can be a backend
([#1](https://github.com/matbun/kueue-offload/issues/1)). Slurm is the first.

- [docs/design.md](docs/design.md): how the code works today and the contracts between the
  components.
- [docs/kueue-native.md](docs/kueue-native.md): the shape it should take to follow Kueue and
  MultiKueue, and the upstream change that shape needs.
- [docs/backends.md](docs/backends.md): the backends, and how one plugs in.
- [docs/landscape.md](docs/landscape.md): what Kueue, MultiKueue and related projects offer for
  sites without a Kubernetes API, and the path upstream.

## Components

| Component | Where | What it does |
| --- | --- | --- |
| offload controller (`cmd/offload-controller`) | one Deployment for every site, namespace `kueue-offload` | Kueue admission check controller (`controllerName: interlink.eu/offload`), each check with its own `OffloadConfig`: hands a Workload whose quota is reserved to the user's executor as an `OffloadRequest` per attempt, keeps the check `Pending` with the batch system's state in its message while the job waits, and sets it `Ready` once the job started, so that Kueue admits the Workload and the owner (Job, JobSet, TrainJob, RayJob, RayCluster or pods) makes its pods. Runs one virtual node per site, `kueue-offload-<OffloadConfig>`: binds those pods to it, shows on each the state of the member it stands for, so that owners end from their pods, and serves their logs from the executor (kubelet API on port 10250). Refuses a wall time above the flavor's queue limit, and releases the quota of a job the site would start too late (its start estimate). Keeps each site's `OffloadWorker` `Active` from its login endpoint. Holds no Secret and creates no pod |
| membership controller (`cmd/membership`) | one Deployment per site, namespace `kueue-offload-membership` | from the site's account associations and an `IdentityMap`: a ClusterQueue per project, the namespace labels Kueue's selectors match, a LocalQueue per membership, and each user's executor from a template; offboards a namespace that leaves the site once nothing of it runs there. Reads Secrets' metadata only, to roll an executor whose Secret was replaced |
| executor (`cmd/executor`) | the user's namespace, one pod with the interLink API server and the plugin | runs the `OffloadRequest`s of its namespace and reports on them in their status, turning a Workload's PodSets into member pods for the plugin, through the API server: sizes them by the site's rules, resolves their Secrets, ConfigMaps, tokens and downward API, asks the site's start estimate, stages inputs and outputs (also when a started job is deleted), derives the job's state from the plugin's status, renders a shadow (an SSH tunnel whose IP rank 0's pod takes) for a job that exposes ports or tunnels back, and serves the job's logs to the offload controller |
| interLink API server and plugin | next to the executor, stock images | run the members as one batch job with the user's own key (with Slurm, a pod group of N members is one `sbatch --nodes=N`); the flags of the flavor's queue come from the OffloadConfig |

CRDs (group `interlink.eu`): `OffloadConfig`, `OffloadWorker` and `OffloadRequest` for the offload
controller and the executors, `IdentityMap` and `ProjectQuota` for the membership controller. The
controller and the executors talk only through `OffloadRequest`s: the controller writes the spec,
the executor of the request's namespace the status, and a ValidatingAdmissionPolicy
(`config/admission`) holds everyone to that split.

## What a user writes

A Job (or a JobSet, a TrainJob, a KubeRay RayJob or RayCluster, a plain pod or a Kueue pod group)
with a queue name, ordinary resource requests, optionally a wall time (Kueue's label
`kueue.x-k8s.io/max-exec-time-seconds`, or the annotation `interlink.eu/time-limit: "HH:MM:SS"`) and
no node selector:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: train
  labels: {kueue.x-k8s.io/queue-name: hpc-a-proj42}
spec:
  parallelism: 2
  completions: 2
  completionMode: Indexed
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: train
        image: registry.example.org/train:1.0
        command: [python, train.py]
        resources: {requests: {cpu: "16", memory: 64Gi, nvidia.com/gpu: "4"}}
```

The Workload's check message follows the job (`job 123 queued`, `running on node07`,
`completed`). The Job stays suspended while its job waits at the site; once the job started, Kueue
admits the Workload, the Job makes its pods and the offload controller binds them to the site's
node, `kueue-offload-hpc-a`. There they show the job: `Pending`, `Running` once their member runs,
then `Succeeded` or `Failed` with the member's exit codes, and the Job completes or fails from
them. `kubectl logs <pod>` prints the member's output (`-f`, `--tail`, `--limit-bytes` as on any
pod), also after the job ended. `kubectl exec <pod> -- <command>` runs the command on the member's
compute node, in a new container of the pod's image with its mounts and environment, where the
site's plugin can reach the node (the Slurm plugin's `ExecPrefix`); elsewhere it answers that the
site offers no exec.

## Build and deploy

```
make test                          # unit tests; the executor's run an interLink API server built
                                   # from INTERLINK_SRC (default ../interLink-hardening)
make docker-build IMG=<image>      # /offload-controller, /membership and /executor in one image
kubectl apply --server-side -k config/default     # CRDs, RBAC, who may write an OffloadRequest,
                                                  # the offload controller (image kueue-offload)
kubectl apply --server-side -k config/membership  # its namespace and RBAC, the executors' ClusterRole
```

One offload controller serves every site and runs each site's node; it approves its own
kubelet-serving CSR, and the API server reaches its pod's port 10250 for `kubectl logs`. The pods
of an admitted Workload must not be scheduled: they carry the node selector
`offload.interlink.eu/node` (Kueue's `podSetUpdates`), so the default scheduler leaves them
pending until the controller binds them. Three mutating admission rules are needed at create,
for pods with the node label and plain pods with a queue of the site: `schedulerName:
kueue-offload`; no `tolerationSeconds` on the `node.kubernetes.io/not-ready` and `unreachable`
tolerations (else a node not ready for 5 minutes evicts plain pods and cancels their jobs); and
`backoffLimit: 0` on Jobs ([docs/design.md](docs/design.md#virtual-node)). Then per site: an
`OffloadWorker`, a `ResourceFlavor` per batch queue, an `AdmissionCheck` naming the site's
`OffloadConfig`, the `OffloadConfig` itself (naming the worker, with its `membership` block), the
associations and executor template ConfigMaps in `kueue-offload-membership`, an `IdentityMap`,
optional `ProjectQuota`s, and the membership Deployment of
`config/samples/membership/manager.yaml` with `--site=<site>`. `config/samples/membership` has
a complete example.

## Uninstall

In two steps, so that no job is lost and nothing is left behind:

1. Take every tenant off: delete the `IdentityMap`s (or their entries). Each membership controller
   offboards its namespaces ([docs/design.md](docs/design.md#offboarding)): once nothing of a
   namespace runs at the site, the OffloadRequests of its finished Workloads, whose executor cleans
   up after their jobs, the `offboardTemplate` Jobs, then its objects, the executor's stage Jobs,
   its labels and annotations; then each ClusterQueue no namespace holds, and the site's Cohort.
   Namespaces and their Secrets stay. The step is done when `kubectl get offloadconfigs` shows
   `MEMBERS` and `LEAVING` at 0 for every site and
   `kubectl get clusterqueues,cohorts,persistentvolumeclaims -A -l hpc.interlink.eu/managed-by=kueue-offload-membership`
   finds nothing: a claim the executor template made stays Terminating until the pods that mount
   it end.
2. Delete the rest, in any order (`kubectl delete -k` of what was applied, or the Argo CD
   Application with its resources). No OffloadRequest is left at that point; one that is (its
   executor gone for good) keeps its finalizer and blocks the CRD: release it as
   [docs/design.md](docs/design.md#who-may-write-it) says.

## Requirements

Kubernetes 1.30 or later (ValidatingAdmissionPolicy v1). Kueue v0.16 or later (API
`kueue.x-k8s.io/v1beta2`) with the `batch/job` integration, the `pod` integration for plain pods
and the `ray.io/rayjob` and `ray.io/raycluster` integrations for KubeRay owners. The reverse tunnel
needs an interLink API server built with `PodStatus.SocketDir`, and the start estimate one with the
`/estimate` route and a plugin that lists the `estimate` capability. The Slurm plugin's job directory must be shared with the site's filesystem (a CSI
volume such as csi-rclone works; see [docs/design.md](docs/design.md#job-directory)). The virtual
nodes need the cluster's `kubernetes.io/kubelet-serving` signer to issue their serving certificate,
a role granting the controller patch and delete on its nodes and patch on their status by name
(`kueue-offload-<OffloadConfig>`, [docs/design.md](docs/design.md#virtual-node)), and the API server to reach the controller pod on port 10250
([docs/design.md](docs/design.md#virtual-node)).

## License

Apache License 2.0, see `LICENSE`. Copyright 2026 CERN; see `NOTICE`.
