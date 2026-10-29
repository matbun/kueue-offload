# kueue-offload

Offloads Kubernetes Jobs and plain pods to Slurm through a Kueue admission check and a
per-user executor, with no virtual node and no interLink API server. Kueue decides whether and
when a workload runs, the user's executor decides how and as whom, and no cluster-scoped
component holds a user's credential.

The design follows a white paper; `docs/design.md` records where this
implementation departs from it and fixes the contracts between the components.

## Components

| Component | Where | What it does |
| --- | --- | --- |
| offload controller (`cmd/offload-controller`) | one Deployment per site, namespace `kueue-offload` | Kueue admission check controller (`controllerName: interlink.eu/offload`): submits a Workload whose quota is reserved to the user's executor, keeps the check `Pending` with Slurm's state in its message while the job is queued and running, and ends the owner Job or pods when Slurm ends the job. Holds no Secret and creates no pod |
| membership controller (`cmd/membership`) | one Deployment per site, namespace `kueue-offload-membership` | from the site's Slurm associations and an `IdentityMap`: a ClusterQueue per project, the namespace labels Kueue's selectors match, a LocalQueue per membership, and each user's executor from a template. Never reads a Secret |
| executor | the user's namespace | `interlink-slurm-plugin` with a `/submit` and `/jobs` API (branch `feat/dispatch-agent`): turns a PodSet of `count: N` into one `sbatch --nodes=N` through the plugin's own job script code, with the user's key, and renders the SSH shadow (tunnel plus a `job-logs` container) once the job runs |

CRDs (group `interlink.eu`): `OffloadConfig` and `OffloadRecord` for the offload controller, `IdentityMap`
and `ProjectQuota` for the membership controller.

## What a user writes

A Job (or a plain pod, or a Kueue pod group) with a queue name, ordinary resource requests and
no node selector:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: train
  labels: {kueue.x-k8s.io/queue-name: hpc-a-proj42}
  annotations: {slurm-job.vk.io/flags: "--time=00:30:00"}
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

What `kubectl` then shows:

| Command | Result |
| --- | --- |
| `kubectl get workload` | quota reserved, the check message: `Slurm job 123 queued`, `running on node07`, `completed` |
| `kubectl get job train` | suspended while it runs (no pod is ever created), then `Complete` or `Failed` |
| `kubectl logs deploy/shadow-train-<ns> -c job-logs -f` | the job's output, live, `[rank N]` per rank; still there after the job ended |
| `kubectl get svc -l interlink.eu/shadow-of=<workload uid>` | the tunnel to the job's exposed ports |

## Build and deploy

```
make test                          # unit tests of both controllers
make docker-build IMG=<image>      # /offload-controller and /membership in one image
kubectl apply --server-side -f config/crd/bases
kubectl apply -k config/rbac; kubectl apply -k config/rbac/membership; kubectl apply -k config/rbac/executor
```

Then per site: a `ResourceFlavor` per partition, an `AdmissionCheck` naming the site's
`OffloadConfig`, the `OffloadConfig` itself (with its `membership` block), the associations and
executor template ConfigMaps in `kueue-offload-membership`, an `IdentityMap`, optional
`ProjectQuota`s, and the two Deployments of `config/manager` and `config/membership` with
`--site=<site>`. `config/samples/membership` has a complete example.

## Requirements

Kueue v0.16 or later (API `kueue.x-k8s.io/v1beta2`) with the `batch/job` integration, and the
`pod` integration for plain pods. The executor's job directory must be shared with the site's
filesystem (a CSI volume such as csi-rclone works; see `docs/design.md`, D4 and D11).
