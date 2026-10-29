# kueue-offload: implementation design

The specification is the white paper (`../../interlink-dispatch-whitepaper.md`, in the wrapper
directory). This file records where the implementation departs from it, and why, and fixes the
contracts the components are built against. Where this file and the white paper disagree, this
file wins.

## Deviations from the white paper

D1. The check is never set `Ready`; the Job is never unsuspended.
The white paper writes `Ready` when Slurm reports `running`, which makes the Workload `Admitted`.
Kueue then unsuspends the Job (or ungates plain pods) and the Job controller creates real pods
that the default scheduler runs on cluster nodes. The white paper's "the user's pod stays gated"
holds only where something stops that: `Job.spec.managedBy` (MultiKueue's way), which is alpha
and off by default before Kubernetes 1.32 and is silently dropped by the API server on the
target cluster (1.30.2: a server dry-run of a Job with `managedBy` returns it without the field).
Instead, the offload controller keeps the check `Pending` for the job's whole life. Kueue keeps the quota
reserved (`QuotaReserved=True`) and the Job suspended, so no pod of the user's ever exists. The
check `message` carries the Slurm state (`queued (Priority)`, `running on node07 since ...`). When
Slurm reports a terminal state the offload controller patches the owner Job's status to `Complete` or
`Failed`; the Job controller stops syncing a finished Job, and Kueue marks the Workload
`Finished` and releases the quota. Verified on the target cluster: a suspended Indexed Job with
`parallelism: 2` held at `Pending`, zero pods, then a status patch with `Complete=True`,
`succeeded: 2` gave `Finished=True(Succeeded)` on the Workload and a reservation of 0.
`Retry` and `Rejected` are used as in the white paper.

D2. Owners: `batch/Job`, `JobSet`, `TrainJob` and plain pods (single pods and pod groups).
A Workload owned by a `batch/v1` Job finishes through the Job's status (D1). One owned by a
`jobset.x-k8s.io/v1alpha2` JobSet or a `trainer.kubeflow.org/v1alpha1` TrainJob finishes the same
way, through the condition Kueue's integration for that kind reads: the offload controller reads and
patches them as unstructured objects (no Go module of either project), on the `status`
subresource. A Workload owned by
pods (Kueue's `pod` integration: one pod with `kueue.x-k8s.io/queue-name`, or a group with
`kueue.x-k8s.io/pod-group-name` and `pod-group-total-count`) keeps its pods `SchedulingGated`
for the job's whole life, as the white paper says, because the check never goes `Ready`. At a
terminal state the offload controller patches each owner pod's `status.phase` to `Succeeded` or `Failed`
(reason `KueueOffload`, message with the Slurm job ID) and a terminated state for each
container, so a workflow engine that reads the main container (Argo) sees how the step ended;
never `spec.nodeName`. Kueue then
removes its finalizers and marks the Workload `Finished`. Verified on the target cluster: a
2-pod group held at `Pending`, both pods patched `Succeeded`, gave `Finished=True(Succeeded)`
and a reservation of 0. A homogeneous group is one PodSet with `count: N` (Kueue names it by a
hash of the pod shape). Any other owner kind is `Rejected` with a message saying so. This needs
`pods` get and `pods/status` patch on the offload controller, the white paper's `mirrorPodStatus`
permission, used only for the terminal phase, and `get` plus `status` patch on `jobsets` and
`trainjobs`.

D3. The existing Kueue namespace selector is reused.
`managedJobsNamespaceSelector` is cluster-wide Kueue configuration shared with other tenants. The
deployment keeps the cluster's selector and the membership controller only adds the white
paper's labels (`hpc.interlink.eu/tenant`, `hpc.interlink.eu/<site>-<account>`) on top.

D4. The executor's job directory is a CSI volume, not a FUSE sidecar.
User namespaces enforce the `baseline` Pod Security level, which forbids privileged containers,
so the executor cannot run an sshfs sidecar. The job directory is a `csi-rclone` volume (SFTP to the
site, mounted by the node plugin) at the same absolute path as on the site's filesystem.
Measured: a local write is visible remotely at once, a remote append is visible locally after
0.2 s, `rm -rf` works; `chmod` is not carried (files land as 0644), so the site's `sbatch` shim
makes the job's scripts executable remotely before submitting.

D5. Owner annotations are forwarded.
The white paper's example puts `slurm-job.vk.io/flags` on the Job's metadata, but a Workload's
PodSet template holds only the pod template. The offload controller merges the owner object's
(the Job's, JobSet's or TrainJob's, or the first owner pod's) annotations with prefixes `slurm-job.vk.io/` and `interlink.eu/` into each PodSet template's
annotations (the template's own value wins).

D6. The shadow outlives the job until the Workload is deleted.
At a terminal state the executor re-renders the shadow without `ssh-forward`; `job-logs` prints the
whole log and then stays idle, so `kubectl logs <shadow> -c job-logs` still shows the output after
the job ended. The executor deletes the shadow on `DELETE /jobs/{uid}`, which the offload controller sends
when the Workload is deleted (the Job deleted by the user or by its TTL) or evicted.

D7. Job deletion is detected from the OffloadRecord side.
The offload controller has no `update` on Workloads, so it cannot hold a finalizer. It resyncs its
OffloadRecords; one whose Workload is gone gets `DELETE /jobs/{uid}` on the executor and is removed.

D8. One site per executor in v1.
The plugin runs `sbatch`, `squeue` and `scancel` through shell shims that read the target from
the environment; the executor sets that environment once, from its only `Sites` entry. Two sites
need two executors (one Deployment per site in the namespace) until the shims take the site per
call.

D9. Retry resubmits under the same Workload UID.
On `cancelled` the offload controller first sends `DELETE /jobs/{uid}` (which clears the executor's record),
increments `OffloadRecord.status.retries`, writes `Retry`, and submits again on the next
reservation. Idempotency on the Workload UID alone is therefore enough.

D10. The membership controller never checks a Secret.
Reading a Secret's existence needs `get` on Secrets, which also reads its content. The
controller instead watches the executor Deployment it created: when the executor is not available
because a referenced Secret is missing, it records that on the namespace (Event and annotation
`hpc.interlink.eu/<site>-executor: missing credentials`).

D11. The executor writes nothing through a rename onto an existing file, and opens job files
write-only.
Found on the target cluster's SFTP volume (D4): a rename onto an existing file fails on the
server while the mount reports success (`sed -i` returned 0, the remote file was unchanged and
the temporary file stayed), and a file opened read-write (`os.Create`) is uploaded after
`close`, so the upload could land after `sbatch` read the script or after its mode was set
(`job.sh: Permission denied` in one run of two). The executor removes a record before renaming
the new one over it (and takes a lone `<uid>.json.tmp` at start), opens the job script, the
job's `job.sh`, its environment files and its bookkeeping files write-only, and the site's
`sbatch` shim rewrites files in place.

D12. Limits on submitted jobs are transient.
`sbatch` refusing with `QOSMaxSubmitJobPerUserLimit` (or another `MaxSubmitJob` limit, or an
unreachable controller) is a 503, not a permanent 400: the job waits in the offload controller with
the reason in the check message and is submitted once the user's other job ends. Seen when
Kueue admitted a second job by borrowing quota in the cohort while the QOS took one job per
user.

D13. Requests size the job.
Kueue charges a Workload by its requests; the plugin sizes the job script from limits. The
executor gives every resource that has a request and no limit a limit equal to the request, so a
Job that requests 20 CPUs is not submitted with `--cpus-per-task=1` (sbatch refused it:
"Cpus requested: 1. Gpus: 1, Required cpus: 20").

D14. A followed log ends only once it stops growing.
After the job ends the stream keeps reading until the files have not changed for 10 s: the
head rank's log is completed after the ranks exit (the MPI launcher's output was 63 of 122
lines without this).

## Components and where they live

| Component | Code | Image |
| --- | --- | --- |
| offload controller (Kueue admission check) | this repo, `cmd/offload-controller`, `pkg/offload` | `kueue-offload` (`/offload-controller`) |
| membership sync controller | this repo, `cmd/membership`, `pkg/membership` | same image (`/membership`) |
| CRDs `OffloadConfig`, `OffloadRecord`, `IdentityMap`, `ProjectQuota` | this repo, `api/v1alpha1`, `config/crd` | |
| executor | `interlink-slurm-plugin`, branch `feat/dispatch-agent`: `pkg/slurm/dispatch*.go`, `pkg/shadow` | the plugin image |

API group of the CRDs: `interlink.eu`, version `v1alpha1`.

## Executor HTTP API (`offload.interlink.eu/v1alpha1`)

Every JSON body carries `"apiVersion": "offload.interlink.eu/v1alpha1"`. Every call from the
offload controller carries `Authorization: Bearer <token>` (the offload controller's projected ServiceAccount
token, audience `kueue-offload-executor`) and `InterLink-Http-Session: <workload UID>`.

### POST /submit

```json
{
  "apiVersion": "offload.interlink.eu/v1alpha1",
  "workloadUID": "0b6c...",
  "workloadName": "job-train-5d565",
  "namespace": "alice",
  "owner": {"apiVersion": "batch/v1", "kind": "Job", "name": "train", "uid": "..."},
  "site": "hpc-a",
  "account": "proj42",
  "podSets": [
    {
      "name": "main",
      "count": 4,
      "partition": "gpu",
      "template": {"metadata": {}, "spec": {}},
      "secretRefs": ["hf-token"],
      "configMapRefs": ["train-config"]
    }
  ]
}
```

- `owner` is the Job, JobSet or TrainJob, or for pods `v1` `Pod` named after the pod group (`kueue.x-k8s.io/pod-group-name`;
  the pod's name for a single pod), with the first owner pod's UID.
- `partition` may be empty: no `--partition` flag.
- `template` is the PodSet template as stored on the Workload, with the owner annotations of D5
  merged in.
- `secretRefs` / `configMapRefs`: every name the template references through volumes (`secret`,
  `configMap`, `projected` sources), `envFrom` and `env[].valueFrom`. The executor fetches exactly
  these from its own namespace.
- Several PodSets run as one allocation holding the members of every PodSet (white paper, "Open
  decisions", option c). They must share one `partition`; with a site launcher
  (`interlink.eu/launcher`) also the image, command and args of their first container, because
  the launcher starts the head's container on every rank.

Responses:

| Code | Body | Meaning |
| --- | --- | --- |
| 201 | `{"apiVersion", "jobID": "123", "accepted": true}` | submitted now |
| 200 | same | a job for this `workloadUID` already exists (idempotent) |
| 400 | `{"apiVersion", "permanent": true, "message": "..."}` | will never work: account not in `AllowedAccounts`, unknown site, namespace not the executor's, PodSets with different partitions (or, with a launcher, different containers), more than 9999 members, a reference the executor cannot read, `sbatch` refused (message is its stderr verbatim) |
| 401 / 403 | `{"permanent": false, "message"}` | caller not authenticated / not allowed |
| 503 | `{"apiVersion", "permanent": false, "message": "..."}` | transient: ssh failed, filesystem not ready |

### GET /jobs/{workloadUID}

```json
{
  "apiVersion": "offload.interlink.eu/v1alpha1",
  "workloadUID": "0b6c...",
  "jobID": "123",
  "state": "queued",
  "reason": "Priority",
  "message": "Slurm job 123 pending (Priority)",
  "exitCode": 0,
  "nodeName": "node07",
  "nodes": ["node07", "node08"],
  "socketDir": "/scratch/alice/.interlink/sockets/abc",
  "startTime": "2026-10-29T10:14:02Z",
  "completionTime": null,
  "ranks": [
    {"rank": 0, "podUID": "0b6c...-0", "state": "running", "exitCode": 0,
     "containers": []}
  ],
  "shadow": {"deployment": "shadow-train-...", "service": "shadow-train-..."}
}
```

`ranks[].containers` holds `corev1.ContainerStatus` objects as the plugin's `/status` reports them.
`state` is one of `queued`, `running`, `completed`, `failed`, `cancelled`, `rejected`.

- `queued`: Slurm has the job pending; `reason` is Slurm's pending reason.
- `running`: the allocation started (any rank running). `nodeName` is the first allocated node
  (the head rank's node), the one the shadow tunnels to.
- `completed`: every rank ended with exit code 0.
- `failed`: a rank ended non-zero, or the job hit a node failure, out of memory or the time limit.
  `exitCode` is the head rank's, `message` says which rank and why.
- `cancelled`: `scancel` from outside the executor (Slurm state `CA` without a `DELETE` from the
  offload controller), or the job vanished from Slurm with no exit status.
- `rejected`: kept for completeness; `/submit` reports refusals synchronously.

404 `{"permanent": true, "message": "no job for workload ..."}` when the executor has no record.

### DELETE /jobs/{workloadUID}

`scancel` if the job is not terminal, delete the shadow objects, remove the job directories and the
executor's record. 204, also when there is no record.

### GET /getLogs

Query: `workloadUID` (required), `container` (default: the template's first container), `rank`
(optional), `follow` (`true`/`false`), `tail` (lines), `sinceSeconds`.

- With `rank`: that rank's `run-<container>.out`, as the plugin's per-pod log path reads it.
- Without `rank` and count 1: rank 0, no prefix.
- Without `rank` and count > 1: every rank, each line prefixed `[rank N] `. Without `follow`: rank
  0's lines, then rank 1's, and so on. With `follow`: new complete lines of every rank as they
  appear, polled every second; the stream ends when the job is terminal and every file is drained.

Not covered by token auth: reachable only from the namespace's shadow pods (NetworkPolicy).

### GET /healthz

200 `ok`.

## Executor configuration (plugin `SlurmConfig`, new keys)

```yaml
SidecarPort: "8000"
Executor:
  Enabled: true
  Namespace: ""            # empty: read from the ServiceAccount namespace file
  Auth:
    Mode: tokenreview      # tokenreview | network
    Audience: kueue-offload-executor
    AllowedServiceAccounts: ["system:serviceaccount:kueue-offload:kueue-offload"]
  StateDir: ""             # empty: <DataRootFolder>executor
Sites:
  hpc-a:
    LoginHost: login.hpc-a.example.org
    User: alice
    Port: 22
    KeyFile: /etc/interlink/keys/hpc-a/ssh-privatekey
    AllowedAccounts: [proj42, proj99]
Shadow:
  Enabled: true
  AlwaysShadow: true
  Image: ghcr.io/interlink-hq/interlink/ssh-tunnel:0.6.3-pre2
  ExecutorImage: ""        # image of the job-logs container; empty: env EXECUTOR_IMAGE
  ExecutorURL: http://kueue-offload-executor:8000
  LoginHost: ""            # empty: the site's LoginHost
  User: ""                 # empty: the site's User
  KeySecret: hpc-a-tunnel-key
  KeySecretKey: ssh-privatekey
  KnownHostsConfigMap: hpc-a-known-hosts
  NodeHostKeys: verify
  ForwardTarget: node
  ForwardMode: portforward
  NodeWaitTimeout: 30m
Staging:
  Enabled: true
  Image: registry.a.example.org/stage-tools:1
  Command: /usr/local/bin/stage-rsync   # the default
  TransferHost: transfer.hpc-a.example.org
  StageRoot: /scratch/proj42/stage
  KeySecret: ""            # empty: Shadow.KeySecret
  KeySecretKey: ""         # empty: Shadow.KeySecretKey, else ssh-privatekey
  KnownHostsConfigMap: ""  # empty: Shadow.KnownHostsConfigMap
  Volumes:                 # the namespace's claims the staged paths live on
  - {Name: eos, ClaimName: eos, MountPath: /eos, Kerberos: true}
  - {Name: shared, ClaimName: shared, MountPath: /shared}
  Kerberos:                # for paths on a Kerberos volume
    KeytabSecret: alice-keytab
    KeytabKey: keytab
    Principal: alice@A.EXAMPLE.ORG
    Krb5ConfigMap: krb5-config
```

The existing keys (`SbatchPath`, `DataRootFolder`, `SocketDirRoot`, `Flavors`,
`GangSchedulingEnabled`, `SiteScriptPath`, ...) keep their meaning. The executor exports
`REMOTE_USER`, `REMOTE_LOGIN`, `REMOTE_PORT` and `SSH_KEY` from the one `Sites` entry before the
first shim call (D8).

### How `/submit` uses the plugin

`/submit` expands each PodSet into `count` member pods and hands them to the plugin's existing
create path, so the job script, the resource translation, the launchers and the probes are the
ones `/create` produces. Members are numbered across the PodSets in request order, and `<count>`
below is the total of every PodSet's count:

- member `i` (0-based): `metadata.name` `<owner name>-<i>`, `uid` `<workloadUID>-<i>`, namespace
  the Workload's, labels and annotations from its own PodSet's template, plus label
  `batch.kubernetes.io/job-completion-index: "<i>"` and annotation
  `interlink.eu/podset: <PodSet name>`;
- `count > 1`: also the Kueue pod-group label `kueue.x-k8s.io/pod-group-name: <workloadUID>` and
  annotation `kueue.x-k8s.io/pod-group-total-count: "<count>"`, so the plugin's gang path buffers
  the members and submits one `sbatch --nodes=<count>`; member 0 (of the first PodSet) is the
  head, and each rank runs its own member's `job.sh`;
- the allocation gives every rank a node of one shape: the head's flags, with the largest CPU,
  memory and GPU request of any member, so members of different PodSets each fit;
- env `INTERLINK_RANK=<i>` and `INTERLINK_WORLD_SIZE=<count>` added to every container;
- `--account=<account>` and, when set, `--partition=<partition>` appended to the members'
  `slurm-job.vk.io/flags` annotation;
- `env[].valueFrom` resolved by the executor (secretKeyRef, configMapKeyRef, fieldRef for
  `metadata.name|namespace|uid|labels['k']|annotations['k']`, `spec.serviceAccountName`,
  resourceFieldRef for requests and limits), as the virtual kubelet does before calling `/create`.

The executor keeps one record per workload (`<StateDir>/<workloadUID>.json`: request, member UIDs,
job ID, shadow names) and reloads them at start.

`GET /getLogs` without `container` reads each member's first container; with `container`, the
members that have it.

### Staging

Paths listed, `:`-separated, in a template's `interlink.eu/stage-in` and `interlink.eu/stage-out`
annotations (owner annotations included, D5; the union over the PodSets) are copied through the
site's transfer host by a batch Job in the executor's namespace that runs `Staging.Command`. Every
path must be absolute, clean and below a `Staging.Volumes` mount path, else `/submit` answers 400.

- `/submit` with stage-in paths creates `stage-in-<first 8 of the UID>` once and answers 503
  `staging in` while it runs, 400 with the Job's failure reason if it failed, and submits as
  usual once it succeeded.
- When the job is `completed` or `failed` and has stage-out paths, the executor creates
  `stage-out-<8>`; `GET /jobs` reports `running` (reason `StagingOut`, message `staging out: ...`)
  until it ends, then the job's own state. A failed stage-out makes the job `failed` (reason
  `StageOutFailed` for a job that had completed), the message says so. A cancelled job is not
  staged out: it is deleted and retried (D9).
- `DELETE /jobs/{uid}` deletes both Jobs (propagation `Background`), also for a workload whose
  stage-in never let it be submitted.

A stage Job has labels `interlink.eu/stage-of: <workloadUID>`, `interlink.eu/stage-direction: in|out`
and `sidecar.istio.io/inject: "false"`, `backoffLimit: 0`, `restartPolicy: Never`, requests
50m/64Mi, limits 2 CPU/2Gi, the site key at `/ssh` and the known hosts at
`/etc/ssh/ssh_known_hosts`, and mounts only the volumes its paths are on. The command's contract is
its environment: `STAGE_DIRECTION` (`in`/`out`), `STAGE_PATHS`, `REMOTE`
(`<site User>@<TransferHost>`), `STAGE_ROOT`, `STAGE_NAMESPACE`, `SSH_KEY`, and for a path on a
`Kerberos` volume `KEYTAB`, `PRINCIPAL` and `KRB5CCNAME=FILE:/tmp/krb5cc` (keytab at `/keytab`,
`krb5.conf` from `Krb5ConfigMap`). The executor needs `get`, `list`, `create` and `delete` on
`batch/jobs` in its namespace.

The job finds the staged inputs at their own paths only through binds of the site's stage
directory onto them in `SingularityDefaultOptions`: that is the deployment's configuration, the
executor adds no bind.

### Shadow interface (executor side)

The executor code in `pkg/slurm` talks to the shadow package through this interface, so the two
are built separately:

```go
// package shadow
type Spec struct {
    WorkloadUID  string
    WorkloadName string // the owner's name, base of the shadow's resource names
    Namespace    string
    Template     corev1.PodTemplateSpec // rank 0's template: ports, annotations, labels
    NodeName     string                 // empty until running
    SocketDir    string
    Terminal     bool                   // job ended: logs-only shadow (D6)
}

type Ref struct{ Deployment, Service, NodeConfigMap string }

type Manager interface {
    // Ensure creates or updates the shadow for spec: Deployment, Service (only with exposed
    // ports), node ConfigMap with compute-node and socket-dir. Idempotent; called on every poll
    // once the job runs.
    Ensure(ctx context.Context, spec Spec) (Ref, error)
    // Delete removes every object labelled for this workload. Idempotent.
    Delete(ctx context.Context, workloadUID string) error
}
```

## Offload controller behaviour

Per Workload whose ClusterQueue's checks include one with `controllerName: interlink.eu/offload`
and `parameters` naming this offload controller's `OffloadConfig`:

| Situation | Action |
| --- | --- |
| no `QuotaReserved=True`, evicted or deactivated, and a record with a job | `DELETE /jobs/{uid}`, clear the job from the record |
| `Finished=True` | nothing (the job directory and the shadow stay until the Workload goes, D6) |
| reserved, check `Pending`, no job in the record | `POST /submit` with one entry per PodSet, each with the partition of its own admitted flavors (the executor decides how many PodSets it runs); 201/200: store `jobID`, message `submitted as Slurm job N`; 400: check `Rejected` with the message, owner `Failed` (reason `Rejected`); 5xx or network: requeue with backoff |
| record with a job | `GET /jobs/{uid}` every `pollInterval` and map: `queued` and `running` keep `Pending` with the state in the message; `completed`/`failed` patch the owner's status to its terminal condition (D1, D2), or each owner pod's phase to `Succeeded`/`Failed` (D2); `cancelled` gives `DELETE`, `retries++`, check `Retry` (`Rejected` once `maxRetries` is reached); unreachable keeps the state until `executorLostTimeout`, then `Retry` |
| OffloadRecord whose Workload no longer exists | `DELETE /jobs/{uid}`, delete the OffloadRecord |

Check state writes are server-side apply patches of `status.admissionChecks` (a list keyed by
`name`) on `workloads/status` with field manager `kueue-offload`. Owner status patches use
`jobs/status`, `jobsets/status`, `trainjobs/status` or `pods/status`. Every transition also emits an Event on the Workload.

Owner Job status on completion: `startTime` and `completionTime` from the executor, `succeeded` =
`completions` (`parallelism` when unset), `completedIndexes` `0-(n-1)` for Indexed Jobs, condition
`Complete=True` (reason `KueueOffload`, message with the Slurm job ID). On failure:
`failed: 1`, condition `Failed=True` (reason `KueueOffload` or `Rejected`, message from the
executor). Clusters that require `SuccessCriteriaMet`/`FailureTarget` before a terminal condition
get those first: try with them, fall back without them on a 422.

Owner JobSet: condition `Completed=True` or `Failed=True` and `status.terminalState` `Completed`
or `Failed`. Owner TrainJob: condition `Complete=True` or `Failed=True`. Both with reason and
message as on the Job's condition; an owner that already has one of those conditions is skipped.

Owner pods at a terminal state: `status.phase` `Succeeded` or `Failed`, `reason` `KueueOffload`
(`Rejected` for a rejection), `message` as on the Job's condition. `status.containerStatuses` has
one entry per spec container (`name`, `image`, `ready: false`, `restartCount: 0`,
`state.terminated` with `exitCode`, reason `Completed` for 0 and `Error` otherwise, `startedAt` and
`finishedAt` from the executor's `startTime` and `completionTime`, the offload controller's clock when
unset). The exit code is the one `ranks[0].containers` reports for that container name, else 0
for a completed job and 1 for a failed or rejected one. `status.initContainerStatuses` has every
init container terminated with exit code 0. Pods already `Succeeded` or `Failed`, and pods that
no longer exist, are skipped.

## Kueue objects per site and project

As the white paper's appendix A, with Kueue v1beta2 field names: `cohortName`, and
`admissionChecksStrategy.admissionChecks: [{name: <site>}]` (the cluster runs Kueue 0.16.9).
