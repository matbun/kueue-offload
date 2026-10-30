# kueue-offload design

This file describes how kueue-offload works today and fixes the contracts its components are
built against. [kueue-native.md](kueue-native.md) describes the shape it should take to follow
Kueue and MultiKueue, and [backends.md](backends.md) the batch systems it runs on. The examples use
a site `hpc-a` and a project `proj42`.

## Components

| Component | Code | Image |
| --- | --- | --- |
| offload controller (Kueue admission check) | this repo, `cmd/offload-controller`, `pkg/offload` | `kueue-offload` (`/offload-controller`) |
| membership controller | this repo, `cmd/membership`, `pkg/membership` | same image (`/membership`) |
| CRDs `OffloadConfig`, `OffloadWorker`, `OffloadRequest`, `IdentityMap`, `ProjectQuota` | this repo, `api/v1alpha1`, `config/crd` | |
| names the executor shares with the controller and the API server | this repo, `pkg/executorapi` | |
| executor, in each tenant namespace | this repo, `cmd/executor`, `pkg/executor`, `pkg/shadow` | same image (`/executor`) |
| interLink API server, next to the executor | interLink `cmd/interlink`, with `PodStatus.SocketDir` | the stock API server image |
| plugin, next to the API server | an interLink plugin; for Slurm, `interlink-slurm-plugin` | the plugin image |

API group of the CRDs: `interlink.eu`, version `v1alpha1`. One offload controller serves every
site; one membership controller runs per site, with `--site=<OffloadConfig name>`. Neither reads
a Secret. The executor speaks only the interLink API, the one a virtual kubelet speaks, so the
plugin behind the API server is any interLink plugin ([backends.md](backends.md)).

## Admission check

The offload controller serves every AdmissionCheck with `controllerName: interlink.eu/offload`
and resolves each check's `parameters` (`apiGroup: interlink.eu`, `kind: OffloadConfig`)
separately, with Kueue's `ConfigHelper` (`sigs.k8s.io/kueue/pkg/util/admissioncheck`), as Kueue's
own check controllers do. A Workload is the controller's for every check state it carries whose
AdmissionCheck has that `controllerName` (Kueue's `FilterForController`); each such check runs on
its own, with its own OffloadConfig and its own requests.

A check's `Active` condition follows the OffloadWorkers its OffloadConfig's `workers` names, as
MultiKueue's checks follow their MultiKueueClusters: `True` (`Active`) while one is `Active`,
`False` with `NoUsableWorkers` and the missing or inactive ones in the message otherwise, and
`False` with `BadConfig` when the OffloadConfig cannot be read ([The OffloadWorker](#the-offloadworker)).

### The check stays Pending

The offload controller never sets the check `Ready`. Kueue therefore never admits the Workload,
never unsuspends the owner and never ungates plain pods, so no pod of the user's ever exists in the
cluster. Kueue keeps the quota reserved (`QuotaReserved=True`) for the job's whole life, queued and
running. The check `message` carries the batch system's state (`job 123 queued (Priority)`,
`job 123 running on node07 since ...`). When the job ends the controller writes the owner's
terminal status ([Owners](#owners)); the owner's own controller stops syncing a finished object,
and Kueue marks the Workload `Finished` and releases the quota. Verified on the reference cluster:
a suspended Indexed Job with `parallelism: 2` held at `Pending`, zero pods, then a status patch with
`Complete=True`, `succeeded: 2` gave `Finished=True(Succeeded)` on the Workload and a reservation
of 0.

`Ready` would make the Workload `Admitted`: Kueue would unsuspend the Job (or ungate the pods), and
the Job controller would create real pods that the default scheduler runs on cluster nodes. Only
`Job.spec.managedBy` (MultiKueue's way) stops that, and it is alpha and off by default before
Kubernetes 1.32. The API server of the reference cluster (1.30.2) drops it silently: a server
dry-run of a Job with `managedBy` returns it without the field.

### Transient submit failures

A submit the executor refuses for now (a limit on submitted jobs, a site or scheduler that cannot
be reached, a stage-in that still runs) leaves the check `Pending` with the quota held: the
executor writes the reason in the request's `status.reason` and `status.message` and tries again
on its next poll, and the check message says `waiting to submit (<reason>): <message>`. Such a
refusal ends once the user's other jobs end, so the Workload waits for its slot as long as it
lasts, instead of going through `Retry`, which would release the quota and, after a few attempts,
reject it. Seen when Kueue reserved quota for a second job by borrowing in the cohort while the
QOS took one job per user.

An executor that stops answering is noticed by its heartbeat ([Executor health](#executor-heartbeat)).

### Retries

A job is lost when the executor reports it `Evicted` (cancelled outside the executor, or a rank
gone from the plugin's status for `Executor.LostTimeout`), when no executor heartbeat came for
`executorLostTimeout`, or when it waited in the site's queue longer than the OffloadConfig's
`maxQueueTime` (unset: no limit). A lost job is retried through Kueue's own mechanism, as Kueue's
provisioning check does:

1. The controller sets the check to `Retry` with `requeueAfterSeconds` from Kueue's
   `UpdateAdmissionCheckRequeueState` (`sigs.k8s.io/kueue/pkg/workload`): `backoffBaseSeconds`
   doubled per retry, capped at `backoffMaxSeconds`.
2. It deletes the attempt's OffloadRequest; the executor's finalizer cancels the job first.
3. Kueue evicts the Workload, which releases the quota, and requeues it after the delay.
4. At the next reservation the controller creates the next attempt's OffloadRequest.

The retries spent are the attempt's number less one, read from the request (`spec.attempt`), not
from Kueue's `retryCount`: Kueue drops the state of a check that covers only some flavors, its
`retryCount` included, when it evicts the Workload. The deleted request keeps the controller's
finalizer `offload.interlink.eu/attempt`, and with it the attempt number, until the next attempt
exists or the Workload can have none (it went, finished or was deactivated). The next attempt is
one past the newest request of the check, deleted ones included, and past Kueue's `retryCount`
while the check state holds one; every new reservation after an eviction is an attempt, whatever
evicted the Workload.

Once `backoffLimitCount` retries were spent the check goes to `Rejected` instead and the owner
fails; 0 rejects at the first loss. The three limits are OffloadConfig's `retryStrategy`, which has
ProvisioningRequestConfig's type and defaults (3 retries, 60 s base, 1800 s cap): with them the
retries wait about 1, 2 and 4 minutes. `retryCount` is Kueue's; the controller never writes it. A
job that ran and failed is not retried.

### Workload deletion

Each OffloadRequest is controller-owned by its Workload, without `blockOwnerDeletion` (which would
need `update` on `workloads/finalizers`), so garbage collection deletes it with the Workload, and
the executor's finalizer `offload.interlink.eu/executor` cancels the job and removes its objects
before the request goes. A Workload goes with its owner (the Job deleted by the user or by its
TTL). A Workload that loses its reservation (evicted, deactivated, or its quota released) gets its
live requests deleted the same way.

### Owners

The controller handles Workloads owned by:

- a `batch/v1` Job;
- a `jobset.x-k8s.io/v1alpha2` JobSet;
- a `trainer.kubeflow.org/v1alpha1` TrainJob;
- plain pods of Kueue's `pod` integration: one pod with `kueue.x-k8s.io/queue-name` (the Workload's
  controller), or a pod group with `kueue.x-k8s.io/pod-group-name` and
  `kueue.x-k8s.io/pod-group-total-count` (every member owns the Workload, none as its controller).
  A homogeneous group is one PodSet with `count: N`, which Kueue names by a hash of the pod shape.

Any other owner kind is `Rejected` with a message saying so.

A Job, JobSet or TrainJob stays suspended for the job's whole life and finishes through its status:
the controller patches the `status` subresource with the condition Kueue's integration for that
kind reads ([Offload controller behaviour](#offload-controller-behaviour) lists the fields). JobSet
and TrainJob are read and patched as unstructured objects, with no Go module of either project.

Plain pods stay `SchedulingGated`. A single pod shows its job while it runs
([Notebooks and Services](#notebooks-and-services)). At a terminal state the controller patches
each owner pod's `status.phase` to `Succeeded` or `Failed`, with a terminated state for each
container, so a workflow engine that reads the main container (Argo) sees how the step ended;
`spec.nodeName` is never set. Kueue then removes its finalizers and marks the Workload
`Finished`. Verified on the reference cluster: a 2-pod group held at `Pending`, both pods patched
`Succeeded`, gave `Finished=True(Succeeded)` and a reservation of 0.

When a pod group fails or is rejected, the controller first sets
`kueue.x-k8s.io/retriable-in-group: "false"` on each group pod, then marks it `Failed`: without the
annotation Kueue waits for replacement pods and never finishes the group. Annotating first means a
pod is never left `Failed` without it if the controller stops halfway. Single pods and groups that
succeed are not annotated. The key and value come from Kueue's `pkg/controller/jobs/pod/constants`.

The controller needs `get` and `status` patch on `jobs`, `jobsets` and `trainjobs`, `get` and
`patch` on `pods` (the annotation) and `patch` on `pods/status` (the terminal phase).

### Owner annotations

A Workload's PodSet template holds only the pod template, while users put batch flags on the owner
(`slurm-job.vk.io/flags` on a Job's metadata). The controller copies the owner's annotations with
prefix `slurm-job.vk.io/`, `htcondor-job.vk.io/`, `interlink.eu/` or `offload.interlink.eu/` into
the request's `spec.ownerAnnotations`; the executor applies them to every PodSet template, whose
own value wins. The templates in the request are the Workload's as they are, so the executor can
compare them with the Workload. The owner is the Job, JobSet or TrainJob, or for pods the first
owner pod that still exists.

## Offload controller behaviour

Per Workload, for each of its checks this controller serves:

| Situation | Action |
| --- | --- |
| `Finished=True` | nothing; the request stays until the Workload goes, and with it the job directory and the shadow, so the logs stay readable ([Shadow](#shadow)) |
| no `QuotaReserved=True`, evicted or deactivated | delete the live requests ([Workload deletion](#workload-deletion)) |
| check not `Pending` | nothing (`Retry`: Kueue evicts next; `Rejected`: Kueue deactivates) |
| check `Pending`, no OffloadConfig | nothing, requeue after a minute (the check is inactive) |
| reserved, owner of an unsupported kind | check `Rejected` |
| reserved, check `Pending`, no live request | create the next attempt's OffloadRequest ([The OffloadRequest](#the-offloadrequest)); refusals found while building it (account label, flavors) are `Rejected` |
| a live request | map its status, newest first in this order: `Accepted=False`: check `Rejected` with the condition's message, owner `Failed` (reason `Rejected`); `Finished=True`: owner's terminal status ([Owners](#owners)), check stays `Pending` with `job N completed` or `job N failed: <message>`; `Evicted=True`: [Retries](#retries) (`job N cancelled: <message>`); no heartbeat for `executorLostTimeout`: [Retries](#retries); `QuotaReserved=True` for `maxQueueTime` without `Admitted`: [Retries](#retries); otherwise `Pending` with the state in the message, and a single owner pod's status follows the job ([Notebooks and Services](#notebooks-and-services)) |

The `Pending` messages: `StagingOut`: `job N ended, staging out: <message>`; `Admitted`: `job N
running on <node> since <start>`; `QuotaReserved`: `job N queued (<reason>): <message>`;
`StagingIn`: `staging in: <message>`; a reason or message and no condition: `waiting to submit
(<reason>): <message>`; `Accepted`: `accepted by the executor in namespace X`; nothing yet:
`waiting for the executor in namespace X`. With no heartbeat for twice `pollInterval` the message is
`no executor answering in namespace X since <time>` instead. A live request is looked at again every
`pollInterval`, and on every change of it.

The account is the ClusterQueue's label `hpc.interlink.eu/account`, which the membership controller
sets on the ClusterQueues it makes; a ClusterQueue without it is `Rejected`. Each PodSet's
annotations come from the OffloadConfig's `flavors` entry of its admitted flavors. Each value is a
Go text/template, executed with `.Account`, `.MaxExecSeconds` (the Workload's
`spec.maximumExecutionTimeSeconds`, 0 when unset) and `.MaxExecMinutes` (the same, rounded up):

```yaml
flavors:
  hpc-a-debug:
    annotations:
      slurm-job.vk.io/flags: "--account={{.Account}} --qos=debug --time={{or .MaxExecMinutes 30}}"
  hpc-a-prod:
    annotations:
      slurm-job.vk.io/flags: "--account={{.Account}} --qos=prod{{with .MaxExecMinutes}} --time={{.}}{{end}}"
```

PodSets admitted on different flavors (the Workload runs in one allocation, which takes one queue;
the message asks to pin one queue on every PodSet), a PodSet whose flavors render different
annotations, a PodSet none of whose flavors is listed (its job would carry no account) and a
template that does not execute are `Rejected`.

Check state writes are server-side apply patches of `status.admissionChecks` (a list keyed by
`name`) on `workloads/status` with field manager `kueue-offload`. They carry `name`, `state`,
`message`, `lastTransitionTime` and, on `Retry`, `requeueAfterSeconds`, never `retryCount`. A write
happens only when the state or the message changed, and `lastTransitionTime` moves only with the
state. Owner status patches use `jobs/status`, `jobsets/status`, `trainjobs/status` or
`pods/status`. Events on the Workload: `Submitted` for each attempt, `Running` when a job runs or
moves node, `Completed`, `Failed`, `Retry`, `Rejected`, and `ExecutorUnreachable` when the heartbeat
stops.

Owner Job status on completion: `startTime` and `completionTime` from the request's status, `succeeded` =
`completions` (`parallelism` when unset), `completedIndexes` `0-(n-1)` for Indexed Jobs, condition
`Complete=True` (reason `KueueOffload`, message with the job ID). On failure: `failed: 1`,
condition `Failed=True` (reason `KueueOffload` or `Rejected`, message from the executor). Clusters
that require `SuccessCriteriaMet`/`FailureTarget` before a terminal condition get those first: the
patch is tried with them and sent again without them on a 422.

Owner JobSet: condition `Completed=True` or `Failed=True` and `status.terminalState` `Completed`
or `Failed`. Owner TrainJob: condition `Complete=True` or `Failed=True`. Both with reason and
message as on the Job's condition; an owner that already has one of those conditions is skipped.

Owner pods at a terminal state: `status.phase` `Succeeded` or `Failed`, `reason` `KueueOffload`
(`Rejected` for a rejection), `message` as on the Job's condition, conditions `Ready` and
`ContainersReady` `False`. `status.containerStatuses` has one entry per spec container (`name`,
`image`, `ready: false`, `restartCount: 0`, `state.terminated` with `exitCode`, reason
`Completed` for 0 and `Error` otherwise, `startedAt` and `finishedAt` from the executor's
`startTime` and `completionTime`, the controller's clock when unset). The exit code is the one the request's `status.ranks[0].containers` reports for that container name, else 0
for a completed job and 1 for a failed or rejected one. `status.initContainerStatuses` has every
init container terminated with exit code 0. Pods already `Succeeded` or `Failed`, and pods that
no longer exist, are skipped.

The executor's `Shadow.AlwaysShadow` decides which jobs get a shadow.

## The OffloadWorker

An `OffloadWorker` is one site, cluster scoped and named after it, with MultiKueueCluster's role
for a site that has no Kubernetes API. Types in `api/v1alpha1/offloadworker_types.go`.

```yaml
apiVersion: interlink.eu/v1alpha1
kind: OffloadWorker
metadata: {name: hpc-a}
spec:
  loginEndpoint: login.hpc-a.example.org:22
```

The offload controller writes its `Active` condition from site-level facts only, as MultiKueue
writes a MultiKueueCluster's: `True` while a TCP connection to `loginEndpoint` succeeds (tried
every minute, 10 s timeout), `False` with `BadConfig` when the endpoint is not host:port and with
`Unreachable` when the connection fails. It never depends on one tenant's executor, since an
inactive check makes every ClusterQueue that uses it inactive; executors answer per request
([Executor heartbeat](#executor-heartbeat)). An OffloadConfig names the workers its check may use
in `workers`, one until a dispatcher chooses among several. The request's `spec.site` and its label
`offload.interlink.eu/site` are that worker's name, which each executor selects on. The controller
needs `get`, `list` and `watch` on `offloadworkers` and `get` and `patch` on
`offloadworkers/status`.

## The OffloadRequest

The offload controller and the executors talk through a namespaced object, as Kueue's provisioning
check talks to the cluster autoscaler through ProvisioningRequests, not through HTTP. Types in
`api/v1alpha1/offloadrequest_types.go`.

For each attempt the controller creates an `OffloadRequest` named `<workload>-<check>-<attempt>`
(hashed past 253 characters, as Kueue names ProvisioningRequests) in the Workload's namespace,
controller-owned by the Workload, labelled `offload.interlink.eu/site: <site>`, with the finalizer
`offload.interlink.eu/attempt`. The controller writes the spec once and never the status; the
executor writes the status and its finalizer.

### Spec

```yaml
spec:
  workload: {name: job-train-5d565, uid: 0b6c1d2e-3f40-4a5b-8c6d-7e8f9a0b1c2d}
  admissionCheck: hpc-a
  attempt: 1
  site: hpc-a                  # the OffloadWorker, the check's OffloadConfig's workers[0]
  account: proj42              # the ClusterQueue's hpc.interlink.eu/account label
  owner: {apiVersion: batch/v1, kind: Job, name: train, uid: ...}
  ownerAnnotations: {slurm-job.vk.io/flags: "--gres=gpu:4"}
  podSets:
  - name: main
    flavors: {cpu: hpc-a-prod, nvidia.com/gpu: hpc-a-prod}
    count: 4
    annotations: {slurm-job.vk.io/flags: "--account=proj42 --qos=prod --time=240"}
    template: {metadata: {}, spec: {}}
    secretRefs: [hf-token]
    configMapRefs: [train-config]
  maximumExecutionTimeSeconds: 14400
  priority: 100
```

- `owner` is the Job, JobSet or TrainJob, or for pods `v1` `Pod` named after the pod group
  (`kueue.x-k8s.io/pod-group-name`; the pod's name for a single pod), with the first owner pod's
  UID.
- `ownerAnnotations` ([Owner annotations](#owner-annotations)) apply to every template; the
  template's own value wins.
- `ownerLabels`: for a Workload of one plain pod, that pod's labels as they are, since Kueue's
  PodSet template of a pod holds its spec only; empty for every other owner. Only the shadow
  uses them ([Notebooks and Services](#notebooks-and-services)).
- Per PodSet, `flavors` and `count` are Kueue's admission, `annotations` the flavor's, expanded;
  empty when the flavor has none. The executor appends each value to the template's own value for
  that key, after a space, and never parses it. `template` is the Workload's PodSet template as it
  is.
- `secretRefs` / `configMapRefs`: every name the template references through volumes (`secret`,
  `configMap`, `projected` sources), `envFrom` and `env[].valueFrom`. The executor fetches exactly
  these from its own namespace.
- Several pods run as one allocation holding the members of every PodSet
  ([Workload shapes](#workload-shapes)). The PodSets must then carry the same annotations (one
  queue); with a site launcher (`interlink.eu/launcher`) also the same image, command and args of
  their first container, because the launcher starts the head's container on every rank. With
  `offload.interlink.eu/independent-pods: "true"` they run as one job per pod instead, and neither
  rule applies.

### Status

The executor of the namespace whose site is `spec.site` takes a request by adding its finalizer
`offload.interlink.eu/executor` and setting `Accepted=True`, and writes on every poll:

| Field | Meaning |
| --- | --- |
| `lastSyncTime` | the heartbeat, on every poll, also when nothing changed ([Executor heartbeat](#executor-heartbeat)) |
| condition `Accepted` | `True` once taken; `False` for a permanent refusal, with the refusal as message: an account not in `AllowedAccounts`, a site it does not serve, a request its Workload does not match (below), PodSets of one allocation with different annotations (or, with a launcher, different containers), several pods in one allocation on a plugin that does not list `pod-group`, exposed ports or reverse forwards on a site with no `Shadow.LoginHost`, more than 9999 members, a stage path outside the staging volumes, a failed stage-in, a reference it cannot read, a token it may not request, the plugin refusing the job with 400, 403 or 422 |
| condition `StagingIn` | `True` while the stage-in Job runs |
| condition `QuotaReserved` | `True` once the job is submitted and waits in the site's queue; its `lastTransitionTime` starts `maxQueueTime` |
| condition `Admitted` | `True` while a container of a rank runs, or the head has a node; `False` while the job waits in the site's queue, also while ranks of independent pods wait for the site after the others ended |
| condition `StagingOut` | `True` while the stage-out Job of an ended job runs |
| condition `Finished` | `True` once every rank ended and staging out is done: reason `Succeeded` (every rank exited 0) or `Failed`, with the message saying why (the failed rank, `SlurmJobTimeout`, `OOMKilled`, the site refusing the job, a failed stage-out) |
| condition `Evicted` | `True` when the job was lost: a container terminated with reason `Cancelled` (cancelled outside the executor), or a rank missing from the plugin's status for `Executor.LostTimeout` (reason `Vanished`); `False` with reason `RankMissing` while a rank is missing, since its `lastTransitionTime` |
| `reason`, `message` | what it is doing or waiting for: the first waiting container's reason and message of a queued job; for a submit refused for now (staging in, the plugin's 503, a cluster read that failed) why |
| `jobID`, `nodeName`, `nodes`, `socketDir`, `startTime`, `completionTime` | as the plugin's `/status` reports them; for independent pods `jobID` lists every rank's job, comma-separated, or the first and the last past eight; `nodeName` is the head rank's node, the one the shadow tunnels to |
| `ranks[]` | `rank`, `podUID` (empty for a rank of independent pods that waits for the site), `jobID` (for independent pods the one `/create` named), `state` (`queued`, `running`, `completed`, `failed`), `exitCode`, `containers` (`corev1.ContainerStatus` as the plugin reports them; the head rank's exit codes reach the owner pods) |
| `shadow` | `deployment`, `service` of the shadow; `podIP` and `ready` (its `Ready` condition) of its pod, a ready one if there is one, pods being deleted left out |

When a request is deleted, the executor cancels its job unless it ended (`/delete` for every
member the plugin has, which also removes its files), deletes the shadow objects and the stage
Jobs, and removes its finalizer; a failure keeps the finalizer and is tried again. Before it
submits, also before a stage-in, it checks the request against the Workload it names, read from
the API server (`get` on workloads): the request's controller owner is that Workload by name and
UID, the Workload has the check, and its PodSets (name, template) and their admission (count,
flavors) are the request's; otherwise it refuses the request. A Workload without its reservation
(`QuotaReserved` not `True`, evicted, inactive or finished) or with the check not `Pending` makes
the request wait, since the controller deletes it. A request also waits while an earlier attempt
of its Workload still carries the executor's finalizer: both attempts' members, shadow and stage
Jobs carry the Workload UID.

### Who may write it

The controller needs `create`, `get`, `list`, `watch`, `patch` (its finalizer) and `delete` on
`offloadrequests`; the executor `get`, `list`, `watch` and `patch` (its finalizer) on
`offloadrequests` and `patch` on `offloadrequests/status`, in its namespace only. Tenants get no
write on the kind. Because a request is what makes an executor submit as its namespace's user, the
ValidatingAdmissionPolicy `kueue-offload-offloadrequests` (`config/admission`,
`admissionregistration.k8s.io/v1`, Kubernetes 1.30 or later) holds anyone else with write to the
same split, on create and update of `offloadrequests` and `offloadrequests/status`. It needs the
API server's `ValidatingAdmissionPolicy` admission plugin: a cluster that serves the API but does
not run the plugin accepts the policy and enforces nothing (a deny-all probe policy shows it), and
there RBAC alone guards the kind.

| Change | Only by |
| --- | --- |
| create a request | the controller's ServiceAccount (`kueue-offload` in `kueue-offload`) |
| its `spec`, its label `offload.interlink.eu/site` | the controller |
| add or remove the finalizer `offload.interlink.eu/attempt` | the controller |
| its `status` | the ServiceAccount `kueue-offload-executor` of the request's own namespace |
| add or remove the finalizer `offload.interlink.eu/executor` | that same executor |

Other metadata stays open, so the garbage collector can drop an owner reference. Deletion is not
matched. To release a request whose executor is gone for good, an administrator deletes the
binding, removes the finalizer and applies the binding again. The executor's ServiceAccount name is the one the executor template renders, after
`executorService`; a site that renders another name edits the policy's `executor` variable.
`pkg/offload/policy_test.go` compiles the policy with Kubernetes 1.30's CEL environment and runs
each rule.

### Executor heartbeat

`lastSyncTime` (the request's creation before the first one) tells the controller that an
executor answers in the namespace. With none for twice the OffloadConfig's `pollInterval` the
check message says `no executor answering in namespace X since <time>`; with none for
`executorLostTimeout` the check goes to `Retry`, as MultiKueue's `workerLostTimeout`. Executors
therefore poll at least every `pollInterval` (`Executor.PollInterval`). An executor that cannot
reach the interLink API server or the plugin writes no heartbeat, so such a failure also ends in
`Retry`. Polls stop once the job finished or was lost. Only the site's facts make a check inactive
([Admission check](#admission-check)), never one tenant's executor.

## Executor HTTP API

The offload controller calls no executor endpoint: submitting a job, reading its state and
cancelling it go through the OffloadRequest, so the executor authenticates no caller and needs no
ingress from the controller's namespace, and the controller resolves no executor address. What
remains is for the shadows: plain text answers, no wire types. `pkg/executorapi` holds the names
the executor shares with the controller and the API server.

### GET /getLogs

Query: `workloadUID` (required), `container` (default: each member's first container), `rank`
(optional), `follow` (`true`/`false`), `tail` (lines), `sinceSeconds`. Every member is read through
the API server's `/getLogs`, as the plugin returns it (for Slurm, the job's `job.out` and the
container's output).

- With `rank`: that rank's log.
- Without `rank` and one member: rank 0, no prefix.
- Without `rank` and several members: every member that has the container, each line prefixed
  `[rank N] `. Without `follow`: rank 0's lines, then rank 1's, and so on, `tail` over the whole.
  With `follow`: the lines of every rank's followed stream as they arrive, `tail` per rank.

The logs are those of the Workload's newest submitted request that still carries the executor's
finalizer; with none, 404. A followed log starts once the job ran (the request's `startTime`,
`Admitted=True` or an end), so the plugin is not asked to follow a log that does not exist yet. It ends when every rank's stream ended and the plugin's `/status` shows those containers
terminated. A rank's stream that ends before that (a plugin without Follow, or the API server
ending a stream it lost from the plugin as if it were complete) is opened again after 5 s with the
lines already sent skipped, so each line reaches the caller once; a last line without a newline is
held back until the container ended. With `tail` or `sinceSeconds` the lines of a new stream
cannot be matched to the old one, so such a stream is broken: the executor aborts the answer, and
the shadow's `executor logs` reconnects.

No authentication: the sample's NetworkPolicy lets only the namespace's shadow pods reach it.

### GET /healthz

200 `ok` while the API server answers `/pinglink`, which reaches the plugin; a success is trusted
for 30 s. 503 otherwise. Each answer also updates the plugin's capabilities
([Workload shapes](#workload-shapes)).

## Executor

The executor runs in the tenant's namespace and calls the interLink API server next to it in the
tenant pod, as a virtual kubelet does: one `/create`
per member with a PodCreateRequests, `/status` with every member and a phase, `/getLogs`,
`/delete` and `/pinglink`, with interLink's own wire types (`pkg/interlink`). The plugin behind the
API server renders and runs the job (for Slurm, the job script, the resource translation, the
launchers and the probes) with the tenant's own site key. The executor holds no batch system
flags and reads no job directory.

It is a controller-runtime manager whose cache holds the OffloadRequests of its namespace
labelled `offload.interlink.eu/site: <its site>` only, so a namespaced Role is enough. It
reconciles up to four requests at once, a request never twice at once, and ignores its own
status writes: each submitted request is polled every `Executor.PollInterval`. A request it
shows unsubmitted is read again from the API server before a submit, since the cache can lag
behind the executor's last status write and a second submit would start a second job. It needs,
in its namespace: `get`, `list`, `watch` and `patch` on `offloadrequests`, `patch` on
`offloadrequests/status`, `get` on `kueue.x-k8s.io` `workloads`, and the rules of
`config/rbac/executor/role.yaml` for Secrets, ConfigMaps, tokens, shadows and stage Jobs.

What it keeps about a request lives in the request's status, so a restarted executor follows
every job from its request alone: the job ID, the ranks (each member's `podUID`, and for
independent pods the job `/create` named; a rank without a `podUID` waits for the site), the
shadow's names, and since when a rank is missing (`Evicted=False`, `RankMissing`). Only one fact
is in memory: that an ended job's shadow was rendered for the end. A restarted executor renders it
once more, which changes nothing.

### Reconciling a request

`pkg/executor/request.go`, for each request of its namespace and site:

1. A deleted request: cancel the job, delete the shadow and the stage Jobs, remove the finalizer
   ([Status](#status)).
2. Add the finalizer `offload.interlink.eu/executor` if missing.
3. A refused request (`Accepted=False`) or one whose job finished or was lost: nothing more,
   except rendering an ended job's logs-only shadow once.
4. No job yet: read the request again from the API server, check its shape, its Workload and
   the earlier attempts (refuse, or wait with the reason in the status), set `Accepted=True` and
   submit, staging in first.
5. A submitted job: create the ranks of independent pods that wait for the site (after reading
   the request again), read the job's state through `/status`, ensure the shadow, run the
   stage-out, and write the status with the heartbeat, or without one when the API server or the
   plugin cannot be reached. Requeue after `Executor.PollInterval` until `Finished` or `Evicted`.

### One site per executor

The executor checks the account against its only `Sites` entry and refuses a configuration with
more or fewer. Two sites need two executors (one Deployment per site in the namespace). How the
plugin reaches the site is the plugin's configuration: the Slurm plugin runs `sbatch`, `squeue`
and `scancel` through ssh shims that read `REMOTE_USER`, `REMOTE_LOGIN`, `REMOTE_PORT` and
`SSH_KEY` from the plugin container's environment. One executor holds one site user, so the
membership controller gives a namespace mapped to two users of one site no executor.

### Configuration

`/executor --config <file>` (default `/etc/kueue-offload/executor.yaml`; `--log-level`, default
`info`). Unknown keys are refused.

```yaml
Listen: ":8000"            # /getLogs and /healthz; the default
InterLink:
  URL: http://127.0.0.1:3000   # the interLink API server
Executor:
  Namespace: ""            # empty: read from the ServiceAccount namespace file
  PollInterval: 15s        # the default; at most the OffloadConfig's pollInterval
  KubernetesAPI:           # the API server as a job reaches it; empty Host: not set
    Host: ""
    Port: ""
  AutomountServiceAccountToken: false
  LostTimeout: 10m         # a member missing from /status this long: the job vanished
Sites:
  hpc-a:
    AllowedAccounts: [proj42, proj99]
Shadow:
  Enabled: true
  AlwaysShadow: true       # unset: true
  Image: ghcr.io/interlink-hq/interlink/ssh-tunnel:0.6.3-pre2
  ExecutorImage: ""        # image of the job-logs container; empty: env EXECUTOR_IMAGE
  ExecutorURL: http://kueue-offload-executor:8000
  LoginHost: login.hpc-a.example.org   # empty: logs-only shadows, no tunnels
  User: alice
  Port: 22                 # 0: 22
  KeySecret: hpc-a-tunnel-key
  KeySecretKey: ssh-privatekey
  KnownHostsConfigMap: hpc-a-known-hosts
  NodeHostKeys: verify
  ForwardTarget: node
  ForwardMode: portforward
  ExecConnectCommand: ""   # relay of ForwardMode exec; empty: nc
  NodeWaitTimeout: 30m
  Reverse:                 # the virtual kubelet's Network.SSH.Reverse, plus AllowedTargets
    Enabled: false
    AllowedTargets: []
Staging:
  Enabled: true
  Image: registry.a.example.org/stage-tools:1
  Command: /usr/local/bin/stage-rsync   # the default
  TransferHost: transfer.hpc-a.example.org
  User: ""                 # empty: Shadow.User
  StageRoot: /scratch/proj42/stage
  KeySecret: ""            # empty: Shadow.KeySecret
  KeySecretKey: ""         # empty: Shadow.KeySecretKey, else ssh-privatekey
  KnownHostsConfigMap: ""  # empty: Shadow.KnownHostsConfigMap
  Volumes:                 # the namespace's claims the staged paths live on
  - {Name: data, ClaimName: data, MountPath: /data, Kerberos: true}
  - {Name: shared, ClaimName: shared, MountPath: /shared}
  Kerberos:                # for paths on a Kerberos volume
    KeytabSecret: alice-keytab
    KeytabKey: keytab
    Principal: alice@A.EXAMPLE.ORG
    Krb5ConfigMap: krb5-config
```

The plugin needs a pod group to run as one job: for Slurm, `GangSchedulingEnabled: true` (and a
`GangTimeout`, 10 minutes in the sample) in its own configuration, which makes its ping list
`pod-group`.

### Workload shapes

The executor reads the plugin's capabilities from its ping answer, the `capabilities` list that
interLink's PingResponse does not have upstream yet, at start and every 5 minutes; `/healthz`
pings update them too. A missing field, or an answer that is not JSON, means none, and a failed
ping keeps the last answer. A submit that needs them before the first answer pings at once; a
failed ping leaves the request waiting without a heartbeat.

| Shape (Workload PodSets) | Plugin that lists `pod-group` | Plugin without it |
| --- | --- | --- |
| one pod: a single pod, a Job with parallelism 1 | one job | one job |
| several pods: a Job with parallelism N, a pod group, a JobSet, a TrainJob | one allocation, ranks numbered across the PodSets | refused (`Accepted=False`) naming `pod-group` and `offload.interlink.eu/independent-pods` |
| several pods, `offload.interlink.eu/independent-pods: "true"` on every template | one job per pod | one job per pod |

The owner's annotation reaches every template ([Owner annotations](#owner-annotations)). Counts come
from the admitted PodSetAssignments. A refusal prevents a gang whose ranks start hours apart from
holding allocations while they wait for each other; the project can pin a flavor on another site,
or mark the owner independent.

### How the executor builds the members

The executor expands each PodSet into `count` member pods that exist only in its memory and sends
each to `/create`, in rank order. Each template first gets the request's `ownerAnnotations` it
does not set itself. Members are numbered across the PodSets in request order,
and `<count>` below is the total of every PodSet's count:

- member `i` (0-based): `metadata.name` `<owner name>-<i>`, `uid` a name-based UUID of the
  Workload UID and `i` (the API server takes only UUIDs as pod UIDs), namespace the Workload's,
  labels and annotations from its own PodSet's template, plus label
  `batch.kubernetes.io/job-completion-index: "<i>"` and annotation
  `interlink.eu/podset: <PodSet name>`;
- a Workload of one plain pod is that pod: the member takes the pod's name, and its downward API
  reports the pod's UID (annotation `interlink.eu/downward-uid`), because a workflow engine's
  executor (Argo's) addresses its own pod by name and UID;
- `count > 1` and not independent pods: also Kueue's pod-group label
  `kueue.x-k8s.io/pod-group-name: <workloadUID>` and annotation
  `kueue.x-k8s.io/pod-group-total-count: "<count>"`, and the Slurm plugin's `interlink.eu/gang-rank`
  and `interlink.eu/gang-role` (member 0 of the first PodSet is the head), so the plugin's gang path
  buffers the members and submits one `sbatch --nodes=<count>`. It answers the members before the
  last with no job ID; the job ID is the last non-empty one. A plugin that lists `podset-create`
  gets the members in one `/createPodSets`, by PodSet in rank order, and answers every one with the
  job ID ([backends.md](backends.md) 3.3). Independent pods carry none of these
  keys, and each member's answer names its own job. When the site refuses an independent member
  for now (a 503, such as a per-user job limit), the members it took stay: the rest are recorded
  as ranks without a `podUID`, each later poll creates them in rank order, the request's reason
  `WaitingForSite` and message say which rank waits and why, and the workload ends only once
  every rank ran. A pending rank refused for good
  fails the workload;
- every resource that has a request and no limit gets a limit equal to the request: Kueue charges a
  Workload by its requests while the plugin sizes the job from limits, so a Job that requests 20
  CPUs is not submitted with `--cpus-per-task=1`;
- env `INTERLINK_RANK=<i>` and `INTERLINK_WORLD_SIZE=<count>` added to every container;
- the PodSet's `annotations` appended to the template's own, after a space, so the site's flags
  come last;
- what the API server and the kubelet would have done for a real pod: every container gets the
  ServiceAccount directory (namespace and `ca.crt`, the token only with
  `AutomountServiceAccountToken`) unless the template opts out or mounts it. A plain pod already
  carries the API server's `kube-api-access-*` volume: its token is dropped (namespace and `ca.crt`
  stay) unless `AutomountServiceAccountToken` is set or the owner asks for it with
  `offload.interlink.eu/service-account-token: "true"` (a pipeline step, whose launcher calls the
  API server), so no cluster token is written at the site by default. The remaining
  `serviceAccountToken` projections get a token from the TokenRequest API; with
  `KubernetesAPI.Host` and a token, the
  containers get `KUBERNETES_SERVICE_HOST` and `KUBERNETES_SERVICE_PORT`;
- `env[].valueFrom` resolved by the executor (secretKeyRef, configMapKeyRef, fieldRef for
  `metadata.name|namespace|uid|labels['k']|annotations['k']`, `spec.serviceAccountName`,
  resourceFieldRef for requests and limits), and `envFrom` turned into env with the kubelet's
  precedence, as the virtual kubelet does before calling `/create`;
- the PodCreateRequests carries the Secrets and ConfigMaps of the pod's `secret` and `configMap`
  volumes and one ConfigMap per projected or downward API volume with its files (a downward API
  volume becomes a projected one), as the virtual kubelet builds it; the API server hands each
  container the data of the volumes it mounts, and the plugin writes them.

A `/create` that fails makes the executor `/delete` the members created so far, which also drops
the ones a pod group buffers; a `/createPodSets` that fails, or answers other than one job for
every member in rank order, makes it `/delete` them all, except a 404 from an API server without
the route, after which the members go to `/create`. A 503 leaves the request waiting, with the
plugin's text as message and a heartbeat; a 400, 403 or 422 refuses it (`Accepted=False`) with
the plugin's text; anything else leaves it waiting without a heartbeat ([backends.md](backends.md)
3.4).

`/status` sends every created member with `status.phase` `Pending` until a rank ran, then
`Running`: the API server asks the plugin again only for pods in one of those phases. Values of
Secrets and ConfigMaps are read from the cluster when needed, never stored.

### Staging

Paths listed, `:`-separated, in a template's `interlink.eu/stage-in` and `interlink.eu/stage-out`
annotations (owner annotations included, [Owner annotations](#owner-annotations); the union over
the PodSets) are copied through the site's transfer host by a batch Job in the executor's
namespace that runs `Staging.Command`. Every path must be absolute, clean and below a
`Staging.Volumes` mount path, else the request is refused.

- A request with stage-in paths creates `stage-in-<first 8 of the UID>` once, keeps
  `StagingIn=True` with message `Job <name> is running` while it runs, is refused with the Job's
  failure reason if it failed, and submits as usual once it succeeded (`StagingIn=False`, reason
  `Succeeded`).
- When the job completed or failed and has stage-out paths, the executor creates
  `stage-out-<8>`; the request has `StagingOut=True` with message `Job <name> is running` and no
  `completionTime` until it ends, then `StagingOut=False` and `Finished`. A failed stage-out makes
  the job `Failed` (reason `StageOutFailed` for a job that had completed), the message says so. A
  cancelled job is not staged out: the offload controller deletes its request and submits the
  Workload again ([Retries](#retries)).
- Deleting the request deletes both Jobs (propagation `Background`), also for a request whose
  stage-in never let it be submitted.

A stage Job has labels `interlink.eu/stage-of: <workloadUID>`, `interlink.eu/stage-direction: in|out`
and `sidecar.istio.io/inject: "false"`, `backoffLimit: 0`, `restartPolicy: Never`, requests
50m/64Mi, limits 2 CPU/2Gi, the site key at `/ssh` and the known hosts at
`/etc/ssh/ssh_known_hosts`, and mounts only the volumes its paths are on. The command's contract is
its environment: `STAGE_DIRECTION` (`in`/`out`), `STAGE_PATHS`, `REMOTE`
(`<Staging.User>@<TransferHost>`), `STAGE_ROOT`, `STAGE_NAMESPACE`, `SSH_KEY`, and for a path on a
`Kerberos` volume `KEYTAB`, `PRINCIPAL` and `KRB5CCNAME=FILE:/tmp/krb5cc` (keytab at `/keytab`,
`krb5.conf` from `Krb5ConfigMap`). The executor needs `get`, `list`, `create` and `delete` on
`batch/jobs` in its namespace.

The job finds the staged inputs at their own paths only through binds of the site's stage
directory onto them in the plugin's configuration (for Slurm, `SingularityDefaultOptions`): that is
the deployment's configuration, the executor adds no bind.

### Job directory

The Slurm plugin's job directory (`DataRootFolder`) must be the site's filesystem, at the same
absolute path as on the site. Tenant namespaces enforce the `baseline` Pod Security level, which
forbids the privileged container an sshfs sidecar needs, so the job directory is a CSI volume:
`csi-rclone`, SFTP to the site, mounted by the node plugin. Measured: a local write is visible
remotely at once, a remote append is visible locally after 0.2 s, `rm -rf` works; `chmod` is not
carried (files land as 0644), so the site's `sbatch` shim makes the job's scripts executable
remotely before submitting.

On that volume a rename onto an existing file fails on the server while the mount reports success
(`sed -i` returned 0, the remote file was unchanged and the temporary file stayed), and a file
opened read-write (`os.Create`) is uploaded after `close`, so the upload could land after `sbatch`
read the script or after its mode was set (`job.sh: Permission denied` in one run of two). The
plugin therefore opens the job script, the job's `job.sh`, its environment files and its
bookkeeping files write-only, and the site's `sbatch` shim rewrites files in place.

### Shadow

The shadow is a Deployment in the tenant namespace, outside every ClusterQueue, that the executor
renders for a job: an SSH tunnel to the job's exposed ports (`ssh-forward`), a Service when the
template exposes ports, a ConfigMap with the compute node and the socket directory the plugin
reports in `/status`, and a `job-logs` container (`/executor logs`) that streams the job's output
from the executor's `GET /getLogs`. With `Shadow.AlwaysShadow` every job gets one, also without
exposed ports, so its logs are always reachable. The executor ensures the shadow on every poll
once the job runs: a shadow that tunnels waits until the plugin reports the node, one that only
streams logs starts at once, also on a backend that reports no node. A shadow that tunnels for
a Workload of one plain pod also carries the labels its Services select the pod by
([Notebooks and Services](#notebooks-and-services)). Each poll records the IP and readiness of
the shadow's pod in the request's `status.shadow`.

Without `Shadow.LoginHost` every shadow only streams logs, and neither `Shadow.User` nor
`Shadow.KeySecret` is needed: for a backend whose nodes the cluster cannot reach, or that reports no
node. The executor then refuses a head template that exposes a TCP port (container ports or
`interlink.eu/wstunnel-extra-ports`) or asks for reverse forwards, and `Shadow.Reverse.Enabled` is
refused at start.

At a terminal state the executor renders it once more as logs-only: no `ssh-forward`, no
`ssh-reverse`, no init containers, no Service, only `job-logs`, which prints the whole log and
then stays idle, so `kubectl logs <shadow> -c job-logs` still shows the output after the job ended.
The executor deletes the shadow when the job's OffloadRequest is deleted: when the Workload is
deleted, loses its reservation, or its attempt is retried.

A head template with `interlink.eu/reverse-forwards` also gets a tunnel from the compute node back
to the listed cluster endpoints (`ssh-reverse`), parsed with the virtual kubelet's rules plus
`Shadow.Reverse.AllowedTargets`; its set-up goes before rank 0's `slurm-job.vk.io/pre-exec`, as the
virtual kubelet puts it. It needs `Shadow.Reverse.Enabled` and a plugin that reports a socket
directory, and a gang with a site launcher cannot have one, because only the head rank has the
tunnel. Of independent pods only rank 0, the head, has it.

`pkg/executor` talks to `pkg/shadow` through this interface:

```go
// package shadow
type Spec struct {
    WorkloadUID  string
    WorkloadName string // the owner's name, base of the shadow's resource names
    Namespace    string
    Template     corev1.PodTemplateSpec // rank 0's template: ports, annotations, labels
    NodeName     string                 // empty until running
    SocketDir    string
    Terminal     bool // job ended: logs-only shadow
    // Reverse is the job's tunnel back into the cluster, nil unless it asked for
    // one (ParseReverseTunnel).
    Reverse *ReverseTunnel
    Labels  map[string]string // the request's ownerLabels
}

type Ref struct {
    Deployment, Service, NodeConfigMap string
    PodIP                              string // of the shadow's pod, a ready one if there is one
    Ready                              bool
}

type Manager interface {
    // Ensure creates or updates the shadow for spec: Deployment, Service (only with exposed
    // ports), node ConfigMap with compute-node and socket-dir, and reads its pod's IP and
    // readiness. A shadow that tunnels carries the labels of spec.Labels a Service of the
    // namespace selects by, less Kueue's. Idempotent; called on every poll once the job runs.
    Ensure(ctx context.Context, spec Spec) (Ref, error)
    // Delete removes every object labelled for this workload. Idempotent.
    Delete(ctx context.Context, workloadUID string) error
}
```

## Notebooks and Services

A Kubeflow Notebook runs as a StatefulSet whose pod carries `kueue.x-k8s.io/queue-name`, so Kueue's
pod integration makes it a Workload of one plain pod, held gated. Its Service selects
`statefulset: <name>` and sends port 80 to the container's port (8888 by default); the Kubeflow UI
shows the Notebook ready, with Connect, once the StatefulSet counts one ready replica. A shadow
carrying the pod's labels and a pod reported `Running` gave both under the virtual kubelet; here:

- The controller copies the pod's labels into the request's `spec.ownerLabels`. A shadow that
  tunnels carries those by which a Service of the namespace selects the pod, read on every
  poll, and no other: Kueue's (`kueue.x-k8s.io/*`) never, since Kueue gates a pod with its
  queue label, and the shadow's own keys win. Copying every label, as the virtual kubelet does,
  would also hand the shadow the labels PodDefaults and policies select pods by: the spawner's
  PodDefault that sets the queue label would get the shadow gated. The Service then selects the
  shadow, which forwards the pod's ports as any shadow does. The logs-only shadow of an ended job
  carries none of them, so no Service reaches it.
- The executor records the shadow pod's IP and readiness in `status.shadow`.
- While the request is `Admitted`, the controller patches the gated pod's status the way a kubelet
  would, as MultiKueue copies a worker's pod status onto the manager's gated pod: phase
  `Running`; `startTime` and every container running since the job's start, init containers
  terminated with 0; `Initialized` `True`; `Ready` and `ContainersReady` `True` while the shadow
  is ready and the job is not staging out, `False` with reason `KueueOffload` otherwise; `podIP`
  and `podIPs` the shadow pod's. `PodScheduled` stays the gated pod's and `spec.nodeName` stays
  empty. The StatefulSet counts a `Running` pod with `Ready` `True` as ready, and the notebook
  controller copies its `readyReplicas` to the Notebook.
- When the job stops running without ending (requeued at the site, lost, retried, or the Workload
  lost its reservation), the pod goes back to a gated pod's status: `Pending`, no IP, no container
  statuses, no kubelet conditions. At the end it takes the terminal status of [Owners](#owners).

Kueue 0.16.9 keeps such a pod suspended: its pod controller's `IsSuspended` is "gated or terminal",
`Finished` a terminal phase, `IsActive` of a single pod always false, and the jobframework reads
`PodsReady` only for an admitted Workload; its pod webhooks match `pods`, not `pods/status`. Jobs,
JobSets, TrainJobs and pod groups get none of this: a Service in front of several ranks would
reach only the head rank's tunnel.

## Membership controller

The membership controller keeps one site's Kueue objects, namespace labels and executors in step
with the site's Slurm associations (a ConfigMap holding
`sacctmgr --parsable2 --noheader show assoc format=cluster,account,user`) and the `IdentityMap`s,
which map a Kubernetes user to a namespace and a site user. It never infers a mapping from names.
It runs a pass when its OffloadConfig, an IdentityMap, a ProjectQuota of the site, a ConfigMap of
its namespace or an executor Deployment changes, and every `membership.syncInterval` (default 15
minutes), every minute while an executor is not available.

### Kueue objects per site and project

With Kueue v1beta2 field names (the reference cluster runs Kueue 0.16.9):

- a Cohort named `membership.cohort` (default: the site name);
- per account with members, a ClusterQueue `<site>-<account>` labelled
  `hpc.interlink.eu/account: <account>` (the offload controller charges that account) with
  `cohortName` the site's Cohort,
  `namespaceSelector` on `hpc.interlink.eu/<site>-<account>: "true"`, `queueingStrategy:
  BestEffortFIFO`, `preemption.withinClusterQueue` and `reclaimWithinCohort` `Never`,
  `admissionChecksStrategy.admissionChecks: [{name: <membership.admissionCheck, default the site
  name>}]`, and `resourceGroups` from the ProjectQuota `<site>-<account>`, else from
  `membership.defaultProjectQuota`;
- per membership, a LocalQueue `<site>-<account>` in the member's namespace.

The ResourceFlavors, the AdmissionCheck and the OffloadConfig are the deployment's. A membership
that disappears loses its label and its LocalQueue; the namespace and the executor stay.

### Namespace labels

`managedJobsNamespaceSelector` is cluster-wide Kueue configuration shared with other tenants, so
the deployment keeps the cluster's selector. The membership controller only adds its own labels on
top: `hpc.interlink.eu/tenant: "true"` and one `hpc.interlink.eu/<site>-<account>: "true"` per
project the namespace belongs to. It records the accounts it granted in the namespace annotation
`hpc.interlink.eu/<site>-accounts` and removes only labels it set. A namespace that does not exist
is created.

### Executor health

Each tenant namespace gets the site's executor objects from the `membership.executorTemplate` Go
template (namespaced objects only).

The controller never reads a Secret: knowing that a Secret exists needs `get` on Secrets, which
also reads its content. It watches the executor Deployments it created instead: when one is not
available because a Secret it references is missing, it records that on the namespace, with an
Event and the annotation `hpc.interlink.eu/<site>-executor: missing credentials`.
