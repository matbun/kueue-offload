# kueue-offload design

This file describes how kueue-offload works today and fixes the contracts its components are
built against. [kueue-native.md](kueue-native.md) describes the shape it should take to follow
Kueue and MultiKueue, and [backends.md](backends.md) the batch systems it runs on. The examples use
a site `hpc-a` and a project `proj42`.

## Components

| Component | Code | Image |
| --- | --- | --- |
| offload controller (Kueue admission check) | this repo, `cmd/offload-controller`, `pkg/offload` | `kueue-offload` (`/offload-controller`) |
| virtual node of each site, inside the offload controller | this repo, `pkg/node` | same image (`/offload-controller`) |
| membership controller | this repo, `cmd/membership`, `pkg/membership` | same image (`/membership`) |
| CRDs `OffloadConfig`, `OffloadWorker`, `OffloadRequest`, `IdentityMap`, `ProjectQuota` | this repo, `api/v1alpha1`, `config/crd` | |
| names the executor shares with the controller and the API server | this repo, `pkg/executorapi` | |
| executor, in each tenant namespace | this repo, `cmd/executor`, `pkg/executor`, `pkg/shadow` | same image (`/executor`) |
| interLink API server, next to the executor | interLink `cmd/interlink`, with `PodStatus.SocketDir` | the stock API server image |
| plugin, next to the API server | an interLink plugin; for Slurm, `interlink-slurm-plugin` | the plugin image |

API group of the CRDs: `interlink.eu`, version `v1alpha1`. One offload controller serves every site;
one membership controller runs per site, with `--site=<OffloadConfig name>`. The offload controller
reads no Secret and runs one virtual node per site, on which the Workloads' pods show their jobs
([Virtual node](#virtual-node)); the membership controller reads Secrets' metadata only, never their
data ([Executor health](#executor-health)). The executor speaks only the interLink API, the one a
virtual kubelet speaks, so the plugin behind the API server is any interLink plugin
([backends.md](backends.md)).

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

### The check goes Ready when the job starts

While the job waits the check stays `Pending`: Kueue keeps the quota reserved (`QuotaReserved=True`)
and the owner suspended (a plain pod gated), and the check `message` carries the batch system's
state (`job 123 queued (Priority)`). Once the job started at the site (the request's
`status.startTime`, `Admitted=True` or `Finished=True`), the controller sets the check `Ready`
with `podSetUpdates`, Kueue admits the Workload, and the owner unsuspends (a plain pod is ungated)
and makes its pods as on any cluster. For each PodSet sent to the site the updates carry:

- label `offload.interlink.eu/node: <node>` and annotation `offload.interlink.eu/workload:
  <Workload>`;
- node selector `offload.interlink.eu/node: <node>`: the default scheduler cannot place the pod on
  a real node, so a pod that kept the default `schedulerName` stays unschedulable until the
  controller binds it;
- tolerations of `node.kubernetes.io/unreachable` and `node.kubernetes.io/not-ready`, `NoExecute`,
  with no `tolerationSeconds`: a restart of the controller does not evict the pods. A plain pod got
  the API server's 300 s ones of the same keys at create, and the shortest counts; the
  deployment's admission rules drop those limits at create (below), and the binder warns (event
  `TimedNodeTolerations` on the pod) when it binds a pod that still has one.

Kueue holds `podSetUpdates` immutable while the check stays `Ready` (its Workload webhook), so every
`Ready` write carries the same ones, built from the Workload alone (`podSetUpdates` in
`pkg/offload/admit.go`). `<node>` is `kueue-offload-<OffloadConfig name>`, the site's virtual
node: the controller binds the pods to it, the node shows on each the state of its member, and
owners end from their pods ([Owners](#owners), [Virtual node](#virtual-node)).

Admitting at submit instead would make every retry and start-estimate deferral before the start
evict an admitted Workload, and Kueue deletes the pods of an evicted plain-pod Workload (`Stop` of
its pod integration): a notebook or a pipeline step would be killed instead of requeued. Admitting
at the start keeps every path before it as it is, for every kind; the cost is that the pods appear
only when the job starts. Kueue counts a Workload's `maximumExecutionTimeSeconds` from admission,
so from the job's start, and deactivates it once that time passed; the site's wall time comes from
the same value ([Offload controller behaviour](#offload-controller-behaviour)).

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

### Start estimate

On a plugin that lists the capability `estimate`, the executor asks the API server's `/estimate`,
before it creates anything of an attempt, when the site would start a job of the Workload's shape
([backends.md](backends.md) 3.1). It records the answer in the request's `status.estimate`
(`probeTime`, `nodes`, and `startTime`, or `reason` and `message` when the site gave none) and
submits nothing on that pass; on the next pass, a `pollInterval` later, it submits if the check is
still `Pending` (or `Ready`, for an attempt retried in place). A 502 whose text names a submit limit
(`QOSMaxSubmitJob...`, `AssocMaxSubmitJob...`, `MaxSubmitJobLimit`) is recorded without `startTime`,
with the limit's name as `reason`. Any other failure, unreadable capabilities included, is logged
and the job goes without an estimate. The executor stops asking once the attempt's stage-in started
or its job was submitted.

While no job is submitted, the controller shows the estimate in the check message: `Slurm expects
to start N node(s) at <UTC>`. With the OffloadConfig's `maxStartDelay` set, when `startTime -
probeTime` is above `maxStartDelay` or there is no `startTime`, it deletes the attempt's request and
deactivates the Workload (`spec.active: false`) for `deferInterval` (default 30s), recording when
in the annotation `offload.interlink.eu/deferred-until`. Every deferral waits the same
`deferInterval`, never the `retryStrategy` backoff: it waits for the site and is not a failure, so
a job queued behind another starts soon after the site frees a slot. Kueue evicts a
deactivated Workload at once, which releases the quota, so that a job the site would keep waiting
does not hold the project's quota meanwhile. The check stays `Pending` with `..., in <s> s: Workload
deactivated until <UTC>, quota released` (event `Deferred`). When that time comes the controller
sets `spec.active` back and drops the annotation (event `Reactivated`), and Kueue queues the
Workload again; a Workload deactivated without the annotation stays inactive. A `Retry` with
`requeueAfterSeconds` would not do: Kueue 0.16.9 keeps the reservation until
`status.requeueState.requeueAt` and evicts only then. These deferrals never reject the
Workload: the next attempt's `spec.deferrals` is the previous one's plus one, and [Retries](#retries)
count `attempt - 1 - deferrals` against `backoffLimitCount`. A submitted job, or an attempt whose
stage-in started, is never deferred. Without `maxStartDelay` the estimate only shows in the
message.

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
3. Kueue keeps the reservation until the delay has passed (`status.requeueState.requeueAt`), then
   evicts the Workload, which releases the quota, and queues it again at once.
4. At the next reservation the controller creates the next attempt's OffloadRequest.

The retries spent are the attempt's number less one less the attempts the start estimate deferred,
read from the request (`spec.attempt`, `spec.deferrals`; [Start estimate](#start-estimate)), not
from Kueue's `retryCount`: Kueue drops the state of a check that covers only some flavors, its
`retryCount` included, when it evicts the Workload. The deleted request keeps the controller's
finalizer `offload.interlink.eu/attempt`, and with it the attempt number, until the next attempt
exists or the Workload can have none (it went, finished or was deactivated, other than by a
deferral). The next attempt is one past the newest request of the check, deleted ones included,
and past Kueue's `retryCount` while the check state holds one; every new reservation after an
eviction is an attempt, whatever evicted the Workload.

After the start the Workload is admitted. An owner with a controller of its own (Job, JobSet,
TrainJob, RayJob, RayCluster) is retried the same way: Kueue evicts the admitted Workload, suspends
the owner, whose pods go, and queues it again. Kueue would delete an evicted plain pod instead, so
a plain pod, a pod group or a notebook's pod is retried in place: the check stays `Ready` and the
quota held, the controller deletes the request (the executor cancels the job), computes the delay
as `UpdateAdmissionCheckRequeueState` does for that retry, records when in the Workload annotation
`offload.interlink.eu/next-attempt-at`, and creates the next attempt once that time passed. The
pods stay, `Pending` meanwhile. A start estimate defers such an attempt by `deferInterval` the same
way, without deactivating the Workload.

Once `backoffLimitCount` retries were spent the owner fails ([Owners](#owners)); 0 fails it at the
first loss. The three limits are OffloadConfig's `retryStrategy`, which has
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
- a `ray.io/v1` RayJob or RayCluster (Kueue's `ray.io/rayjob` and `ray.io/raycluster`
  integrations);
- plain pods of Kueue's `pod` integration: one pod with `kueue.x-k8s.io/queue-name` (the Workload's
  controller), or a pod group with `kueue.x-k8s.io/pod-group-name` and
  `kueue.x-k8s.io/pod-group-total-count` (every member owns the Workload, none as its controller).
  A homogeneous group is one PodSet with `count: N`, which Kueue names by a hash of the pod shape.

Any other owner kind is `Rejected` with a message saying so.

Owners end from their pods, as on any cluster: a Job by its succeeded or failed pods, a JobSet by
its Jobs, a TrainJob by its JobSet, a plain pod or a pod group by their phases; a notebook's
StatefulSet counts its pod ready as any. The node shows each pod the state of the member it stands
for ([Virtual node](#virtual-node)); the controller writes no owner status.

A RayJob or RayCluster runs as one batch job through a site launcher (`interlink.eu/launcher`;
default `ray`, `RayClusterLauncher` in `pkg/offload/ray.go`), which builds the Ray cluster from the
allocation, head on the first node, and blocks on the head; a template's own launcher wins. Both
heads declare the dashboard (8265) and client (10001) ports, so the head's shadow forwards them and
KubeRay's `<cluster>-head-svc` selects the head pod, which carries the shadow's IP. Once Kueue
admitted the Workload, KubeRay drives it as on any cluster: it makes the head and worker pods,
submits a RayJob's entrypoint (`HTTPMode` from the operator, `K8sJobMode` from its submitter Job,
which runs in the cluster with the default scheduler and is not sent to the site), follows the
Ray job through the dashboard and ends the RayJob `Complete`. Kueue then finishes the Workload, and
the controller deletes the live request, which cancels the allocation. When the batch job ends
first (its time limit, a crash), the controller deactivates the Workload (`spec.active: false`):
Kueue suspends the owner, which releases the quota, and KubeRay deletes its pods; setting
`spec.active` again starts a new cluster. A RayCluster has no terminal state that Kueue reads, so
this is also how its quota is released. A Ray owner of its head alone (no worker group, or groups
of 0 replicas, which are not sent) runs on one node, as a pod group of one
([Workload shapes](#workload-shapes)). A RayJob's head forwards ports, so a site without
`Shadow.LoginHost` refuses RayJobs.

A refused Workload (the executor's or the batch system's refusal, a shape, account, flavor or time
limit the controller refuses, or `backoffLimitCount` retries spent) fails its owner as a kubelet
refusing its pods would: the controller writes the message into the Workload annotation
`offload.interlink.eu/failed` (at most 32 KiB) and sets the check `Ready`; Kueue admits, the owner
makes or ungates its pods, and the node reports each `Failed` with reason `Rejected` and that
message. A Job then fails by its `backoffLimit`, a TrainJob through its JobSet, a pipeline step or
a Katib trial as any failed pod. An owner of an unsupported kind and a Ray owner get the check
`Rejected` instead, and Kueue deactivates the Workload: KubeRay would make failed head pods again
for ever. Event `Rejected` either way.

For a pod group the node sets `kueue.x-k8s.io/retriable-in-group: "false"` on each member before
it reports it `Failed`, and before it deletes a member whose job has not finished: without it Kueue
waits for a replacement pod no allocation can take. The key and value come from Kueue's
`pkg/controller/jobs/pod/constants`.

The controller needs `get` on `jobs`, `jobsets`, `trainjobs`, `rayjobs` and `rayclusters`,
`patch` on `workloads` (its annotations, a deactivation), and for its node the rules of
[Virtual node](#virtual-node).

### Owner annotations

A Workload's PodSet template holds only the pod template, while users put batch flags on the owner
(`slurm-job.vk.io/flags` on a Job's metadata). The controller copies the owner's annotations with
prefix `slurm-job.vk.io/`, `htcondor-job.vk.io/`, `interlink.eu/` or `offload.interlink.eu/` into
the request's `spec.ownerAnnotations`; the executor applies them to every PodSet template, whose
own value wins. The templates in the request are the Workload's as they are, so the executor can
compare them with the Workload. The owner is the Job, JobSet, TrainJob, RayJob or RayCluster, or for pods the first
owner pod that still exists.

## Offload controller behaviour

Per Workload, for each of its checks this controller serves:

| Situation | Action |
| --- | --- |
| `Finished=True` | delete a live request whose job did not end (a RayJob that KubeRay completed, a pod group whose pods a user deleted), which cancels the job; an ended request stays until the Workload goes, and with it the job directory, so the logs stay readable |
| no `QuotaReserved=True`, evicted or deactivated | delete the live requests ([Workload deletion](#workload-deletion)); a Workload a deferral deactivated is reactivated when its `offload.interlink.eu/deferred-until` time comes ([Start estimate](#start-estimate)) |
| check `Retry` or `Rejected` | nothing (`Retry`: Kueue evicts next; `Rejected`: Kueue deactivates) |
| check `Pending`, no OffloadConfig | nothing, requeue after a minute (the check is inactive) |
| reserved, owner of an unsupported kind | check `Rejected` |
| reserved, no live request | create the next attempt's OffloadRequest ([The OffloadRequest](#the-offloadrequest)); with the check `Ready`, an attempt retried in place, once its `offload.interlink.eu/next-attempt-at` passed ([Retries](#retries)). Refusals found while building it (account label, flavors, time limit) fail the owner ([Owners](#owners)) |
| a live request | map its status, newest first in this order: `Accepted=False`: the owner fails with the condition's message ([Owners](#owners)); the job started or `Finished=True`: check `Ready` with `podSetUpdates` and the state in the message (`job N running on <node> since <start>`, `job N completed`, `job N failed: <message>`), and a Ray owner whose job ended is deactivated (its check stays `Pending` if it was, so that KubeRay makes no pods the deactivation would remove); `Evicted=True`: [Retries](#retries) (`job N cancelled: <message>`); no heartbeat for `executorLostTimeout`: [Retries](#retries); `QuotaReserved=True` for `maxQueueTime` without `Admitted`: [Retries](#retries); no job submitted and a `status.estimate` beyond `maxStartDelay`: Workload deactivated for `deferInterval`, check `Pending` ([Start estimate](#start-estimate)); otherwise `Pending` with the state in the message |

The check messages: `StagingOut`: `job N ended, staging out: <message>`; `Admitted`: `job N
running on <node> since <start>`; `QuotaReserved`: `job N queued (<reason>): <message>`;
`StagingIn`: `staging in: <message>`; a reason or message and no condition: `waiting to submit
(<reason>): <message>`; a `status.estimate` and no job yet: `Slurm expects to start N node(s) at
<UTC>`; `Accepted`: `accepted by the executor in namespace X`; nothing yet:
`waiting for the executor in namespace X`. With no heartbeat for twice `pollInterval` the message is
`no executor answering in namespace X since <time>` instead. A live request is looked at again every
`pollInterval`, and on every change of it.

The account is the ClusterQueue's label `hpc.interlink.eu/account`, which the membership controller
sets on the ClusterQueues it makes; without it the owner fails ([Owners](#owners)). Each PodSet's
annotations come from the OffloadConfig's `flavors` entry of its admitted flavors. Each value is a
Go text/template, executed with `.Account`, `.MaxExecSeconds` (the job's wall time: the Workload's
`spec.maximumExecutionTimeSeconds`, else the longest `interlink.eu/time-limit` (`HH:MM:SS`) of its
PodSets, a template's value winning over the owner's and an empty one counting as none; 0 when
neither) and `.MaxExecMinutes` (the same, rounded up, and one minute more when it is Kueue's
`maximumExecutionTimeSeconds` and the flavor's `maxExecutionTimeSeconds` leaves room). Kueue
deactivates the Workload at its limit, counted from admission, which is the job's start as the
executor sees it (the plugin's status cache and the executor's poll take up to 25 s by default), so
a job past Kueue's limit is deactivated and its owner suspended, as on any Kueue cluster, and the
site's limit is the backstop. At the flavor's limit, or when the start is seen more than a minute
late (an executor down when the job starts), whichever comes first ends the job. A job past
`interlink.eu/time-limit` alone ends `Failed` by the site's limit, as on the virtual kubelet: an
owner whose controller waits for a final state (a Katib trial, a TrainJob) sets that annotation,
not Kueue's label. The `/estimate` probe takes the flavor's `--time` when its flags carry one, the
one the job gets. The request's
`spec.maximumExecutionTimeSeconds` carries that wall time:

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
template that does not execute are refused, and the owner fails.

A flavor's `maxExecutionTimeSeconds` is the longest wall time of the batch queue it stands for. A
Workload asking for more is refused (`a job runs at most 02:00:00 on queue <flavor>;
interlink.eu/time-limit asks for 02:01:00`), and so is an `interlink.eu/time-limit` that is not
`HH:MM:SS` or asks for no time: Kueue cannot choose a flavor by time, and the queue would hold or
refuse the job.

Check state writes are server-side apply patches of `status.admissionChecks` (a list keyed by
`name`) on `workloads/status` with field manager `kueue-offload`. They carry `name`, `state`,
`message`, `lastTransitionTime` and, on `Retry`, `requeueAfterSeconds`, never `retryCount`. A write
happens only when the state or the message changed, and `lastTransitionTime` moves only with the
state; a `Ready` write also carries the `podSetUpdates` ([The check goes Ready when the job
starts](#the-check-goes-ready-when-the-job-starts)). Events on the Workload: `Submitted` for each
attempt, `Running` when a job runs or moves node, `Completed`, `Failed`, `Retry`, `Deferred` and
`Reactivated` ([Start estimate](#start-estimate)), `Rejected`, and `ExecutorUnreachable` when the
heartbeat stops.

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
  maximumExecutionTimeSeconds: 14400   # the job's wall time (Offload controller behaviour)
  deferrals: 0                 # earlier attempts the start estimate deferred
  priority: 100
```

- `owner` is the Job, JobSet or TrainJob, or for pods `v1` `Pod` named after the pod group
  (`kueue.x-k8s.io/pod-group-name`; the pod's name for a single pod), with the first owner pod's
  UID.
- `ownerAnnotations` ([Owner annotations](#owner-annotations)) apply to every template; the
  template's own value wins.
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
| condition `Accepted` | `True` once taken; `False` for a permanent refusal, with the refusal as message: an account not in `AllowedAccounts`, a site it does not serve, a member outside the site's `Resources` or a Slurm option outside its `AllowedFlags` ([Configuration](#configuration)), a request its Workload does not match (below), PodSets of one allocation with different annotations (or, with a launcher, different containers), several pods in one allocation, or one that names a site launcher, on a plugin that does not list `pod-group`, exposed ports or reverse forwards on a site with no `Shadow.LoginHost`, more than 9999 members, a stage path outside the staging volumes or on a volume whose claim the namespace lacks, a failed stage-in, a reference it cannot read, a token it may not request, the plugin refusing the job with 400, 403 or 422 |
| condition `StagingIn` | `True` while the stage-in Job runs |
| condition `QuotaReserved` | `True` once the job is submitted and waits in the site's queue; its `lastTransitionTime` starts `maxQueueTime` |
| condition `Admitted` | `True` while a container of a rank runs, or the head has a node; `False` while the job waits in the site's queue, also while ranks of independent pods wait for the site after the others ended |
| condition `StagingOut` | `True` while the stage-out Job of an ended job runs |
| condition `Finished` | `True` once every rank ended and staging out is done: reason `Succeeded` (every rank exited 0) or `Failed`, with the message saying why (the failed rank, `SlurmJobTimeout`, `OOMKilled`, the site refusing the job, a failed stage-out) |
| condition `Evicted` | `True` when the job was lost: a container terminated with reason `Cancelled` (cancelled outside the executor), or a rank missing from the plugin's status for `Executor.LostTimeout` (reason `Vanished`); `False` with reason `RankMissing` while a rank is missing, since its `lastTransitionTime` |
| `reason`, `message` | what it is doing or waiting for: the first waiting container's reason and message of a queued job; for a submit refused for now (staging in, the plugin's 503, a cluster read that failed) why |
| `jobID`, `nodeName`, `nodes`, `socketDir`, `startTime`, `completionTime` | as the plugin's `/status` reports them; for independent pods `jobID` lists every rank's job, comma-separated, or the first and the last past eight; `nodeName` is the head rank's node, the one the shadow tunnels to |
| `ranks[]` | `rank`, `podUID` (empty for a rank of independent pods that waits for the site), `jobID` (for independent pods the one `/create` named), `nodeName` (the member's compute node), `state` (`queued`, `running`, `completed`, `failed`), `exitCode`, `containers` and `initContainers` (`corev1.ContainerStatus` as the plugin reports them); what the rank's pod on the virtual node shows ([Virtual node](#virtual-node)) |
| `shadow` | while the job tunnels: `deployment`, `service` of the shadow; `podIP` and `ready` (its `Ready` condition) of its pod, a ready one if there is one, pods being deleted left out; rank 0's pod on the virtual node takes that IP |
| `estimate` | the site's start estimate asked before the attempt was submitted: `probeTime`, `nodes`, `startTime`, or `reason` and `message` when the site gave none ([Start estimate](#start-estimate)) |

When a request is deleted, the executor cancels its job unless it ended (`/delete` for every
member the plugin has, which also removes its files), deletes the shadow objects and the stage-in
Jobs, copies back the outputs of a job that started and has stage-out paths not copied yet
([Staging](#staging)), and removes its finalizer; a failure keeps the finalizer and is tried
again. Before it
submits, also before a stage-in, it checks the request against the Workload it names, read from
the API server (`get` on workloads): the request's controller owner is that Workload by name and
UID, the Workload has the check, and its PodSets (name, template) and their admission (count,
flavors) are the request's; otherwise it refuses the request. The PodSets it compares are those
the controller prepares (`offload.RequestPodSets`): for a Ray owner without a RayJob's submitter,
with the launcher and the head's ports. A Workload without its reservation (`QuotaReserved` not
`True`, evicted, inactive or finished) or with the check `Retry` or `Rejected` makes the request
wait, since the controller deletes it. A request also waits while an earlier attempt
of its Workload still carries the executor's finalizer: both attempts' members, shadow and stage
Jobs carry the Workload UID, and both stage to the same directory at the site. The wait covers the
earlier attempt's copy back, at most `Staging.StageOutTimeout`, during which the Workload keeps its
quota and the request says `waiting for OffloadRequest X to copy back the outputs of its job (Job
stage-out-...)`.

### Who may write it

The controller needs `create`, `get`, `list`, `watch`, `patch` (its finalizer) and `delete` on
`offloadrequests`; the executor `get`, `list`, `watch` and `patch` (its finalizer) on
`offloadrequests` and `patch` on `offloadrequests/status`, in its namespace only. Tenants get no
write on the kind. The membership controller `list`s them and `delete`s those of finished
Workloads when it offboards a namespace ([Membership controller](#membership-controller)).
Because a request is what makes an executor submit as its namespace's user, the
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

Submitting a job, reading its state and cancelling it go through the OffloadRequest. What remains
over HTTP is the job's log, which the offload controller's virtual node reads for `kubectl logs`
([Virtual node](#virtual-node)), at the OffloadConfig's `executorService` and `executorPort` in
the request's namespace: plain text answers, no wire types. `pkg/executorapi` holds the names the
executor shares with the controller and the API server (`GetLogsPath` and the query parameters).

### GET /getLogs

Query: `workloadUID` (required), `container` (default: each member's first container), `rank`
(optional), `follow` (`true`/`false`), `tail` (lines), `sinceSeconds`, `sinceTime` (RFC 3339),
`limitBytes`, `timestamps`, `previous`. Every member is read through the API server's `/getLogs`,
as the plugin returns it (for Slurm, the job's `job.out` and the container's output).

- With `rank`: that rank's log, which is what the node asks for the rank's pod.
- Without `rank` and one member: rank 0, no prefix.
- Without `rank` and several members: every member that has the container, each line prefixed
  `[rank N] `. Without `follow`: rank 0's lines, then rank 1's, and so on, `tail` over the whole.
  With `follow`: the lines of every rank's followed stream as they arrive, `tail` per rank.

400 for what a kubelet would refuse too: `previous=true` (an offloaded container has no previous
instance), `tail` with `limitBytes` and `sinceSeconds` with `sinceTime` (the interLink API server
refuses both pairs), a `sinceTime` that does not parse, and, without `follow`, a rank whose job
has not started or that waits for the site (`container <c> of rank <n> is waiting to start`).

The logs are those of the Workload's newest submitted request that still carries the executor's
finalizer; with none, 404. A followed log starts once the job ran (the request's `startTime`,
`Admitted=True` or an end), so the plugin is not asked to follow a log that does not exist yet. It
ends when every rank's stream ended and the plugin's `/status` shows those containers terminated.
A rank's stream that ends before that (a plugin without Follow, or the API server ending a stream
it lost from the plugin as if it were complete) is opened again after 5 s with the lines already
sent skipped, so each line reaches the caller once; a last line without a newline is held back
until the container ended. With `tail`, `sinceSeconds` or `limitBytes` the lines of a new stream
cannot be matched to the old one, so such a stream is not opened again: the executor aborts the
answer, and `kubectl logs -f` ends.

No authentication: the sample's NetworkPolicy lets only the offload controller's pods (namespace
`kueue-offload`, label `app: kueue-offload`) reach the port.

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
`config/rbac/executor/role.yaml` for Secrets, ConfigMaps, tokens, shadows, stage Jobs, Events on
the owner and the staging volumes' claims.

What it keeps about a request lives in the request's status, so a restarted executor follows
every job from its request alone: the job ID, the ranks (each member's `podUID`, and for
independent pods the job `/create` named; a rank without a `podUID` waits for the site), the
shadow's names, and since when a rank is missing (`Evicted=False`, `RankMissing`). Only one fact
is in memory: that an ended job's shadow was deleted. A restarted executor deletes it once more,
which changes nothing.

### Reconciling a request

`pkg/executor/request.go`, for each request of its namespace and site:

1. A deleted request: cancel the job, delete the shadow and the stage-in Job, copy back a started
   job's outputs, remove the finalizer ([Status](#status), [Staging](#staging)).
2. Add the finalizer `offload.interlink.eu/executor` if missing.
3. A refused request (`Accepted=False`) or one whose job finished or was lost: nothing more,
   except deleting an ended job's shadow once.
4. No job yet: read the request again from the API server, check its shape, its sizing and
   flags, its Workload and the earlier attempts (refuse, or wait with the reason in the status),
   set `Accepted=True`, ask the start estimate once ([Start estimate](#start-estimate)), then on a
   later pass submit, staging in first.
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
Listen: ":8000"            # /getLogs (the offload controller) and /healthz; the default
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
    Resources:               # unset: a member goes as its template asks
      GPUResource: nvidia.com/gpu
      GPUAliases: [hpc-a.example.org/gpu]
      CPUsPerGPU: 16
      GPUsPerMember: 4
    AllowedFlags:            # unset: anything
      Flags: [--constraint]
      Flavors: [default]
Shadow:
  Enabled: true            # shadows for jobs that expose ports or tunnel back
  Image: ghcr.io/interlink-hq/interlink/ssh-tunnel:0.6.3-pre2
  LoginHost: login.hpc-a.example.org   # empty: no shadows; ports and reverse forwards refused
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
  DeleteGrace: 30s         # the default; 0 allowed
  StageOutTimeout: 4h      # the default
```

`Sites.<site>.Resources` sizes the members at a site that hands out whole GPUs, each with a fixed
share of a node's cores. A member asks for 1 to `GPUsPerMember` of `GPUResource` or its aliases,
summed over its containers (limit, else request); 0, or more than that, is refused. A template that sets no CPU
gets `CPUsPerGPU` times its GPUs as its first container's CPU limit; one that sets another CPU (the
largest any container asks for) is refused. Aliases are other names of `GPUResource`, such as the
one an admission policy renames it to so that a namespace's ResourceQuota does not count GPUs of
pods that never run in the cluster; they are renamed back before `/create`, and Kueue charged the
Workload by the template as it is. `Sites.<site>.AllowedFlags` is all a workload may ask of the
batch system itself: options by name in `slurm-job.vk.io/flags` from `Flags`, plugin flavors in
`slurm-job.vk.io/flavor` from `Flavors`, checked on the owner's and the templates' annotations and
never on the flavor's PodSet annotations, which are the site's. Refusals are permanent
(`Accepted=False`).

`Staging.DeleteGrace` is how long after a started job's request is deleted the copy back starts, so
that the cancelled job stops writing first; `Staging.StageOutTimeout` is the stage-out Job's
`activeDeadlineSeconds`, and the longest a deleted request waits for it.
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
| one pod that names no site launcher: a single pod, a Job with parallelism 1 | one job | one job |
| one pod that names a site launcher (`interlink.eu/launcher`, on its template, the owner or the flavor): a 1-node TrainJob, a Ray owner of its head alone | one allocation of one node, a pod group of one, which the plugin starts through the launcher | refused (`Accepted=False`) naming the launcher and `pod-group` |
| several pods: a Job with parallelism N, a pod group, a JobSet, a TrainJob | one allocation, ranks numbered across the PodSets | refused (`Accepted=False`) naming `pod-group` and `offload.interlink.eu/independent-pods` |
| several pods, `offload.interlink.eu/independent-pods: "true"` on every template | one job per pod | one job per pod |

The owner's annotation reaches every template ([Owner annotations](#owner-annotations)). Counts come
from the admitted PodSetAssignments. A refusal prevents a gang whose ranks start hours apart from
holding allocations while they wait for each other; the project can pin a flavor on another site,
or mark the owner independent.

### How the executor builds the members

The executor expands each PodSet into `count` member pods that exist only in its memory and sends
each to `/create`, in rank order. Each template first gets the request's `ownerAnnotations` it
does not set itself, and then loses the init containers that its
`interlink.eu/disable-offload-init-containers` names (comma separated, as the virtual kubelet reads
it) and the volumes that only they mount. The virtual kubelet runs them on the cluster, in the
shadow; here they run nowhere (the pod on the virtual node shows them terminated). A site sets it
by admission policy for an
init container that cluster policies inject into every pod and that breaks the site's container
hooks. Members are numbered across the PodSets in request order,
and `<count>` below is the total of every PodSet's count:

- member `i` (0-based): `metadata.name` `<owner name>-<i>`, `uid` a name-based UUID of the
  Workload UID and `i` (the API server takes only UUIDs as pod UIDs), namespace the Workload's,
  labels and annotations from its own PodSet's template, plus label
  `batch.kubernetes.io/job-completion-index: "<i>"` and annotation
  `interlink.eu/podset: <PodSet name>`;
- a Workload of one plain pod is that pod: the member takes the pod's name, and its downward API
  reports the pod's UID (annotation `interlink.eu/downward-uid`), because a workflow engine's
  executor (Argo's) addresses its own pod by name and UID;
- a pod group (`count > 1`, or one member that names a site launcher, and not independent pods):
  also Kueue's pod-group label
  `kueue.x-k8s.io/pod-group-name: <workloadUID>` and annotation
  `kueue.x-k8s.io/pod-group-total-count: "<count>"`, and the Slurm plugin's `interlink.eu/gang-rank`
  and `interlink.eu/gang-role` (member 0 of the first PodSet is the head), so the plugin's gang path
  buffers the members and submits one `sbatch --nodes=<count>`. When the head names no launcher
  (`interlink.eu/launcher`), the members also carry `interlink.eu/gang-end: all`: each runs its own
  command to its end, as a JobSet's replicas do, instead of ending with the head. It answers the members before the
  last with no job ID; the job ID is the last non-empty one. A plugin that lists `pod-group-create`
  gets the members in one `/createPodGroup`, in rank order, and answers every one with the
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
- with the site's `Resources` ([Configuration](#configuration)), a template with no CPU gets
  `CPUsPerGPU` times its GPUs as its first container's CPU limit, and the GPU aliases are renamed
  back to `GPUResource`;
- env `INTERLINK_RANK=<i>` and `INTERLINK_WORLD_SIZE=<count>` added to every container;
- the PodSet's `annotations` appended to the template's own, after a space, so the site's flags
  come last;
- what the API server and the kubelet would have done for a real pod: every container gets the
  ServiceAccount directory (namespace and `ca.crt`, the token only with
  `AutomountServiceAccountToken`) unless the template opts out or mounts it. A plain pod already
  carries the API server's `kube-api-access-*` volume: its token is dropped (namespace and `ca.crt`
  stay) unless `AutomountServiceAccountToken` is set or the owner asks for it with
  `offload.interlink.eu/service-account-token: "true"` (a pipeline step, whose launcher calls the
  API server), so no cluster token is written at the site by default. A deployment may set the
  annotation by admission policy for a class of pods, together with their reverse forwards. The
  remaining
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
the ones a pod group buffers; a `/createPodGroup` that fails, or answers other than one job for
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
`Staging.Volumes` mount path, and the namespace must have the claim of that volume (`get` on
`persistentvolumeclaims`; a Forbidden read skips the check), else the request is refused.

- A request with stage-in paths creates `stage-in-<first 8 of the UID>-<attempt>` once, keeps
  `StagingIn=True` with message `Job <name> is running` while it runs, is refused with the Job's
  failure reason if it failed, and submits as usual once it succeeded (`StagingIn=False`, reason
  `Succeeded`).
- When the job completed or failed and has stage-out paths, the executor creates
  `stage-out-<8>-<attempt>`; the request has `StagingOut=True` with message `Job <name> is running`
  and no `completionTime` until it ends, then `StagingOut=False` and `Finished`. A failed stage-out
  makes the job `Failed` (reason `StageOutFailed` for a job that had completed), the message says so.
- A deleted request whose job started and has stage-out paths not copied yet (its owner stopped or
  deleted, or the attempt lost and retried) keeps the executor's finalizer through the cancel
  (members, shadow, stage-in Job), `Staging.DeleteGrace` when the job had not ended, then the
  stage-out Job until it ends. A permanent error creating that Job (Forbidden, such as a namespace
  being deleted) lets the request go with a `StageOutFailed` event. A job that never started copies
  nothing.
- Deleting the request deletes the stage-in Job (propagation `Background`), also for a request
  whose stage-in never let it be submitted. A stage-out Job is not deleted: it has
  `ttlSecondsAfterFinished` 86400 and `activeDeadlineSeconds` `Staging.StageOutTimeout`, so its log
  stays readable for a day, or until the namespace is offboarded from the site.

Events on the owner: `StageOut` (`copying <paths>`), then `StagedOut` (`copied back <paths>`) or
`StageOutFailed` (`copying back <paths> failed (<why>), see the logs of Job <name>`), after a normal
end as after a delete.

A stage Job has labels `interlink.eu/stage-of: <workloadUID>`, `interlink.eu/stage-direction: in|out`,
`offload.interlink.eu/site: <the executor's site>` (offboarding deletes the site's stage Jobs) and
`sidecar.istio.io/inject: "false"`, `backoffLimit: 0`, `restartPolicy: Never`, requests
50m/64Mi, limits 2 CPU/2Gi, the site key at `/ssh` and the known hosts at
`/etc/ssh/ssh_known_hosts`, and mounts only the volumes its paths are on. The command's contract is
its environment: `STAGE_DIRECTION` (`in`/`out`), `STAGE_PATHS`, `REMOTE`
(`<Staging.User>@<TransferHost>`), `STAGE_ROOT`, `STAGE_NAMESPACE`, `SSH_KEY`, and for a path on a
`Kerberos` volume `KEYTAB`, `PRINCIPAL` and `KRB5CCNAME=FILE:/tmp/krb5cc` (keytab at `/keytab`,
`krb5.conf` from `Krb5ConfigMap`). The executor needs `get`, `list`, `create` and `delete` on
`batch/jobs`, `create` on `events` and `get` on `persistentvolumeclaims` in its namespace.

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
renders for a job whose rank 0 template exposes a TCP port (container ports or
`interlink.eu/wstunnel-extra-ports`) or asks for a reverse tunnel, and for no other job: an SSH
tunnel to the job's exposed ports (`ssh-forward`), a Service when the template exposes ports, and a
ConfigMap with the compute node and the socket directory the plugin reports in `/status`. The
executor ensures it on every poll once the job runs on a known node, and deletes it once the job
ended (`status.shadow` goes with it). Each poll records the IP and readiness of the shadow's pod in
the request's `status.shadow`, and the virtual node gives rank 0's pod that IP: the owner's
Services, a RayCluster's head Service, a Notebook's Service and any EndpointSlice select the
user's pod as on any cluster and reach the shadow. The shadow carries only its own labels
(`interlink.eu/shadow`, `interlink.eu/shadow-of: <Workload UID>`) and publishes no EndpointSlice.
Its Deployment uses the `Recreate` strategy, so a reverse tunnel is never held by two pods. A
deployment restricts who reaches shadows with a NetworkPolicy on their label
`interlink.eu/shadow: "true"`. The logs never go through a shadow ([Virtual node](#virtual-node)).

Without `Shadow.LoginHost` no job has a shadow, and neither `Shadow.User` nor `Shadow.KeySecret`
is needed: for a backend whose nodes the cluster cannot reach, or that reports no node. The executor
then refuses a head template that exposes a TCP port or asks for reverse forwards, and
`Shadow.Reverse.Enabled` is refused at start. A deployment that wants such ports ignored there
removes them at admission instead. The executor deletes the shadow when the job's OffloadRequest is
deleted: when the Workload is deleted, loses its reservation, or its attempt is retried.

A head template with `interlink.eu/reverse-forwards` also gets a tunnel from the compute node back
to the listed cluster endpoints (`ssh-reverse`), parsed with the virtual kubelet's rules plus
`Shadow.Reverse.AllowedTargets`; its set-up goes before rank 0's `slurm-job.vk.io/pre-exec`, as the
virtual kubelet puts it. It needs `Shadow.Reverse.Enabled` and a plugin that reports a socket
directory, and a gang of more than one node with a site launcher cannot have one, because only the
head rank has the tunnel; the plugin sets it up for a gang of one node in the batch shell, before
the launcher. Of independent pods only rank 0, the head, has it.

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
    // Reverse is the job's tunnel back into the cluster, nil unless it asked for
    // one (ParseReverseTunnel).
    Reverse *ReverseTunnel
}

type Ref struct {
    Deployment, Service, NodeConfigMap string
    PodIP                              string // of the shadow's pod, a ready one if there is one
    Ready                              bool
}

type Manager interface {
    // Ensure creates or updates the shadow for spec: Deployment, Service (only with exposed
    // ports), node ConfigMap with compute-node and socket-dir. Nothing, and Ref{}, when spec
    // neither forwards ports nor tunnels back; an error when it does without LoginHost.
    // Idempotent; called on every poll once the job runs.
    Ensure(ctx context.Context, spec Spec) (Ref, error)
    // Delete removes every object labelled for this workload. Idempotent.
    Delete(ctx context.Context, workloadUID string) error
}
```

## Notebooks and Services

A Kubeflow Notebook runs as a StatefulSet whose pod carries `kueue.x-k8s.io/queue-name`, so Kueue's
pod integration makes it a Workload of one plain pod, gated until its job started. Its Service
selects `statefulset: <name>` and sends port 80 to the container's port (8888 by default); the
Kubeflow UI shows the Notebook ready, with Connect, once the StatefulSet counts one ready replica.
Here, as on the virtual kubelet:

- Once the job started the pod is ungated and bound to the site's node, which reports it `Running`
  with the shadow's IP ([Virtual node](#virtual-node)). The Service selects the pod as on any
  cluster, so its endpoint is the shadow, which forwards the pod's ports.
- The pod is `Ready` when the shadow is ready, the job is not staging out, and every container
  with a `readinessProbe` in the request's template is ready in the rank's status: the site runs
  the probe in the job, for example a notebook's test of the socket its server listens on. The
  StatefulSet counts it ready, and the notebook controller copies `readyReplicas` to the Notebook.
- `kubectl logs` of the pod, and the UI's log tab, show the job's output; `kubectl exec`, and the
  UI's terminal, answer that exec is not supported.
- A job that stops without ending (requeued at the site, lost, retried in place) shows the pod
  `Pending` until the next attempt runs; at the end the pod takes the rank's terminal state.

Jobs, JobSets, TrainJobs and Ray owners get the same pods, statuses and Services: a Service that
selects rank 0's pod reaches the shadow, one that selects other ranks reaches no endpoint, since
only rank 0's pod carries an IP.

## Virtual node

One node per site, named `kueue-offload-<OffloadConfig name>` (`v1alpha1.NodeName`), run inside the
offload controller by `pkg/node` on the leader: no Deployment, image or credential of its own.
The pods of a Workload admitted through the site's check are bound to it, and it shows on each the
state of the member of the job it stands for, as a kubelet shows a pod it runs, and serves its
logs. It uses the `virtual-kubelet` library (v1.11.0, packages `node` and `node/api`) for the node
status loop and the kubelet API's routes, not its pod controller.

| Item | Value |
| --- | --- |
| owner | the OffloadConfig (garbage collected with it); the node's Lease is owned by the node |
| labels | `kubernetes.io/hostname`, `kubernetes.io/os: virtual-kubelet` (DaemonSets that select an OS skip it), `kubernetes.io/role: agent`, `type: virtual-kubelet`, `node.kubernetes.io/exclude-from-external-load-balancers`, `offload.interlink.eu/node: <name>`, `offload.interlink.eu/site: <worker>` |
| labels from the OffloadConfig | `nodeLabels`, for the cluster's own conventions (the label its DaemonSets keep off virtual nodes by); never replacing the ones above |
| providerID | `external:///<name>`: a cloud controller manager deletes a node that is not ready and has no instance of its provider, and with it the pods bound to it |
| taint | `offload.interlink.eu/node=<name>:NoSchedule`; binding ignores taints |
| capacity | `cpu: 1`, `memory: 1Gi`, `pods: 1000`: nothing is scheduled by fit, and autoscalers that count node cores (CoreDNS's) do not grow with the site |
| addresses, port | `InternalIP` the controller pod's IP (`POD_IP`), kubelet port 10250 |
| Lease | `kube-node-lease/<name>`, 40 s |
| conditions | `Ready`, no pressure, network available |

Binding (`pkg/offload/bind.go`): a pod with the annotation `offload.interlink.eu/workload`, no
node, no scheduling gate and not being deleted, whose Workload is admitted, not finished, and has
the check `Ready`, is numbered (annotation `offload.interlink.eu/rank`, kept once written) and bound
through `pods/binding` to the node its `offload.interlink.eu/node` label names, which must be its
check's. The rank is the offset of the pod's PodSet among those sent to the site plus its index:
JobSet job and completion index, a Job's completion index, a StatefulSet ordinal, a Ray head 0,
else the lowest free index of its PodSet in name order; it is the executor's rank of that member.
A deployment needs three mutating admission rules (Kyverno rules; Kubernetes 1.30 has no
MutatingAdmissionPolicy), each at create, for a pod or Job with the site's queue label or, made after
admission, the node label:

- pods: `schedulerName: kueue-offload` (immutable after create): the default scheduler never takes
  these pods, and the node selector keeps a pod the rule missed away from real nodes;
- pods: no `tolerationSeconds` on the `NoExecute` tolerations of `node.kubernetes.io/not-ready`
  and `unreachable`, which the API server adds before admission webhooks run: without it a node
  not ready for 5 minutes (the controller down) evicts a plain pod and cancels its job. At create,
  since a policy may freeze a pod's tolerations after;
- Jobs: `backoffLimit: 0`: a lost job is retried through Kueue (`retryStrategy`), and with the
  default 6 the node would fail each new pod again from the same finished job, for minutes.

Pod status (`pkg/node/status.go`, from the Workload, its live request and the rank):

| Request | Pod |
| --- | --- |
| none, the rank queued or pending, or the job not started | `Pending`, containers waiting with the request's reason (`Queued`, `StagingIn`, `WaitingForAttempt`) and message |
| started, not finished | `Running` since the rank's start; init containers terminated; `Ready` as in [Notebooks and Services](#notebooks-and-services); rank 0's pod with the shadow's IP |
| `Finished`, `Succeeded` | `Succeeded`, containers terminated with the rank's exit codes |
| `Finished`, `Failed` | each rank's own end, its exit codes (1 when missing); when no rank failed (a failed stage-out), rank 0 `Failed` with the request's message |
| Workload annotation `offload.interlink.eu/failed` | `Failed`, reason `Rejected`, the annotation as message ([Owners](#owners)) |
| a pod with the node's label but no Workload or rank annotation | `Failed`, reason `NotOffloaded` |

A pod already `Succeeded` or `Failed` is never written again. A pod being deleted first ends, as a
kubelet ends one whose containers it stopped: as its member ended, else `Failed` with its containers
at exit code 137; it is then deleted at once (grace 0), and its job goes with its request. Without
the end, a finalizer (Kueue's `kueue.x-k8s.io/managed` on a pod group's pods) would keep the pod
`Running` and the group's quota held. The controller caches only pods with the label
`offload.interlink.eu/node`, so a pod bound here without it (a DaemonSet's that tolerates the
taint) is left as it is.

Logs: `kubectl logs <pod>` reaches the node's kubelet API, which asks the executor of the pod's
namespace (`GET /getLogs` with the Workload's UID, the rank and the container) at the
OffloadConfig's `executorService` and `executorPort`, and streams the answer: `-f`, `--tail`,
`--since`, `--since-time`, `--limit-bytes` and `--timestamps` as on any pod, `--previous` refused
([GET /getLogs](#get-getlogs)). Nothing is stored in the cluster, so no kubelet log file bounds a
large log. Exec, attach and port-forward are not supported: `kubectl exec` answers "exec into an
offloaded container is not supported: the interLink plugin API has no exec call". Supporting it
needs an exec call in interLink's plugin API, an executor route that streams stdin, stdout, stderr
and resizes, and in each plugin a way to run the command in the job's container on the member's
node; the virtual kubelet has none of these either. `kubectl port-forward` to a pod's Service
reaches the shadow.

Kubelet API (`pkg/node/server.go`): one HTTPS listener on `:10250` for every node of the
controller. Clients are authenticated as a kubelet does, with the API server's client CA
(`kube-system/extension-apiserver-authentication`) and TokenReview, and authorized with
SubjectAccessReview on `nodes/proxy`, `nodes/log`, `nodes/stats` and `nodes/metrics`. The serving
certificate comes from a `kubernetes.io/kubelet-serving` CSR (CN `system:node:kueue-offload`, IP
SAN the pod's IP), which the controller approves itself when it is its own ServiceAccount's
(`--approve-kubelet-serving-csr`, default true), kept in memory: a restarted controller makes a
new one. The API server verifies a kubelet's serving certificate (the reference cluster refuses one that
is not the cluster's), so there is no other source.

What the node needs in the cluster:

| Object | How many |
| --- | --- |
| Node, and its Lease in `kube-node-lease` | one per site |
| kubelet-serving CSR | one per start of the controller |
| pods | the Workload's own, made by its owner once Kueue admits and bound to the node; none runs a container in the cluster |
| shadow | one per job that exposes ports or tunnels back; a batch job has no cluster pod but its own |
| mutating admission rules | three: `schedulerName` and untimed node tolerations on pods, `backoffLimit: 0` on Jobs |
| node RBAC by name | patch and delete of the node, patch of its status, granted by the deployment for `kueue-offload-<OffloadConfig>` |

The node has no Deployment, image, CRD or credential of its own: it runs in the offload
controller's process, reads logs from the executor through the Service the executor already has,
and keeps the pod to member mapping in pod annotations.

RBAC of the node (markers in `pkg/node/doc.go`): nodes `get`, `list`, `watch`, `create` and
`nodes/status` `get`; patch and delete of the node and patch of its status, the only writes the
controller and the virtual-kubelet library make to it, are the deployment's to grant by name, one `kueue-offload-<OffloadConfig>` per site, since
`resourceNames` cannot name a node before it exists: the controller cannot repoint a node it does
not run, so the serving certificate it approves for itself is of no use for another node. Leases in
`kube-node-lease`; pods `get`, `list`, `watch`, `patch`, `delete`, `pods/status`, `pods/binding`
`create`; TokenReviews and SubjectAccessReviews; the client CA ConfigMap in `kube-system`;
CertificateSigningRequests, their approval and the `kubernetes.io/kubelet-serving` signer. The
Roles in `kube-node-lease` and `kube-system` are bound in `config/rbac/role_binding.yaml`.

## Membership controller

The membership controller keeps one site's Kueue objects, namespace labels and executors in step
with the site's Slurm associations (a ConfigMap holding
`sacctmgr --parsable2 --noheader show assoc format=cluster,account,user`) and the `IdentityMap`s,
which map a Kubernetes user to a namespace and a site user. It never infers a mapping from names.
It runs a pass when its OffloadConfig, an IdentityMap, a ProjectQuota of the site, a ConfigMap of
its namespace or an executor Deployment changes, and every `membership.syncInterval` (default 15
minutes), every minute while an executor is not available or a namespace is leaving the site.

### Kueue objects per site and project

With Kueue v1beta2 field names (the reference cluster runs Kueue 0.16.9):

- while the site has an account with members, a Cohort named `membership.cohort` (default: the
  site name);
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
that disappears loses its label and its LocalQueue at once ([Offboarding](#offboarding)). The
ClusterQueue of an account goes once no namespace holds it or waits to be offboarded from it (Kueue
keeps a deleted ClusterQueue until the Workloads it admitted end), and the Cohort once the site has
no ClusterQueue left; the controller needs `delete` on `clusterqueues` and `cohorts`.

At the end of each pass the controller writes to the OffloadConfig's `status.members` the number of
member namespaces and to `status.leaving` the number of namespaces that wait to be offboarded, when
they changed (`patch` on `offloadconfigs/status`); `kubectl get offloadconfigs` shows both. With
both at 0 after the last membership went, every namespace is offboarded and the site's
ClusterQueues and Cohort are deleted.

### Offboarding

When a namespace has no membership of the site left (it left the IdentityMap, or its user the
associations), the controller records the accounts it had in the annotation
`hpc.interlink.eu/<site>-left`. From the next pass on, a minute later at most:

1. While an OffloadRequest of the site's workers in the namespace is in use (its Workload runs, is
   gone, or the request is being deleted), it waits.
2. It deletes the OffloadRequests of finished Workloads, one by one with a UID precondition, so
   that the executor cleans up after their jobs, and waits until they are gone.
3. Once, it runs the Jobs of `membership.offboardTemplate` (the executor template's data with the
   accounts the namespace had; Jobs only, labelled `hpc.interlink.eu/offboard`), for example one
   that removes the tenant's directories at the site, and waits until they end; then it marks the
   namespace `hpc.interlink.eu/<site>-offboarded`, so that a later pass does not run them again. A
   failed Job gives a Warning event `OffboardFailed` about the Namespace (so in namespace
   `default`) and does not stop the rest.
4. It deletes, with `deletecollection`, the namespace's objects labelled with its `managed-by` and
   `site` labels, of every kind the executor template renders for those accounts and of Jobs, and
   the stage Jobs of the site's executor (labels `interlink.eu/stage-of` and
   `offload.interlink.eu/site: <worker>`, [Staging](#staging)), which outlive their requests.
   A claim it made that a pod still mounts stays Terminating until that pod ends.
5. It removes its labels (the tenant label only when no other site keeps the namespace, the
   `namespaceLabels` only where it added them) and its annotations, and records an event
   `Offboarded`.

The namespace and its Secrets stay, so adding the namespace back restores everything at the next
sync. A namespace deleted outright is not offboarded: there is no finalizer on namespaces. The
controller needs `list` and `delete` on `offloadrequests`, `get` on `workloads` and
`deletecollection` on the kinds it renders.

### Namespace labels

`managedJobsNamespaceSelector` is cluster-wide Kueue configuration shared with other tenants, so
the deployment keeps the cluster's selector. The membership controller only adds its own labels on
top: `hpc.interlink.eu/tenant: "true"` and one `hpc.interlink.eu/<site>-<account>: "true"` per
project the namespace belongs to. It records the accounts it granted in the namespace annotation
`hpc.interlink.eu/<site>-accounts` and the site user in `hpc.interlink.eu/<site>-user` (kept for
offboarding), and removes only labels it set. The OffloadConfig's `membership.namespaceLabels` are
added to every member namespace too, for example the label the cluster's
`managedJobsNamespaceSelector` selects, so that Kueue manages every tenant's jobs; those the
namespace did not have already are recorded in `hpc.interlink.eu/<site>-labels`, and only those are
removed at offboarding. A namespace that does not exist is created.

### Executor health

Each tenant namespace gets the site's executor objects from the `membership.executorTemplate` Go
template (namespaced objects only). The key it names is the template; as for
`membership.offboardTemplate`, the other keys of that ConfigMap ending in `.tpl.yaml` are templates
of their own name for `{{ template "<key>" . }}`, and `file "<key>"` returns a key's content
unparsed (`{{ file "known_hosts" | toJson }}` is a YAML string), so a kustomize
`configMapGenerator` can build the ConfigMap from plain files with no rendering step.

The controller reads Secrets only as metadata (`PartialObjectMetadata`, RBAC `get` on Secrets, no
`list` or `watch`): the `resourceVersion` of each Secret a rendered pod template mounts or reads
into env goes into the template's annotation `hpc.interlink.eu/secret-versions` (`<name>=<version>`,
comma-separated), so a replaced Secret rolls a Deployment and replaces a Job at the next pass. While
a Secret is missing, the version the live object carries is kept, so a running executor is not torn
down. RBAC cannot restrict `get` to metadata; the code never reads data, and the tests count every
typed Secret call. A missing Secret is still found from the executor Deployments the controller
created: when one is not available because a Secret it references is missing, it records that on
the namespace, with an Event and the annotation `hpc.interlink.eu/<site>-executor: missing
credentials`.

A namespace may be a member of several sites. Each site's executor template names its objects
after the OffloadConfig's `executorService` (template data `.ExecutorService`, `.ExecutorPort`), so
the executors of two sites live side by side: give each OffloadConfig its own `executorService`, and
let the admission policy's `executor` variable accept each.
