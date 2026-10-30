# kueue-offload, the Kueue-native shape

kueue-offload runs Kueue Workloads on a batch system that has no Kubernetes API (Slurm today)
with no virtual node: an admission check controller hands a Workload whose quota is reserved to a
per-tenant executor, which runs all its PodSets as one batch job under the tenant's own identity.
The reference deployment runs batch Jobs, pod groups, JobSet and TrainJob gangs, a Katib
experiment, a pipeline step and notebooks this way on a production HPC system. `design.md`
records how the current code works. This file describes the shape it should take to follow Kueue
and MultiKueue, and the upstream change that shape needs.

Citations: Kueue paths are relative to https://github.com/kubernetes-sigs/kueue at a tag, written
`0.16.9` (what the reference cluster runs) or `0.20.4` (the latest release). Abbreviations:
`v1beta2/` is `apis/kueue/v1beta2/`, `jf/` is `pkg/controller/jobframework/`, `mk/` is
`pkg/controller/admissionchecks/multikueue/`, `keps/N` is the KEP README. Paths with no tag are in
this repository. The examples use a site `hpc-a` and a project `proj42`.

## 1. The problem in Kueue terms

A ClusterQueue's quota can stand for capacity outside the cluster: the batch queues (a partition
and a QOS each) that project `proj42` holds at site `hpc-a`, for example a short debug queue, an
interactive queue and a long production queue. The site is reached only through its login
service, under each user's own account, and offers no Kubernetes API. What is needed:

- R1. Kueue decides whether and when a Workload runs: quota, cohorts, fair sharing, priorities.
  The site's scheduler decides where. Kueue does not preempt a running HPC job until that path is
  designed (3.1, section 5 Risks).
- R2. The Workload runs as one batch allocation that holds every PodSet, so a gang is one
  allocation, and no pod of it is created in the cluster.
- R3. It runs as the user who submitted it. No cluster-scoped component holds a tenant credential.
- R4. The owner (Job, JobSet, TrainJob, plain pods) ends in the state the job ended in, so
  workflow engines that read it (pipelines, hyperparameter search) work unchanged.
- R5. A project can have several queues on one machine, and work can burst between cluster
  capacity and HPC capacity.

Kueue's extension point for this is the admission check, which KEP 993 defines as checks
"implemented inside or outside of Kueue" that "may provide functionality like budgeting, capacity
provisioning or some elements of multicluster dispatching" (`0.16.9 keps/993:28-31`). The only
built-in way to run a Workload somewhere else is MultiKueue, which assumes four things an HPC
system breaks:

- the worker is a Kubernetes cluster running Kueue: the state MultiKueue reads is a remote
  `*kueue.Workload` (`0.16.9 mk/workload.go:81-88`);
- the credential is a kubeconfig, in a Secret of Kueue's own namespace or a file on its disk, or
  comes from a ClusterProfile, so it is per cluster, not per user
  (`0.16.9 mk/multikueuecluster.go:606-640`, `v1beta2/multikueue_types.go:43-48, 61-66, 75-84`;
  unchanged in `0.20.4 v1beta2/multikueue_types.go:82-91`);
- the check covers every flavor of its ClusterQueue: a per-flavor MultiKueue check makes the
  ClusterQueue inactive (`0.16.9 pkg/cache/scheduler/clusterqueue.go:299-301, 401-408`; still in
  `0.20.4` at `:372-374, 473-478`);
- local execution is suppressed, and `managedBy` defaulted, only for MultiKueue's literal
  controller name (`0.16.9 pkg/util/admissioncheck/admissioncheck.go:206-235`,
  `jf/defaults.go:99-120`).

An HPC offload today therefore either rebuilds Kubernetes in front of the site (a virtual
kubelet, or an API server facade that MultiKueue could talk to) or runs as a separate admission
check controller that can never report `Ready`, which is what kueue-offload does (`design.md`,
[The check stays Pending](design.md#the-check-stays-pending)). This document proposes the shape in between.

## 2. How kueue-offload maps onto Kueue and MultiKueue

| kueue-offload today | Kueue concept | Kueue source | Gap |
| --- | --- | --- | --- |
| AdmissionCheck `controllerName: interlink.eu/offload`, parameters `OffloadConfig` | admission check with a parameters object, like ProvisioningRequestConfig and MultiKueueConfig | `0.16.9 v1beta2/admissioncheck_types.go:55-82` | none |
| a ResourceFlavor per Slurm queue, `OffloadConfig.flavors` maps it to partition and QOS | ResourceFlavor; a check per flavor with `onFlavors` (KEP 1432) | `0.16.9 v1beta2/clusterqueue_types.go:159-169`, `keps/1432:31-33` | `onFlavors` unused; the target sits in the check's config, not on the flavor |
| a ClusterQueue per project, a LocalQueue per membership, a Cohort | ClusterQueue `namespaceSelector`, LocalQueue, Cohort | `0.16.9 v1beta2/clusterqueue_types.go:81, 105`, `v1beta2/localqueue_types.go:33-37`, `v1beta2/cohort_types.go:24` | none |
| check `Pending` while the job is queued and while it runs ([The check stays Pending](design.md#the-check-stays-pending)) | `Pending` is "the check still hasn't been performed"; `Ready` means the Workload "can begin execution" | `0.16.9 v1beta2/admissioncheck_types.go:37-44` | everything keyed on `Admitted` is inert (section 3.4) |
| Job kept suspended, pods kept gated, terminal status patched | MultiKueue's batch Job path with `MultiKueueBatchJobWithManagedBy` off: skip the local start, copy the finished status | `0.16.9 jf/reconciler.go:1231-1246`, `pkg/controller/jobs/job/job_multikueue_adapter.go:89-94`, `pkg/features/kube_features.go:364-367`, `pkg/controller/jobs/pod/pod_multikueue_adapter.go:140-170` | the skip is keyed on MultiKueue's name; for Jobs the gate is on since 0.15 and GA in 0.17 (3.4) |
| per-kind finishing in `pkg/offload/owner.go` | `MultiKueueAdapter` (SyncJob, DeleteRemoteObject, IsJobManagedByKueue, GVK) | `0.16.9 jf/interface.go:259-273`; `0.20.4 jf/multikueue.go:31-102` | terminal state only, written by hand |
| unknown owner kind: `Rejected` | no adapter, or job not managed: `Rejected` | `0.16.9 mk/workload.go:156-239` | same shape |
| an `OffloadRequest` per attempt ([The OffloadRequest](design.md#the-offloadrequest)) | create a remote Workload (`cloneForCreate`), then the job through `SyncJob` | `0.16.9 mk/workload.go:812-824` | MultiKueue sends only the Spec, not `status.admission` |
| the executor's status on the request, watched | remote watches feeding the workload controller | `0.16.9 mk/multikueuecluster.go:192-249` | none; the executor polls the site |
| deleting the request, whose executor finalizer cancels the job; garbage collection with the Workload ([Workload deletion](design.md#workload-deletion)) | `RemoveRemoteObjects` and a GC by origin label | `0.16.9 mk/workload.go:129-154`, `mk/multikueuecluster.go:380-431` | ownership instead of an origin label |
| `executorLostTimeout` | `workerLostTimeout`, then `Retry` "Reserving remote lost" | `0.16.9 apis/config/v1beta2/configuration_types.go:268-269`, `mk/workload.go:494-503` | same |
| `retryStrategy` (Kueue's `ProvisioningRequestRetryStrategy`), the request's `attempt` and Kueue's `requeueAfterSeconds` ([Retries](design.md#retries)) | `AdmissionCheckState.retryCount` and `requeueAfterSeconds` (KEP 3258); ProvisioningRequestConfig `retryStrategy` | `0.16.9 v1beta2/workload_types.go:760-777`, `v1beta2/provisioningrequestconfig_types.go:67-78`, `pkg/controller/admissionchecks/provisioning/controller.go:540-560` | same; with `onFlavors` on a subset of flavors Kueue drops the check state, `retryCount` included, when it clears the admission (`0.16.9 pkg/workload/workload.go:1462-1487`), so the attempt number then has to live on the request object |
| a transient `sbatch` refusal waits holding the quota ([Transient submit failures](design.md#transient-submit-failures)) | `Retry`: "back off (possibly allowing other to try, unblock quota)" | `0.16.9 v1beta2/admissioncheck_types.go:26-30` | quota held; kept for submit limits (3.2) |
| owner annotations forwarded into PodSet templates ([Owner annotations](design.md#owner-annotations)) | `provreq.kueue.x-k8s.io/` annotations copied onto the Workload (KEP 1788) | `0.16.9 pkg/controller/constants/constants.go:41-42`, `jf/interface.go:249` | only for ProvisioningRequest |
| an `OffloadWorker` per site, `Active` from its login endpoint; each tenant executor's heartbeat on its requests ([Executor heartbeat](design.md#executor-heartbeat)) | MultiKueueCluster with an `Active` condition that feeds the check's `Active` | `0.16.9 v1beta2/multikueue_types.go:25, 94-105`, `mk/admissioncheck.go:53-111` | MultiKueue's credential model is per cluster |
| check `Active` from the OffloadWorkers its OffloadConfig names | check `Active`; an inactive check makes the ClusterQueue inactive | `pkg/offload/admissioncheck.go`; `0.16.9 pkg/cache/scheduler/clusterqueue.go:289-292, 394-396` | none |
| one controller Deployment serving every check, parameters resolved per check | one controller per `controllerName` serving every check, parameters resolved per check | `0.16.9 pkg/util/admissioncheck/admissioncheck.go:47-153` | none |
| membership controller: ClusterQueues and labels from Slurm associations, `ProjectQuota` | outside Kueue (cluster discovery and configuration sync are MultiKueue non-goals); since 0.18 quota from workers (KEP 9988, alpha, off), since 0.20 from a `CapacityProvider` (KEP 12382, gate `DynamicQuotaOrchestration`, alpha, off) | `0.16.9 keps/693-multikueue:64-67`; `0.20.4 keps/9988:53-55`, `keps/12382:62-80`, `apis/kueue/v1alpha1/capacityprovider_types.go:24-48`, `pkg/features/kube_features.go:1196-1198, 1347-1349` | a second source of quota |
| one site per check | MultiKueue dispatchers: AllAtOnce, Incremental, External | `0.16.9 apis/config/v1beta2/configuration_types.go:271-275, 297-303`, `keps/693-multikueue:417-420` | none needed yet |
| PodSets expanded into synthetic pods, rebuilt into a gang by the plugin's buffer | PodSet (template, count, minCount, topologyRequest) and its PodSetAssignment (flavors, resourceUsage, count) | `0.16.9 v1beta2/workload_types.go:250-275, 517-562` | the gang is solved one layer too low |
| shadow pods for logs and tunnels, staging | nothing; storage is a MultiKueue non-goal | `0.16.9 keps/693-multikueue:61-63` | stays outside Kueue (section 4) |

## 3. Proposed architecture

```
user: Job / JobSet / TrainJob / pods   (queue-name label)
        |
Kueue:  LocalQueue -> ClusterQueue proj42 [flavors: local-gpu, hpc-a-debug, hpc-a-prod]
        |   offload check with onFlavors: [hpc-a-debug, hpc-a-prod]
        v   (quota reserved on an HPC flavor)
offload controller (MultiKueue-shaped): Workload -> OffloadRequest in the user's namespace
        |
executor of that user (namespaced Role, the user's key): OffloadRequest -> one batch job
        |                                           -> status back on the OffloadRequest
offload controller: check state, owner status through a per-kind adapter
```

### 3.1 HPC queues are ResourceFlavors

Each batch queue (site, partition, QOS) is one ResourceFlavor: `hpc-a-debug`, `hpc-a-prod`,
`hpc-a-interactive`. The queue it stands for is written on the flavor as `nodeLabels`
(`offload.interlink.eu/site`, `.../partition`, `.../qos`), so the controller reads the target
from the flavor Kueue assigned and a user can pin one queue with a `nodeSelector` on those keys,
which flavor assignment already matches (`0.16.9 pkg/scheduler/flavorassigner/flavorassigner.go:948-963`).
Kueue copies flavor `nodeLabels` into the pod templates only when it starts the job
(`0.16.9 jf/reconciler.go:1477-1506`, `pkg/podset/podset.go:86`), which never happens in the
cluster for an offloaded Workload. A `NoSchedule` taint on the HPC flavors makes offloading opt-in
through a toleration, since a flavor with an untolerated taint is skipped (same lines).

The offload check is attached to the HPC flavors with `onFlavors`. A Workload admitted on a local
flavor carries no offload check and is `Admitted` in the same patch as its reservation
(`0.16.9 pkg/scheduler/scheduler.go:715-720`); one on an HPC flavor gets the check after its
reservation (`0.16.9 pkg/workload/workload.go:1462-1487`). This is KEP 1432's first user story with
HPC queues in place of spot capacity (`0.16.9 keps/1432:43-46`).

Flavor choice uses quota, taints and node labels, not queue policy
(`0.16.9 pkg/scheduler/flavorassigner/flavorassigner.go:812-905, 921-966`). A project's queues
are ordered flavors of one ClusterQueue, and Kueue falls back from one to the next. Wall time
becomes a node label so that a long job skips a short queue: each flavor carries
`offload.interlink.eu/max-wall-seconds`, and the project's webhook adds a required node affinity
`Gt <wall time - 1>` on that key from the Workload's maximum execution time. Kueue matches
required affinity on flavor label keys with kube-scheduler semantics
(`0.16.9 flavorassigner.go:987-1031`), so the job falls back past the debug queue to the others
(read from the code, not tested). In a mixed ClusterQueue the local nodes need the label too,
because the term also reaches the kube-scheduler. Kueue quota is per ClusterQueue and per flavor
and resource (`0.16.9 v1beta2/clusterqueue_types.go:187-260`). A `pods` quota maps to the
queue's node limit, and a CPU or GPU quota maps to its group TRES. Per-user limits and submit
limits have no Kueue counterpart. They stay on the site, and ordered flavors do not spill over on
them: a job refused for them waits (3.2).

Access to each queue follows the site's associations. Membership reads each association's QOS
list (`format=cluster,account,user,qos`; today only `cluster,account,user`,
`config/samples/membership/associations.yaml:3`). A queue that not every member may use gets a
taint on its flavor, and a webhook admits the matching toleration only from namespaces whose
label grants that QOS. Separate ClusterQueues are only for queues with different members.

Each project's ClusterQueue stays in the site's Cohort (`pkg/membership/sync.go:230`), so
projects at one site borrow from each other. A project with several ClusterQueues groups them
under a child Cohort whose `parentName` is the site's (`0.16.9 v1beta2/cohort_types.go:36`).
Local flavors in a project's ClusterQueue join the same tree: other projects can borrow them
unless their `lendingLimit` is 0 (`0.16.9 v1beta2/clusterqueue_types.go:274`). Every
ClusterQueue with HPC flavors keeps `withinClusterQueue` and `reclaimWithinCohort` at `Never`
(`pkg/membership/sync.go:235-237`) until preempting a running job, its `scancel` and the
quota-release race (section 5 Risks) are designed. Preemption is set per ClusterQueue
(`0.16.9 v1beta2/clusterqueue_types.go:116`), so a mixed ClusterQueue cannot enable it for local
work only.

Each PodSet gets its flavor on its own (`0.16.9 pkg/scheduler/flavorassigner/flavorassigner.go:594-629`),
so a JobSet's leader and workers can land on different queues, or on a local and an HPC flavor.
The controller rejects a Workload whose PodSets are not all on one flavor, with a message asking
to pin one queue on every PodSet; the project's webhook can copy the first PodSet's pin to the
others. A heterogeneous job is used only where the site's OffloadWorker declares support.

`managedBy` constrains bursting. Kueue defaults it when a job is created, from every check of
the ClusterQueue whatever its flavors (`0.16.9 jf/defaults.go:99-120`,
`pkg/cache/scheduler/cache.go:327-341`), and a Job's `managedBy` is immutable. A remote check in
a mixed ClusterQueue would therefore stop a Job from ever running locally on the local flavor. We
believe this is why MultiKueue checks cannot be per flavor, though the code gives no reason
(`0.16.9 pkg/cache/scheduler/clusterqueue.go:401-404`). Consequences:
- ClusterQueues with only HPC flavors can use `managedBy` and `Ready` (3.4).
- Mixed ClusterQueues keep the owner suspended until it ends (the Pending model).
- Pods are not affected, because gating is decided at start, when the flavor is known
  (`0.16.9 jf/reconciler.go:1231-1246`).

### 3.2 The offload controller is a MultiKueue-style check

It has three of MultiKueue's four parts (`0.16.9 keps/693-multikueue:241-333`); ownership
replaces the fourth, the garbage collector:
- an AdmissionCheck reconciler that sets `Active` from the workers it may use
  (`0.16.9 mk/admissioncheck.go:53-111`);
- a worker reconciler with one `OffloadWorker` per site, whose `Active` condition comes from
  site-level facts only (a valid configuration, the login service reachable), never from one
  tenant's executor, since an inactive check makes every ClusterQueue using it inactive
  (`0.16.9 pkg/cache/scheduler/clusterqueue.go:219-220, 289-291`);
- a Workload reconciler that runs the state machine below.

Per-user executor health stays on the namespace ([Executor health](design.md#executor-health)). An executor writes `Accepted` on each
OffloadRequest it takes and a heartbeat (`lastSyncTime`) on every poll. While no executor
answers, the check stays `Pending` with "no executor answering in namespace X", and
`executorLostTimeout`, MultiKueue's `workerLostTimeout`, applies to the heartbeat.

One Deployment serves every AdmissionCheck with its `controllerName` and resolves each check's
OffloadConfig separately, as Kueue's own check controllers do
(`0.16.9 pkg/util/admissioncheck/admissioncheck.go:47-153`). OffloadConfig shrinks to check
parameters: the workers it may use (one until a dispatcher exists), `retryStrategy`,
`pollInterval`, `executorLostTimeout` and a maximum time a job may wait in the site's queue
(`maxQueueTime`). It also keeps the queue target in `flavors` until it moves onto the flavors
(3.1), and the membership controller's settings.

| Executor state | Worker condition (as a remote Workload's) | Check | Kueue then |
| --- | --- | --- | --- |
| staging in | `StagingIn` | `Pending`, the stage Job's progress in the message | holds the quota |
| submitted, pending in the site's queue | `QuotaReserved` | `Pending`, the site's reason in the message | holds the quota |
| submit limit reached, site unreachable | none | `Pending`, the reason in the message | holds the quota ([Transient submit failures](design.md#transient-submit-failures)); the executor does not submit while the user's own jobs on that QOS are at the limit |
| running | `Admitted` | `Ready` (managedBy model) or `Pending` (suspended model) | `Admitted`; the execution time clock starts |
| ended, staging out | `StagingOut` | unchanged | holds the quota |
| completed or failed, stage-out done | `Finished` | unchanged; the adapter writes the owner's terminal status (a failed stage-out fails the job) | finishes the Workload, releases the quota |
| cancelled outside, worker lost past `workerLostTimeout`, queued past the maximum | `Evicted` | `Retry` with `requeueAfterSeconds` | evicts, requeues after the delay |
| permanent refusal (account, partition, bad request) | none | `Rejected` | deactivates the Workload |

Stage Jobs are keyed by the Workload UID. The executor deletes them with the attempt's request, so
a new attempt stages in again.

Going `Ready` only at `running` is MultiKueue's `MultiKueueWaitForWorkloadAdmitted` behaviour
(beta and on in 0.16; `0.16.9 mk/workload.go:449`, `pkg/features/kube_features.go:456-458`). It
makes `Admitted` mean "running": the maximum execution time then counts run time only
(`0.16.9 pkg/controller/core/workload_controller.go:622-650`), and
`admission_checks_wait_time_seconds`, "the time from when a workload got the quota reservation
until admission" (`0.16.9 pkg/metrics/metrics.go:318-323`), becomes the time spent in the site's
queue. Kueue puts no timeout on a `Pending` check: its timers act only on admitted Workloads
(`0.16.9 pkg/controller/core/workload_controller.go:622-626, 1114-1122`). The maximum queue time
is therefore the controller's job.

`Retry` delays come from Kueue's helper (`0.16.9 pkg/workload/admissionchecks.go:252`) with a
`retryStrategy` shaped like ProvisioningRequestConfig's: `Retry` until `backoffLimitCount`, then
`Rejected` (`0.16.9 v1beta2/provisioningrequestconfig_types.go:67-78`,
`pkg/controller/admissionchecks/provisioning/controller.go:540-560`). With that type's defaults
(3 attempts, 60 s base, 1800 s cap) and a doubling backoff (`0.16.9 pkg/workload/admissionchecks.go:252-265`), a
job is `Rejected` after about 7 minutes, which is why a submit limit is not a `Retry`. The
attempt number lives on the request object. Reading the code (not tested), a per-flavor check's
state, including `retryCount`, leaves the Workload when eviction clears `status.admission`
(`0.16.9 pkg/workload/workload.go:1462-1487`, `pkg/controller/core/workload_controller.go:687-698, 777-800`).

The contract with the executor is a namespaced object, not an HTTP call. For each attempt the
controller creates an `OffloadRequest` named `<workload>-<check>-<attempt>` in the Workload's
namespace, controller-owned by the Workload. This is the pattern Kueue uses for
ProvisioningRequests (`0.16.9 pkg/controller/admissionchecks/provisioning/provisioning.go:55-58`,
`controller.go:315`).
- Its spec carries the Workload reference and UID, the check and the attempt, the site and
  account, the owner, every PodSet with its assigned flavors and count (from `status.admission`),
  its template as the Workload holds it and its flavor's expanded annotations (partition and QOS
  among them), the forwarded owner annotations, `maximumExecutionTimeSeconds` and the priority.
- Its status carries the conditions and messages the executor writes, the heartbeat, the job ID,
  the nodes, the ranks and the shadow's names: all the executor keeps about the job.
- The tenant's executor watches it with a RoleBinding in its namespace and holds a finalizer on
  it, so the site job is cancelled before the object goes. Before submitting, it checks the
  request against the Workload it names (`get` on workloads): it refuses the request unless the
  controller owner reference's UID matches and the PodSets and admission are equal, and waits
  while the Workload is not `QuotaReserved=True` with this check `Pending`.
- Only the controller's ServiceAccount may create it, change its spec or site label, or touch its
  own finalizer, and only the executor of the request's namespace its status and its finalizer.
  Tenants get no write on `offloadrequests`, and the ValidatingAdmissionPolicy of
  `config/admission` enforces the split for anyone else with write (admissionregistration v1,
  introduced in 1.30: k8s.io/api v0.31.1 `admissionregistration/v1/types.go:139`;
  [Who may write it](design.md#who-may-write-it)). Otherwise anyone who can write one in the
  namespace could have the executor submit as the user, bounded only by its comparison with the
  Workload.
- The controller deletes it on `Retry` and when the Workload loses its reservation. The request
  of a finished job stays, and with it the job directory and the shadow, so the logs stay
  readable; garbage collection removes it with the Workload. A deleted request keeps the
  controller's finalizer, and with it the attempt number, until the next attempt exists or the
  Workload can have none.

This replaced a private record per Workload and its resync
([Workload deletion](design.md#workload-deletion)), and the executor's HTTP job API with its
caller authentication and endpoint resolution.
KEP 2349 deferred a dedicated API between the MultiKueue check and external controllers
(`0.16.9 keps/2349:188-193`). A versioned CRD that `kubectl` shows answers its protocol and
compatibility points; its point that debugging across layers is harder remains, and the KEP
must address it.

The owner reference omits `blockOwnerDeletion`, because setting it would need `update` on
`workloads/finalizers` (not verified on a cluster).

### 3.3 The worker kind

Upstream, a site becomes a MultiKueueCluster whose `clusterSource` is a third, external kind
(section 5). MultiKueue's workload controller then writes the request object of 3.2 in place of a
remote Workload, and reads its conditions where today it reads the remote Workload's
`QuotaReserved`, `Admitted`, `Evicted` and `Finished` (`0.16.9 mk/workload.go:297-324, 326-508`).
Until that exists, `OffloadWorker` has the same role and shape: one object per site, an `Active`
condition, and the controller that serves it.

The site is normally chosen by flavor, because quota differs per site and per project account. A
dispatcher is needed only when several sites are interchangeable under one quota. MultiKueue's
External dispatcher lets any controller nominate workers through `status.nominatedClusterNames`
(`0.16.9 keps/693-multikueue:417-420`). Such a dispatcher could rank sites by the site
scheduler's start estimate.

### 3.4 managedBy and Ready, the Pending model before that

On Kubernetes 1.32 or later, and for ClusterQueues with only HPC flavors, the owner carries
`managedBy: interlink.eu/offload`. The check goes `Ready` at `running`. Kueue admits and
unsuspends the owner, its own controller ignores the object, and the adapter mirrors running and
terminal status. Two sources support this:
- The Job field is alpha and off in `k8s.io/api` v0.31.1 (`batch/v1/types.go:483-484`) and beta
  and on in v0.33.1 (`:473-474`).
- The 1.32 boundary is from the Kubernetes release notes and was not checked against a 1.32
  source here.

Kueue defaults `managedBy` only for MultiKueue (`0.16.9 jf/defaults.go:99-120`), so until the
KEP lands a webhook of this project sets it, with the same rule. Per kind:
- JobSet's `managedBy` is a CRD field, and the JobSet controller skips any custom value
  (jobset v0.10.1 `api/jobset/v1alpha2/jobset_types.go:138-150`). It works on any Kubernetes
  version.
- TrainJob accepts only Trainer's own value or `kueue.x-k8s.io/multikueue` (trainer v2.1.0
  `pkg/apis/trainer/v1alpha1/trainjob_types.go:135-146`). It needs the MultiKueue path, or a
  change in Trainer.
- Plain pods have no `managedBy`, and only a MultiKueue check skips their ungating
  (`0.16.9 pkg/util/admissioncheck/admissioncheck.go:224-235`). They need the Kueue change.

Everywhere else (Kubernetes 1.30, mixed ClusterQueues, pods and TrainJob today), the Pending
model ([The check stays Pending](design.md#the-check-stays-pending)) stays. The check is `Pending` for the job's whole life, and the owner stays suspended
or gated. The adapter patches the terminal status, and Kueue finishes the Workload, because it
handles a finished job before a suspended one (`0.16.9 jf/reconciler.go:416-459, 563`). MultiKueue
skipped the local start of batch Jobs without `managedBy` only with
`MultiKueueBatchJobWithManagedBy` disabled (`0.16.9 jf/reconciler.go:1231-1246`); that gate is
beta and on since 0.15 (`0.16.9 pkg/features/kube_features.go:364-367`), and with it on the
0.16.9 Job adapter already requires `managedBy`
(`0.16.9 pkg/controller/jobs/job/job_multikueue_adapter.go:212-227`). The gate went GA in 0.17
(`0.20.4 CHANGELOG/CHANGELOG-0.17.md:408`), removing that path: only composable jobs skip the
local start (`0.20.4 jf/reconciler.go:1813-1826`).

In the managedBy model, `SyncStatus` also writes the running rank count to `status.ready` (Job)
and `replicatedJobsStatus[].ready` (JobSet) once the job runs. With `waitForPodsReady` set, Kueue
evicts an admitted Workload that does not report ready pods in time, which would cancel a running
job, and `blockAdmission` would stall the ClusterQueue
(`0.16.9 pkg/controller/core/workload_controller.go:812-840, 1114-1122`,
`pkg/controller/jobs/job/job_controller.go:322-329`,
`pkg/controller/jobs/jobset/jobset_controller.go:178-188`,
`apis/config/v1beta2/configuration_types.go:227-233`).

The Pending model's cost is that everything keyed on `Admitted` does nothing:
- the maximum execution time (`0.16.9 pkg/controller/core/workload_controller.go:622-626`);
- `waitForPodsReady` (`:1114-1122`);
- `admitted_workloads_total` and the wait time metric (`0.16.9 pkg/metrics/metrics.go:254-262, 318-323`);
- Concurrent Admission (alpha, off; `0.20.4 pkg/features/kube_features.go:1183-1185`), whose
  parent is admitted only when a variant is (`0.20.4 keps/8691:496-498, 520`).

On 0.16.9, admission fair sharing treats these Workloads unevenly. The sampled usage
counts admitted Workloads only (`0.16.9 pkg/controller/core/localqueue_controller.go:316-321, 348`),
while the entry penalty is added at reservation and settled only at admission
(`0.16.9 pkg/scheduler/scheduler.go:733-737`, `pkg/controller/core/workload_controller.go:983-989, 1071`).
A Workload that never becomes `Admitted` thus leaves an unsettled penalty in its LocalQueue's
usage (`0.16.9 pkg/workload/workload.go:410-424`). 0.20 moves both to the reservation through
`AdmissionFairSharingAnchorAtQuotaReservation` (beta, on;
`0.20.4 pkg/features/kube_features.go:155-161, 1020-1022`).

### 3.5 Per-kind adapters

This project's adapter is modelled on the owner half of Kueue's `MultiKueueAdapter`
(`IsJobManagedByKueue`, the status copy in `SyncJob`, `DeleteRemoteObject`, `GVK`;
`0.16.9 jf/interface.go:259-273`) and has these methods:
- `GVK()`;
- `IsManaged(owner)`;
- `SyncStatus(owner, outcome)`;
- `Delete(owner)`.

Adapters are registered by GVK, like `multiKueue.externalFrameworks`
(`0.16.9 apis/config/v1beta2/configuration_types.go:276-281`, `keps/2349:74-121`). A generic
unstructured adapter can be configured with a kind's terminal condition types.

`SyncStatus` mirrors running status only onto an owner whose status this project owns through
`managedBy`. On a suspended owner it writes only the terminal state, because a status patch
there would violate Kubernetes Job suspend validation (`0.20.4 jf/multikueue.go:35-38`); in
0.16.9 Kueue's own Job adapter skips that sync because a non-zero `status.startTime` blocks
later spec updates and Kubernetes 1.36 rules need `JobSuspended=True`
(`0.16.9 pkg/controller/jobs/job/job_multikueue_adapter.go:156-166`).

For a failed pod group, the adapter also sets the annotation
`kueue.x-k8s.io/retriable-in-group: "false"` on the pods of the group. Without it, Kueue
keeps waiting for replacement pods and the group never finishes
(`0.16.9 pkg/controller/jobs/pod/pod_controller.go:226-238, 351-394`,
`pkg/controller/jobs/pod/constants/constants.go:34-35`). `finishPods`
(`pkg/offload/owner.go`) sets it on every pod of a failed group before marking the pod Failed.

### 3.6 The executor turns a Workload into one job, with no pods

The executor reads the OffloadRequest:
- the PodSets, with template, count and assigned flavor;
- `resourceUsage` (`0.16.9 v1beta2/workload_types.go:250-275`);
- `maximumExecutionTimeSeconds` (`:77`).

It writes one batch job:
- one allocation for every PodSet, which share one flavor (3.1); a heterogeneous job only where
  the site declares support;
- node sizes from the requests Kueue charged;
- the wall time from `maximumExecutionTimeSeconds`, set through the job label
  `kueue.x-k8s.io/max-exec-time-seconds` (`0.16.9 pkg/controller/constants/constants.go:44-45`).
  KEP 3125 names Slurm's `-t` as its model (`0.16.9 keps/3125-maximum-execution-time:42`).

What the kubelet would have done (resolving `env[].valueFrom`, ServiceAccount files, tokens) is
done once per PodSet template. The plugin's per-pod resource translation is reused, called with
in-memory members built from each template and never created in the API server, and they reach
the plugin as one group, so the gang buffer goes. Per-container exit codes still come from the
plugin's status files (`ranks[].containers`, `design.md` "Offload controller behaviour"), which
Argo and pipelines read on the owner; the batch system's job state is used only for the
allocation (node failure, time limit).

The executor is backend-neutral. It drives a backend plugin through a "submit a group of PodSets"
capability; a backend without it refuses a PodSet with `count` above 1 instead of submitting one
job per pod.

### 3.7 Identity stays in the tenant namespace

Neither controller reads a Secret's data: the offload controller has no Secret access, and the
membership controller reads Secrets as metadata only, to roll an executor whose Secret was replaced
(`design.md`, "Executor health"). The tenant's executor runs in the tenant's namespace with a
RoleBinding there only and the tenant's own key, and it acts only on OffloadRequests there. The worker
object (OffloadWorker, later the external MultiKueueCluster) is a router: it names the controller
that serves the site. The namespace decides whose identity runs the request, from the site's own
account associations.

MultiKueue keeps each worker's kubeconfig in a Secret of Kueue's namespace or a file on its disk
(`0.16.9 mk/multikueuecluster.go:606-640`, `v1beta2/multikueue_types.go:43-48`), or gets
credentials per cluster through a ClusterProfile credential plugin
(`0.16.9 keps/693-multikueue:256-290`). Neither is per user. A shared, site-side endpoint fits the
same contract only if each call carries a per-user credential, such as the tenant executor's
projected ServiceAccount token with an audience the site maps to the user; a cluster-scoped
caller asserting the user could act as anyone.

Tunnel allow lists stay out of the tenant's hands. The executor enforces
`Shadow.Reverse.AllowedTargets` for `interlink.eu/reverse-forwards` (plugin
`pkg/shadow/reverse.go:194`), but its configuration lives in the tenant namespace. The offload
controller therefore also checks those annotations against a site-owned list in OffloadConfig
before it creates the OffloadRequest, and answers `Rejected` for a target outside it (not
implemented yet).

## 4. What stays outside Kueue, and why

| Piece | Why it is not Kueue's |
| --- | --- |
| Executor: SSH, job scripts, site launchers (torchrun, MPI, Ray), image staging, kubelet emulation | specific to each backend. MultiKueue leaves authentication between clusters out of scope (`0.16.9 keps/693-multikueue:68`) |
| Site identity mapping and membership: associations become namespace labels, LocalQueues, ClusterQueues and executors | MultiKueue leaves discovering clusters and syncing configuration to other tooling (`0.16.9 keps/693-multikueue:64-67`). The output is plain Kueue objects. In 0.20 the quota half could be published as a `CapacityProvider` (gate `DynamicQuotaOrchestration`, alpha, off; `0.20.4 keps/12382:62-80`, `pkg/features/kube_features.go:1347-1349`) |
| Tunnels and logs (the shadow) | Kueue has nothing for logs or ingress of remote work. The shadow is a real pod outside every ClusterQueue, rendered by the executor for every job by default (`AlwaysShadow`, plugin `pkg/slurm/executor_config.go:66-68`, with a site opt-out), and it outlives the job so `kubectl logs` still works ([Shadow](design.md#shadow)) |
| Staging data between cluster volumes and the site's filesystem | storage is a MultiKueue non-goal (`0.16.9 keps/693-multikueue:61-63`). It runs as executor-side Jobs |

## 5. KEP sketch: MultiKueue external workers

**Summary.** MultiKueue gains a worker kind with no Kubernetes API. A MultiKueueCluster of this
kind names an external controller. For a Workload nominated to that cluster, MultiKueue creates a
namespaced, Workload-owned `ExternalWorkload` and reads its conditions in place of a remote
Workload's. The external controller has two halves. A per-tenant agent in the Workload's
namespace serves the ExternalWorkload and runs the job on its backend under that tenant's
identity; it is namespaced so that no component holds every user's credential. A
credential-free cluster half writes the owner's status and the MultiKueueCluster's `Active`
condition. Admission, dispatch, `managedBy` defaulting, the local-execution skip and worker-lost
handling stay MultiKueue's.

**Motivation.** Batch systems that are not Kubernetes (Slurm, HTCondor) hold much of the
accelerator capacity research users have. Today they are reached through virtual kubelets, which
fake nodes and create pods before a gang is decided, or through standalone check controllers that
cannot report `Ready` and repeat MultiKueue's state machine. KEP 2349 already considered an
"external" controller type: "MultiKueue would only sync the `Workload` object to the worker
cluster. The external controller would be responsible for creating the job on the worker and
syncing its status" (`0.16.9 keps/2349:243`). It deferred it until "there is a strong demand"
(`:256`). It still assumes Kubernetes workers; the precedent is the split of responsibilities,
not the worker kind. kueue-offload is the reference implementation and runs gangs, Trainer jobs,
pipeline steps and notebooks on a production HPC system.

**Goals.**
- A MultiKueueCluster kind served by an external controller, with no client to a remote API
  server inside Kueue.
- A namespaced contract object carrying the Workload's PodSets, `status.admission` and the owner
  annotations under a configured prefix (generalizing KEP 1788's `provreq.kueue.x-k8s.io/`).
- No credential for such workers held by Kueue: identity is the external controller's business.
- The single-start guarantee of KEP 693 (`0.16.9 keps/693-multikueue:307-313`) also for workers
  where a reservation already queues the job.
- Phase 1: one external cluster per MultiKueueConfig, so one site per ClusterQueue, because
  MultiKueue picks workers from `clusters` with no tie to the assigned flavor
  (`0.16.9 v1beta2/multikueue_types.go:142-150`); or a flavor-to-worker binding.
- Phase 2: external checks per flavor for bursting, with a defined owner mode for mixed
  ClusterQueues (3.1).

**Non-goals.** HPC-specific code in Kueue. Logs, tunnels and storage. Per-user quota or site
limits inside Kueue. One job split across workers (already a non-goal of KEP 693,
`0.16.9 keps/693-multikueue:74-76`). Topology Aware Scheduling on external flavors: it needs Node
objects (`0.16.9 keps/2724-topology-aware-scheduling:1307-1323`). The transport between the external
controller and its backend.

**API change.**

```go
// +kubebuilder:validation:ExactlyOneOf=kubeConfig;clusterProfileRef;external
type ClusterSource struct {
    KubeConfig        *KubeConfig              `json:"kubeConfig,omitempty,omitzero"`
    ClusterProfileRef *ClusterProfileReference `json:"clusterProfileRef,omitempty"`
    // external: the worker is run by an external controller; MultiKueue opens no client.
    External *ExternalWorker `json:"external,omitempty"`
}

type ExternalWorker struct {
    ControllerName string                             `json:"controllerName"`
    Parameters     *AdmissionCheckParametersReference `json:"parameters,omitempty"`
    // startsOnReservation: the backend starts the job as soon as it reports QuotaReserved,
    // so MultiKueue nominates such a worker alone, never alongside another.
    StartsOnReservation bool `json:"startsOnReservation,omitempty"`
}

// ExternalWorkload (v1alpha1, namespaced): created by MultiKueue in the Workload's namespace,
// controller-owned by the Workload, one per nominated external cluster.
type ExternalWorkloadSpec struct {
    WorkloadName string            `json:"workloadName"`
    ClusterName  string            `json:"clusterName"`
    PodSets      []PodSet          `json:"podSets"`
    Admission    *Admission        `json:"admission,omitempty"` // flavors per PodSet
    Annotations  map[string]string `json:"annotations,omitempty"`
    MaximumExecutionTimeSeconds *int32 `json:"maximumExecutionTimeSeconds,omitempty"`
}
type ExternalWorkloadStatus struct {
    // QuotaReserved, Admitted, Evicted, Finished: read as a remote Workload's conditions.
    Conditions []metav1.Condition `json:"conditions,omitempty"`
}
```

Behaviour:
- `readGroup` builds its remotes from ExternalWorkloads for external clusters.
- `SyncJob` is skipped for them, because the external controller's cluster half writes the owner
  status. An alternative is an adapter "local half" that applies an outcome object.
- Deletion goes through ownership plus a finalizer that the tenant agent holds.
- The cluster half sets the MultiKueueCluster's `Active` condition from site-level facts.
- `ShouldSkipLocalExecution` and `ApplyDefaultForManagedBy` need no change, because the check is
  a MultiKueue check.

**Alternatives considered.**

| Alternative | Why not |
| --- | --- |
| A Kubernetes API facade behind `kubeConfig` (no Kueue change) | it must serve Workloads, every job kind, label-selector watches with bookmarks and List for GC (`0.16.9 mk/multikueuecluster.go:268-298, 383-431`): the virtual kubelet problem one layer up |
| A virtual kubelet per tenant | fake nodes and capacity; pods exist before the gang is decided |
| A standalone check controller (kueue-offload today) | cannot report `Ready`; repeats MultiKueue's code, which KEP 2349 already held against custom MultiKueue check controllers (`0.16.9 keps/2349:200-205`) |
| A "runs remotely" property on AdmissionCheck that generalizes `ShouldSkipLocalExecution` and `ApplyDefaultForManagedBy` | small, and works per flavor for pods, but every external controller then runs MultiKueue's state machine itself, the burden KEP 2349 cites against external controllers taking the check's role (`0.16.9 keps/2349:195-198`). A good first step on its own |
| A dedicated protocol to workers, such as gRPC | KEP 2349 deferred a dedicated API for complexity, debugging across layers and compatibility of separately built binaries (`0.16.9 keps/2349:188-193`). ExternalWorkload is also a dedicated API: a CRD answers the protocol and compatibility points, not the layering one |
| ClusterProfile with a credential plugin | per cluster, not per user, and still a Kubernetes API (`0.16.9 keps/693-multikueue:256-290`) |

**Risks.**
- Single start: a worker that queues the job when it reports `QuotaReserved` could start it in
  two places under AllAtOnce. `startsOnReservation` limits nomination to one such worker at a time.
- Two schedulers in series: Kueue holds the quota while the site queues the job. A maximum queue
  time ending in `Retry` bounds that.
- Preemption: Kueue releases quota as soon as the owner is inactive (`0.16.9 jf/reconciler.go:533-560`),
  before the remote cancel lands. KEP 10076's `OnTerminal` (batch Job and Pod integrations) waits
  for local pods to terminate, so it does not cover a remote job with no local pods; it is
  provisional with no code in 0.20.4 (`0.20.4 keps/10076-quota-release-strategy:35, 47`).
- Who may write the contract: only MultiKueue may create an ExternalWorkload or change its spec,
  and only the tenant agent its status, or a tenant gets a job with no reservation (3.2).
- Kubernetes Job status validation keeps changing: Kueue fixed MultiKueue Job status writes for
  1.35 and 1.36 (`0.20.4 CHANGELOG/CHANGELOG-0.17.md:236`, `CHANGELOG/CHANGELOG-0.20.md:85`).
- API surface: a new kind that upstream CI must exercise with a fake external controller.
- Adapter interfaces change between minor releases (`0.16.9 jf/interface.go:259-273` against
  `0.20.4 jf/multikueue.go:31-102`).

## 6. Migration from the current code

Steps 1 to 7 need no Kueue change and run on Kueue 0.16.9 and Kubernetes 1.30.

1. Done. Pod groups: on failure, the annotation `kueue.x-k8s.io/retriable-in-group: "false"` is
   set on every pod of the group. This is a metadata patch, so the controller has `pods` patch.
2. Done. `Retry` with `requeueAfterSeconds`: a `retryStrategy` in OffloadConfig, of
   ProvisioningRequestConfig's type and defaults, replaces `maxRetries` and the retry count the
   controller kept per Workload. The controller counts the retries (since step 6 from the
   request's `attempt`), sets `requeueAfterSeconds` with Kueue's
   `UpdateAdmissionCheckRequeueState`, and writes `Rejected` once `backoffLimitCount` retries are
   spent. The in-memory submit backoff goes: a submit refused for now (a submit limit, an
   unreachable site) stays `Pending` holding the quota and is tried again on the executor's next
   poll ([Transient submit failures](design.md#transient-submit-failures), 3.2).
3. Executor: wall time from `maximumExecutionTimeSeconds`. The `--time` written in
   `slurm-job.vk.io/flags` goes away from the examples.
4. Done, except the queue target. One controller for every check with its `controllerName`,
   parameters per check. The unused `alwaysShadow` and `mirrorPodStatus` fields went. The account
   comes from the ClusterQueue's label `hpc.interlink.eu/account`, which the membership
   controller sets, instead of its name. The queue target still sits in OffloadConfig `flavors`;
   its move onto ResourceFlavor `nodeLabels` is open.
5. Done. `OffloadWorker` per site with `Active` from site-level facts (a host:port
   `loginEndpoint` and a TCP connection to it every minute), executor heartbeats per request
   (3.2). The check's `Active` comes from its workers.
6. Done. `OffloadRequest` (3.2) replaces the executor's HTTP submit, status and cancel routes, the
   controller's per-Workload record, the authentication of the controller's calls, the executor's
   endpoint and its state directory. `GET /getLogs` for the shadow stays. The
   ValidatingAdmissionPolicy of 3.2 is `config/admission`. The reference deployment is a
   development one, so the old contract went at once, with no side-by-side release.
7. Executor submits the whole Workload as one job (3.6). The synthetic member pods and the gang
   buffer go; the plugin's per-pod translation and per-container status stay.
8. Adapters by GVK (3.5). Then `managedBy` and `Ready`, opt-in per ClusterQueue with only HPC
   flavors: JobSet first, batch Job on Kubernetes 1.32 or later, tested with `waitForPodsReady`
   on. `managedBy` is immutable, so Jobs created with it must finish before a ClusterQueue opts
   out. Pods, TrainJob and mixed ClusterQueues stay on the Pending model.
9. Move the target Kueue to 0.20 or later: port to the new adapter interfaces, use reserved-usage
   fair sharing. The Job path there requires `managedBy`.
10. Upstream issue, then the KEP of section 5. Once it lands, OffloadRequest becomes
    ExternalWorkload, OffloadWorker becomes an external MultiKueueCluster, and the state machine
    becomes MultiKueue's for ClusterQueues with only HPC flavors of one site. Mixed ClusterQueues
    stay on the per-flavor check of this project until phase 2, because a MultiKueue check cannot
    be per flavor and a ClusterQueue can hold only one (`0.16.9 pkg/cache/scheduler/clusterqueue.go:223-224, 294-301, 401-408`).
    This project keeps the executor, the membership controller and adapters for kinds MultiKueue
    lacks.

## 7. Open questions for Kueue maintainers

1. Is a MultiKueue worker kind with no Kubernetes API acceptable in principle, or would you
   rather generalize the local-execution skip and `managedBy` defaulting to any check that
   declares remote execution, and leave the state machine to external controllers?
2. Is the per-flavor ban on MultiKueue checks there because `managedBy` is defaulted at creation,
   before a flavor exists? If so, is a suspended-owner mode for per-flavor remote checks
   acceptable, given that 0.17 removed the suspended batch Job path?
3. For workers whose reservation already queues the job, is "nominate one at a time" the right
   way to keep the single-start guarantee, or should the worker report a separate "accepted but
   not started" state?
4. Should the external controller write the owner's status (KEP 2349's deferred external mode),
   or should adapters gain a local half that applies an outcome?
5. Is a configurable annotation prefix copied onto the Workload, generalizing
   `provreq.kueue.x-k8s.io/`, acceptable so the worker never reads owner objects?
6. While the site queues a job, Kueue holds its quota. Is Concurrent Admission (KEP 8691, alpha
   and off in 0.20.4) the intended way to race several site queues and local capacity, given
   that a variant wins only by being admitted?
7. Is `CapacityProvider` (KEP 12382) the intended home for quota derived from a site's account
   limits?
8. Preemption of a running remote job: should an external worker be able to hold the quota until
   its backend reports the job gone?
