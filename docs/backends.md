# kueue-offload backends

kueue-offload runs a Workload on any batch system that has an interLink plugin, not only Slurm.
This file covers what lies below the OffloadRequest of `kueue-native.md`: the executor, the plugin
contract and its additions, a shared plugin at the site, and the place of the API server.

Citations: paths with no prefix are in this repository. `slurm` is interlink-slurm-plugin on
branch `feat/dispatch-agent` at b857430, which carries today's executor. `il` is interLink at
ba9f7e85. `htc` is interlink-htcondor-plugin at 5f9ecdb. Kueue paths follow `kueue-native.md`
(`0.16.9 path`). The examples use a site `hpc-a`, a project `proj42` and queues `debug` and `prod`.

## Summary

- The executor moves into this repository (`cmd/executor`, `pkg/executor`, `pkg/shadow`). It does
  what a kubelet would do for the Workload's pods (references, tokens, downward API, shadow, logs)
  and leaves the batch system to the plugin. It speaks only the interLink API.
- The interLink API server sits in front of every plugin: in the tenant pod for in-cluster use, at
  the site for the edge. The executor uses the same calls as the virtual kubelet, plus
  `/createPodSets` where the plugin lists it.
- Queue, account and wall time reach the plugin as annotations that OffloadConfig defines per
  flavor. The executor holds no Slurm flags.
- A plugin lists `pod-group` in its ping answer when it runs N `/create` calls that share Kueue's
  pod group label as one allocation. The Slurm plugin already does this. On a plugin without it, a
  gang is refused, and a Workload marked `independent-pods` runs as one job per pod.
- `/create` answers 503 for a transient refusal (submit limit, scheduler down) and 4xx for a
  permanent one.
- A plugin that also lists `podset-create` gets a pod group in one `POST /createPodSets` and
  submits it as one job without buffering. The executor uses it when listed and keeps per-pod
  `/create` otherwise, and when an API server without the route answers 404; the route is an
  upstream interLink proposal.

## 1. Components

### 1.1 The split

```
OffloadRequest in the tenant namespace (kueue-native.md 3.2)
   |
executor, tenant namespace, namespaced Role                       this repo, pkg/executor
   |  PodSets -> member pods in memory; Secrets, ConfigMaps, tokens, downward API;
   |  stage Jobs; shadow Deployment; log relay; state
   v  interLink API: /create (PodCreateRequests), /status, /getLogs, /delete, /pinglink
interLink API server, stock image                     in the tenant pod, or at the site (edge)
   v  interLink plugin contract
plugin (Slurm, HTCondor, ...): job rendering, allocation, queue flags, submit limits
```

Before the move the executor was the Slurm plugin process: `Executor.Enabled` added its HTTP
routes to the plugin's mux (slurm `cmd/main.go:224-243`), and it called the plugin's functions in
process, `createPod` (slurm `pkg/slurm/executor.go:422`) and `podStatuses` (`:649`). It also ran
`squeue` and `scancel` itself (`:516`, `:579`) and read the plugin's job directories (`:824-840`,
`executor_logs.go:228-243`). Another backend would have had to reimplement all of this. The
executor now runs on its own in the tenant pod, takes OffloadRequests and keeps only `/getLogs`
and `/healthz` for the shadows (`design.md`, "Executor HTTP API").

### 1.2 What moves into pkg/executor

| Today (slurm `pkg/slurm/`) | Moves to | Change |
| --- | --- | --- |
| `executor.go`: routes, records, submit, job status, delete (`:185-191`, `:364-450`, `:623-750`, `:936-970`) | `pkg/executor/server.go` | `createPod`, `podStatuses`, `slurmJobState` and `scancel` become contract calls (1.4). `classifyCreateError` and `sbatchLaterRe` (`:456-476`) move to the plugin. `memberDirs`, `removeMembers` and `statusesFromFiles` (`:478-514`, `:824-840`) are removed: the plugin's `/status` and `/delete` own the job directory |
| `executor_members.go`: `buildMember`, `withServiceAccountDir`, `mintTokens`, `fetchRefs`, `resolveEnv`, field and resource refs (`:80-168`, `:195-228`, `:276-313`, `:371-566`) | `pkg/executor/members.go` | `buildMember` no longer writes `--account`, `--partition` and `--qos` into `slurm-job.vk.io/flags` (`:110-117`); it appends the flavor's annotations instead (1.5). `retrievedPodData` (`:597`) becomes a PodCreateRequests builder, since the API server's `getData` splits the data per container (il `pkg/interlink/api/func.go:27-66`) |
| single-pod downward identity (`:159-168`, `DownwardUIDAnnotation` `:232`, read at `:438`) | `members.go` | none |
| `executor_staging.go` (`:107-337`) | `pkg/executor/staging.go` | none: `Staging.Command` and its environment contract do not depend on Slurm |
| `executor_state.go` | gone | the OffloadRequest status holds the job ID, the ranks and the shadow names (`kueue-native.md` section 6, step 6) |
| `executor_auth.go` | gone | with the HTTP API it guarded (step 6) |
| `executor_shadow.go`, `executor_reverse.go` | `pkg/executor/shadow.go`, `reverse.go` | none. The srun carrier finds the allocation by a job name that follows the Slurm plugin's naming (`executor_reverse.go:18-24`), so it stays a Slurm-only option; other backends use the ssh carrier |
| `executor_logs.go`, `executor_logs_client.go` | `pkg/executor/logs.go`, `executor logs` | each rank is read through `/getLogs` with `Follow` instead of from the plugin's files (`executor_logs.go:228-243`), so no shared filesystem is needed. The followed stream ends when every rank's stream ended and `/status` shows their containers terminated: the API server ends a stream it lost from the plugin as if it were complete (il `api/handler.go:242-245`). The 10 s quiet wait goes with the file reads; the plugin's `Follow` ends after the container's last write |
| `executor_config.go`: Executor, Shadow and Staging blocks (`:14-160`) | `pkg/executor/config.go` | `SiteConfig` (`:55-61`) keeps `AllowedAccounts`. `LoginHost`, `User`, `Port`, `KeyFile` and `exportSiteEnv` (`:309`) stay in the plugin for its ssh shims, as an optional `Sites` block of at most one entry (slurm `pkg/slurm/site.go`), so the Shadow block names its login node and user itself and Staging gains `User` (default `Shadow.User`). The plugin's configuration sets `GangSchedulingEnabled` itself: the executor no longer turns it on (`:224-228`) |
| `pkg/shadow` | `pkg/shadow` | none: it is the virtual kubelet's SSH shadow (slurm `pkg/shadow/render.go:17`) |

Two checks move as well. `validatePodSetsShareAllocation` (`executor.go:336-361`) checks one
partition and QOS for all PodSets; that half becomes the controller's rule of one flavor per
Workload (`kueue-native.md` 3.1, `pkg/offload/podsets.go:138`). The executor still refuses a pod
group whose PodSets' expanded annotations differ (`pkg/executor/server.go:247`), for a caller other
than the controller. The name pattern for accounts and queues (`slurmNamePattern`,
`:163`) moves to the plugin, which parses its own flags and answers 400. The launcher half (one
image and command when a site launcher starts every rank, `:351-358`) stays in the executor. The
plugin cannot apply it: a launcher starts the head's command on every rank by design (slurm
`gang.go`, `emitSiteLauncher`), and virtual kubelet gangs whose head and workers run different
commands, such as a Ray cluster, rely on that.

### 1.3 What stays in a plugin

- Rendering a pod as a job: the job script, resource translation, runtime and probes (slurm
  `prepare.go`, `Create.go`).
- A gang as one allocation: the buffer that turns N group members into one `sbatch --nodes=N`
  (slurm `gang.go:791`), rank placement and site launchers (`launcher.go`).
- Queue and account flags, read from the plugin's own annotation (`slurm-job.vk.io/flags`, slurm
  `prepare.go:1189`).
- Detecting submit limits and an unreachable site: the stderr patterns now in the executor
  (`executor.go:456`) move to `Create.go`, which answers 503 (3.4).
- The transport to the site (ssh shims, sshfs), job directories and their cleanup.

### 1.4 How the executor drives a plugin

Submit. The executor sends one `POST /create` per member, each with a PodCreateRequests, and keeps
the `PodJID` of each answer. The job ID is the last non-empty one, as today
(`executor.go:420-430`); for independent pods (3.6) every member must name its job, and the job ID
lists them (`pkg/executor/server.go:367-368`). If a call fails, it sends `POST /delete` for the
members created so far, which also drops buffered gang members (slurm `Delete.go:83`). A plugin
that lists `podset-create` gets a pod group's members in one `POST /createPodSets` instead (3.3).

Status. The executor sends one `GET /status` with every member (name, namespace, UID, containers)
and sets `status.phase` to `Pending` or `Running` on each one. The phase is required: the API
server asks the plugin again only for pods that are running, pending or not yet cached (il
`pkg/interlink/api/status.go:68-72`). A member sent without a phase would get its first cached
answer forever. `rankState` (`executor.go:782-810`) maps each PodStatus to a rank unchanged, and
the rules of 3.5 replace `squeue`.

Delete and logs. `POST /delete` and `GET /getLogs` are sent once per member.

Member UIDs. The API server takes only UUIDs as pod UIDs in `/getLogs` (il `api/logs.go:30`,
`:39`), so member i's UID is a name-based UUID of the Workload UID and i, not `<workloadUID>-<i>`.

Data. The executor resolves `env[].valueFrom` and `envFrom` before `/create`, as the virtual
kubelet does (il `pkg/virtualkubelet/execute.go:1048`, `:1168`): the API server gives each container
only the Secrets, ConfigMaps and projected volumes it mounts (il `api/func.go:76`).

Health. `/healthz` is the API server's `/pinglink`, which reaches the plugin.

### 1.5 Kueue facts as plugin inputs

The controller knows the account, the flavor and the wall time. The plugin needs them in its own
annotation syntax. Each OffloadConfig flavor gets `annotations` in place of `partition` and `qos`
(`api/v1alpha1/offloadconfig_types.go:76-84`). The values are Go text/templates, as the membership
executor template already is, and the controller expands them with `.Account` (the
ClusterQueue's label `hpc.interlink.eu/account`, `pkg/offload/workload.go:300`), `.MaxExecSeconds` (the Workload's
`spec.maximumExecutionTimeSeconds`, `0.16.9 apis/kueue/v1beta2/workload_types.go:77`, 0 when
unset) and `.MaxExecMinutes` (the same, rounded up).

```yaml
spec:
  flavors:
    hpc-a-debug:
      annotations:
        slurm-job.vk.io/flags: "--account={{.Account}} --qos=debug --time={{or .MaxExecMinutes 30}}"
    hpc-a-prod:
      annotations:
        slurm-job.vk.io/flags: "--account={{.Account}} --qos=prod{{with .MaxExecMinutes}} --time={{.}}{{end}}"
```

The executor appends each value after the template's own value for that key, separated by a space,
as `buildMember` does for the flags today (`executor_members.go:110`). The site's options therefore
come last. The executor still checks the account against `AllowedAccounts` (`executor.go:296`),
and the site's associations remain the final check. The executor never parses the values. A
PodSet none of whose admitted flavors is listed is rejected, since its job would carry no account.

The OffloadRequest's PodSets carry `annotations` (`api/v1alpha1/offloadrequest_types.go`) where
the executor's former API had `partition` and `qos`. The spec holds the expanded annotations of
each PodSet, so `kubectl get offloadrequest -o yaml` shows the exact flags sent.

## 2. The plugin contract as it is

| Call | Request | Answer |
| --- | --- | --- |
| `POST /create` | one RetrievedPodData: the pod plus each container's ConfigMaps, Secrets and projected volumes (il `pkg/interlink/types.go:89-98`). The API server builds it from PodCreateRequests (`types.go:15-30`, `api/create.go:74`, `:166-176`) | CreateStruct `{PodUID, PodJID}` (`types.go:60-65`) |
| `GET /status` | a list of pods (`api/status.go:46-54`) | a list of PodStatus: name, UID, namespace, JID, `nodeName`, containers, initContainers (`types.go:36-55`) |
| `GET /getLogs` | LogStruct: namespace, pod UID and name, container, and Opts (Tail, LimitBytes, Follow, SinceSeconds, SinceTime) (`types.go:103-136`) | text, streamed with Follow (`api/logs.go:152`) |
| `POST /delete` | one pod (`api/delete.go:57`) | status code |
| ping: `GET /status` with `[]` | sent by `/pinglink` (`api/ping.go:32-54`) | PingResponse `{status, resources, taints}` (`types.go:142-151`) |

- `/create` has taken a single object since interLink 0.3.0 (commit f0754cbd). The list body
  belongs to `/status`. The HTCondor plugin records the change (htc `handles.py:1462`).
- `nodeName` is upstream (`types.go:46-50`). `socketDir`, which the reverse tunnel needs, exists
  only in the interLink fork that the Slurm plugin branch replaces interLink with (slurm
  `go.mod:72`). The API server re-encodes statuses from its own PodStatus type
  (`api/status.go:106-131`), so it must be built from an interLink that has the field.
- Errors. The API server forwards the plugin's status code (`api/handler.go:145`), but replaces
  the body with HTML-escaped text that wraps it (`:170-176`) and forwards no headers. Only the
  status code is a reliable signal.
- Identity. The API server sends the plugin only `Content-Type` and the session header
  (`api/handler.go:116-122`).
- Cache. The API server keeps a pod's last status until `/delete` (`api/status.go:115-129`): a pod
  the plugin stops reporting keeps its cached status, so a rank is seen missing only when the
  plugin never reported it.

### 2.1 The HTCondor plugin

| Piece | What it does | What kueue-offload lacks |
| --- | --- | --- |
| `/create` | single object (htc `handles.py:1454-1480`); one `condor_submit`, `Queue 1` (`:1274`) | every failure is 500 (`:1794`), with no transient or permanent distinction. Wall time is fixed at `+MaxWallTimeMins = 60` (`:1270`). No annotation for an accounting group or extra submit lines |
| `/status` | `condor_q`, then `condor_history` (`:1889-1899`). Idle maps to waiting, Running to running, Completed to terminated with `ExitCode`, Removed to terminated with reason `Cancelled`, Held to waiting `JobHeld` (`:1919-1952`) | no `nodeName` (`:1969-1977`). Every container gets the job's state and exit code. Removed has exit code 0. A job found in neither queue nor history is left out of the answer (`:1897-1899`). `initContainers` is always empty (`:1976`) |
| `/getLogs` | `condor_tail` while the job runs, the transferred file afterwards (`:2048-2138`) | Tail only: no Follow, no SinceSeconds |
| `/delete` | `condor_rm` (`:1385-1396`) | |
| ping | `/status` with `[]` answers `{status: healthy}` (`:1843-1853`) | no capabilities |
| gangs | none: one job per pod | no parallel universe |
| tests | the `htcondor/mini` pool with the plugin in one container (`docker/Dockerfile:1`), driven by a k3s e2e in CI (`.github/workflows/integration-test-k3s.yaml`) | |

## 3. Capabilities and refusals

### 3.1 Advertising

PingResponse gains a `capabilities` field, a list of strings. A plugin returns it on the ping path,
which is `/status` with an empty list (slurm `pkg/slurm/Status.go:110`, htc `handles.py:1843`).
`/pinglink` streams that answer unchanged (il `api/ping.go:54`). The executor asks at start and
then every few minutes, and at the submit of a gang that comes before the first answer
(`pkg/executor/capabilities.go:62`, `:79`); a missing field means no capabilities. Go's JSON
decoder ignores unknown fields, so older virtual kubelets are not affected. Upstream this is one
field in `pkg/interlink/types.go`. Until it lands, the executor and the plugin decode their own
copy of the type; the Slurm plugin already carries one (slurm `pkg/slurm/types.go:302-309`).

### 3.2 `pod-group`: a gang as one allocation over per-pod /create

A plugin that lists `pod-group` makes this promise. N `/create` calls whose pods carry the same
`kueue.x-k8s.io/pod-group-name` label and the annotation `kueue.x-k8s.io/pod-group-total-count: "N"`
(Kueue's keys, `0.16.9 pkg/controller/jobs/pod/constants/constants.go:27-28`) run as one job. Each
pod's rank is its `batch.kubernetes.io/job-completion-index` label, and rank 0 is the head. Until
the last member arrives, the plugin answers with an empty `PodJID`; after that it answers with the
shared job ID.

The Slurm plugin's gang buffer already does exactly this (slurm `gang.go:46-59`, `:149-151`,
`:791`, `Create.go:337-362`), and `buildMember` already writes these keys
(`executor_members.go:99-109`). No new endpoint, type or annotation is needed. From step 1 the
Slurm plugin lists `pod-group` when `GangSchedulingEnabled` is set (slurm `types.go:123`). The
executor sends all members of a Workload in one pass and in rank order, as today
(`executor.go:421-430`). A Workload with several PodSets, such as a JobSet's leader and workers,
forms one group whose ranks are numbered across the PodSets.

### 3.3 `podset-create`: a pod group in one call

Issue 5 asks for the PodSet itself to reach the plugin. A plugin that lists `podset-create` takes
the members of a pod group in one call, `POST /createPodSets`, and submits them as one job without
buffering them. The executor sends:

```go
type PodSetsCreateRequests struct {
	GroupUID string                 `json:"groupUID"` // the Workload UID, the pod-group label value
	PodSets  []PodSetCreateRequests `json:"podSets"`
}

type PodSetCreateRequests struct {
	Name string              `json:"name"`
	Pods []PodCreateRequests `json:"pods"` // each member as /create takes it
}
```

Ranks follow the order of the pods, PodSet after PodSet, and rank 0 is the head. Each rank is a
whole pod, not a template and a count: its projected and downward API files differ (pod name, UID,
rank annotations), and the plugin keeps its per-pod translation. The API server prepares each pod
as for `/create` (`getData`, then the job script step) and sends the plugin the same shape with
`RetrievedPodData` in place of `PodCreateRequests`. It relays the answer unchanged: 200 with a
`[]CreateStruct`, one entry per pod in request order, every `PodJID` the group's job ID; 400 for a
bad request or a permanent refusal and 503 for a transient one, as `/create` (3.4). Status, logs
and delete stay per pod, so the shadow and the log relay do not change.

- interLink: the types and the route are on the fork's branch `feat/create-podsets`, an upstream
  proposal. Until they land, the executor carries its own copy of the types
  (`pkg/executor/interlink.go:114`).
- Slurm plugin: the handler renders each pod as `/create` does and submits the group as the one
  `sbatch --nodes=N` job of the gang path, with no buffer and no timeout sweep. A retried call
  whose pods already share a job gets that job again. It lists `podset-create` next to `pod-group`
  (slurm at 0238623, `pkg/slurm/podsets.go:52`, `:96`, `Status.go:742`).
- Executor: a pod group on a plugin that lists `podset-create` goes in one `/createPodSets`
  (`pkg/executor/capabilities.go:121`, `server.go:415`). A failed call, or an answer that does not
  list every member once in rank order with one job ID, deletes the members as a failed `/create`
  does. A plugin with only `pod-group`, independent pods and a single pod keep the per-pod
  `/create` path. So does a pod group whose `/createPodSets` answers 404: the ping that lists
  `podset-create` passes through any API server, and one without the route submitted nothing.

It saves N round trips that each carry the Workload's Secrets, and the wait for the last member.

### 3.4 Transient and permanent refusals

| `/create` answer | Meaning | Executor and check |
| --- | --- | --- |
| 200 | accepted; `PodJID` may be empty for a buffered member | `Pending` |
| 503 | try later: a submit limit, or the scheduler or site cannot be reached | `Pending` with the plugin's text and the quota held. For a gang the executor deletes the members created so far and submits again at the next poll; for independent pods the members the site took stay and the rest are created at later polls, so a per-user job limit runs them one after the other |
| 400, 403, 422 | permanent: a refused account, an invalid request or an unsupported shape | `Rejected` |
| another 5xx, or no answer | transient | `Pending`, submitted again every `pollInterval`; `Retry` after `executorLostTimeout` |

The executor writes a heartbeat on the request for the second row and none for the last, so the
controller tells them apart. The status code survives the API server (section 2). The virtual kubelet treats every non-200
answer the same way (il `pkg/virtualkubelet/execute.go:454`), so this convention changes nothing
for it. Today the Slurm plugin answers 504 to every sbatch failure (slurm `Create.go:348`,
`:371`). Step 1 makes it classify failures with the patterns the executor uses now.

### 3.5 Job state from /status alone

Without `squeue`, the executor derives the job's state only from `/status`:
- queued: no rank runs; the first waiting reason and message become the check message.
- running: a container runs or `nodeName` is set (`rankState`, `executor.go:782-810`).
- cancelled outside: a container terminated with reason `Cancelled`, whatever its exit code. The
  HTCondor plugin already reports this (htc `handles.py:1935-1943`). The check goes to `Retry`.
- vanished: a submitted rank stays missing from the answer longer than the executor's
  `Executor.LostTimeout` (default 10 minutes, as `executorLostTimeout`; the executor does not read
  the OffloadConfig). The HTCondor plugin leaves unknown jobs out (`:1897-1899`). The check goes
  to `Retry`.
- completed or failed: every rank terminated, decided by exit code. Reasons such as the time
  limit (`SlurmJobTimeout`, slurm `Status.go:36`) are copied into the message.

The Slurm plugin's state pattern has no `CA` (slurm `Status.go:47`), and today the pending reason
comes from the executor's own `squeue` call (`executor.go:516-548`). Step 1 moves both into the
plugin's `/status`.

### 3.6 Workload shapes

`independent-pods` is the owner annotation `offload.interlink.eu/independent-pods: "true"`. It is
forwarded like the owner's other annotations (`pkg/offload/podsets.go:21`, `:53`), and the executor
reads it on every PodSet's template (`pkg/executor/capabilities.go:33`).

| Shape (Workload PodSets) | Plugin with `pod-group` | Plugin without it |
| --- | --- | --- |
| single pod, or a Job with parallelism 1 (one PodSet, count 1) | one job | one job |
| Job with parallelism N (one PodSet, count N) | one allocation of N ranks | refused; N jobs with `independent-pods` |
| pod group (k PodSets, N pods) | one allocation | refused; N jobs with `independent-pods` |
| JobSet (one PodSet per replicated job) | one allocation, ranks numbered across PodSets | refused; N jobs with `independent-pods` |
| TrainJob (one PodSet per role) | one allocation | refused; N jobs with `independent-pods` |
| any shape with `independent-pods` | N jobs | N jobs |

Counts come from the admitted `PodSetAssignment`, so partial admission applies
(`pkg/offload/podsets.go:40-45`). A refusal is `Rejected` with a message naming the missing
capability, so the project can pin a flavor on another site. Refusing by default prevents a gang
whose ranks start hours apart from burning allocations while they wait for each other.
`kueue-native.md` 3.6 says that a backend without the capability refuses any count above 1; the
table above replaces that rule.

## 4. Edge: one plugin for many users

The API server and the plugin run at the site, behind an authenticating proxy, as in interLink's
edge mode (il `docs/docs/cookbook/1-edge.mdx:40`). There is still one executor per tenant
namespace, because everything it reads and writes is namespaced: Secrets, ConfigMaps,
TokenRequests, stage Jobs and shadows. Only its API server URL and credential change. The
membership controller renders both per namespace through the executor template
(`config/samples/membership/executor-template.yaml`).

The identity travels with the namespace: the controller creates the OffloadRequest in the
Workload's namespace, that namespace's executor calls the API server with the namespace's
credential, the site's proxy maps the credential to a site user, and the plugin runs the job as
that user. The credential belongs to the tenant: a projected ServiceAccount token with the site as
audience, which the proxy validates against the cluster's issuer, or a per-user OIDC client
credential in the tenant's Secret. A cluster-wide caller that names the user is ruled out
(`kueue-native.md` 3.7).

Three pieces are missing upstream for a shared plugin:
- the API server passes no caller identity to the plugin (il `api/handler.go:116-122`);
- it does not scope calls by caller: `/status` with `[]` returns every cached status
  (`api/status.go:120-135`) and `/delete` acts on any pod. It must bind each pod UID to its caller;
- a plugin that submits as the mapped user (sudo, a setuid helper, or the scheduler's REST API
  with a per-user token). Neither the Slurm nor the HTCondor plugin can.

Without them the edge works per user: one API server and plugin under each user's account, as the
edge cookbook deploys them (`1-edge.mdx:174-185`), each at its own URL in that user's executor
configuration. The executor code is the same in both cases.

Quotas do not change: Kueue admits the Workload (project ClusterQueue and flavors) before an
OffloadRequest exists, the account reaches the plugin in the flavor annotation (`.Account`), and
the site checks that the user may charge it. The plugin knows nothing about quotas.

Logs work. The executor relays `/getLogs` with Follow through the API server, which streams it
(`api/logs.go:152`), and the shadow's job-logs container reads from the executor (slurm
`pkg/shadow/render.go:165-171`). No shared filesystem is involved.

Tunnels work only where the tenant has an SSH key for the site. The shadow reaches the node over
SSH with a key in the tenant namespace (`executor-template.yaml:60-70`); with such a key, forward
and reverse tunnels work as they do in-cluster. Often, though, the edge exists because the cluster
holds no SSH identity for the user. Tunnels then need a stream into the job from the API server,
keyed by pod, which interLink does not have. Logs-only shadows still work.

## 5. The API server: always in front

Recommendation: option 3 of the issue. The API server stands in front of every plugin, and the
executor calls the API that the virtual kubelet already uses. The executor then has one contract
and no plugin-facing code: `getData`, ping forwarding, log streaming, and TLS and mTLS (il
`pkg/interlink/config.go:117-145`) are the API server's job, and in-cluster and edge deployments
differ only in URL and credential.

The cost is one stock container in the tenant pod, the same layout interLink's own in-cluster
deployment uses. Option 1 (the executor's API as the only contract) makes every backend
reimplement that API, which issue 1 rules out. Option 2 (the API server only at the edge) gives
the executor two create bodies (RetrievedPodData for a plugin, PodCreateRequests for an API
server) and keeps its own copy of `getData` (`executor_members.go:597`).

Because of the API server, the executor must send a phase on the pods it asks `/status` about
(1.4), and it can read refusals only from the status code (section 2). Running the API server's
handlers inside the executor binary would save the container, but it would tie the executor to
the API server's releases and to its `log.Fatal` paths (il `api/status.go:51`, `:87`, `:93`).

## 6. HTCondor

How each shape runs through the HTCondor plugin:
- A single pod is one HTCondor job (`Queue 1`) that runs the containers with singularity (htc
  `handles.py:972`). RequestCpus, RequestMemory and RequestGpus come from the limits
  (`:1070-1081`); the executor's `requestsAsLimits` (`executor_members.go:175`) gives limits to a
  template that sets only requests.
- A Job with parallelism N and `independent-pods` runs as N jobs, with status per rank.
- Gangs are refused, because the plugin does not list `pod-group`. Adding `pod-group` for
  HTCondor would need the parallel universe and a dedicated scheduler on the pool.
- The account and the wall time need a plugin change: an annotation whose submit description
  lines are appended before `Queue`, after the plugin's own `+MaxWallTimeMins = 60`, so the
  flavor's value wins (htc branch `feat/submit-lines`, `_submit_lines`), filled per flavor:

```yaml
    hpc-a-prod:
      annotations:
        htcondor-job.vk.io/submit-lines: |
          accounting_group = group_{{.Account}}
          +MaxWallTimeMins = {{or .MaxExecMinutes 60}}
```

What is refused or limited:
- Tunnels. The plugin reports no `nodeName`, and execute nodes are generally not reachable over
  SSH. Where the site's shadow configuration has no login host, the executor refuses
  `interlink.eu/reverse-forwards` and exposed ports and runs a logs-only shadow
  (`pkg/executor/server.go:220`). A shadow that tunnels waits for `nodeName`; one that only streams
  logs is created as soon as the job runs, whatever the backend (`pkg/executor/server.go:801`).
- Staging. `Staging.Command` is the site's configuration; HTCondor's own `transfer_input_files`
  (htc `handles.py:1234`) belongs to the plugin.
- Logs. Without Follow, each `/getLogs` answers the log so far. While the job runs the executor
  asks again every 5 s and sends only the lines it has not sent yet (`followRank` in
  `pkg/executor/logs.go`); once the job ended it reads the log once more, and the stream ends.
- Exit codes. The plugin reports reason `Completed` whatever the exit code (htc
  `handles.py:1931`), so a failed job reads `rank 0 exited with code 3 (Completed)`.
- Init containers run inside the job script, but their status is not reported.
- A job removed outside (`condor_rm`) goes to `Retry` through reason `Cancelled` (3.5), although
  its exit code is 0. A held job stays queued until the maximum queue time ends it.

The plugin changes go upstream in interlink-htcondor-plugin: the submit-lines annotation; 503
when `condor_submit` cannot reach the schedd and 400 for a refused submit; Follow in `/getLogs`;
`capabilities` in the ping answer; and `nodeName` from `RemoteHost`, needed only for tunnels on
pools whose nodes can be reached.

The tests need no HTCondor pool:
1. A fake plugin in Go (`pkg/executor/fakeplugin_test.go`), an httptest server with `/create`,
   `/status`, `/getLogs`, `/delete` and the ping. Each member is scripted: 503 N times, 400, queued
   with a reason, running with a node, terminated with exit codes, `Cancelled`, missing. Its
   capabilities can be switched on or off. Table tests cover every case of 3.4 to 3.6 and assert
   the check state and message, the deletion of members after a failure, and the OffloadRequest
   status.
2. The same tables through a real API server, built from il `cmd/interlink` and started by the
   test as a subprocess pointing at the fake plugin. These cover the status cache, the error
   wrapping and log streaming.
3. The HTCondor plugin's own container, a one-node `htcondor/mini` pool with the plugin (htc
   `docker/Dockerfile:1`), which the plugin's CI already runs. `test/htcondor/run.sh` runs Kueue,
   the offload controller, the executor, a stock API server and that container in kind through
   these cases: a single pod and its log on the shadow, the account and the wall time in the
   submit file, a Job of 3 with `independent-pods`, a refused Job of 2 pods (a pod group of any
   kind is refused by the same check), an exit code 3, and a `condor_rm` from inside the pool.
4. Multi-node runs, accounting enforcement and a site's authentication need a real pool.

## 7. Reuse

| Piece | Taken from | Instead of |
| --- | --- | --- |
| wire types: PodCreateRequests, PodStatus, CreateStruct, LogStruct, PingResponse | il `pkg/interlink/types.go` | copies in `pkg/executorapi` and the Slurm plugin's local PingResponse |
| per-container data split, ping forwarding, log streaming, TLS and mTLS, tracing | the stock API server | executor code (`retrievedPodData`) |
| SSH shadow template | il `pkg/virtualkubelet/templates/shadow-ssh-template.yaml`, embedded unexported (`virtualkubelet.go:47-48`) | the copy in `pkg/shadow` (slurm `render.go:17-20`); exporting it upstream is one line |
| group, size and rank keys | Kueue's pod group label and annotation (`0.16.9 pkg/controller/jobs/pod/constants/constants.go:27-28`) and Kubernetes' completion index | `interlink.eu/gang-*` |
| wall time | `spec.maximumExecutionTimeSeconds` (`0.16.9 apis/kueue/v1beta2/workload_types.go:77`) | `--time` in users' flags |
| retry delays | `ProvisioningRequestRetryStrategy` and Kueue's helper (`kueue-native.md` 3.2) | `maxRetries` |
| test objects | `0.16.9 pkg/util/testing/v1beta2/wrappers.go:65`, `:421` | hand-built Workloads |

The virtual kubelet's request building cannot be reused as it is. `remoteExecutionHandleVolumes`
and `resolveEnvRefs` (il `pkg/virtualkubelet/execute.go:906`, `:1048`) are unexported and bound to
its Provider. An exported builder from a pod and a reader of its references would let the executor
drop the env and volume code of `executor_members.go`. It is a candidate interLink change after
step 5.

## 8. Implementation plan

Every step ends with the reference deployment running every Slurm feature: gangs as one
allocation over the high-speed network (two or more nodes, with the interconnect proven from the
logs), staging, reverse tunnels, notebooks with a forward tunnel, logs, and several queues per
project. "Slurm e2e" below means that set of tests.

1. **The Slurm plugin follows the conventions.** `Create.go` answers 503 or 400 using the patterns
   of `executor.go:456` and the ssh exit code 255. `/status` reports `CA` as `Cancelled`, and `CF`
   and `PD` as waiting with squeue's reason. The ping lists `pod-group` when
   `GangSchedulingEnabled` is set. The in-process executor does not change. Test: plugin unit
   tests per sbatch stderr and squeue state; the virtual kubelet path is unaffected.
2. **The executor drives the plugin over HTTP, still inside the plugin binary.** Contract calls to
   the plugin's own listener replace `createPod`, `podStatuses`, `squeue`, `scancel` and the file
   reads, and the job state follows 3.5. Test: executor tests against the fake plugin; Slurm e2e.
3. **Per-flavor annotations.** OffloadConfig flavors carry templated annotations, the controller
   expands them, the executor API's PodSet carries them, and `buildMember` stops writing Slurm
   flags. The wall time comes from `maximumExecutionTimeSeconds`. The reference OffloadConfig is
   rewritten in the same change. Test: a golden test that each flavor renders the same `#SBATCH`
   lines as before; Slurm e2e on both queues.
4. **The executor moves to this repository.** It becomes `cmd/executor`, `pkg/executor` and
   `pkg/shadow`. The image gains `/executor`, and the executor template renders it next to the
   plugin in the tenant pod. The plugin loses `executor*.go`, `pkg/shadow` and
   `Executor.Enabled`. Test: the moved unit tests; Slurm e2e.
5. **The API server goes in front.** A stock interLink API server container sits between the
   executor and the plugin; it is built with `PodStatus.SocketDir` until upstream has the field.
   The executor sends PodCreateRequests and pod phases, and `retrievedPodData`'s per-container
   split goes. Test: the fake plugin through a real API server (section 6, test 2); Slurm e2e.
6. **Capabilities and shapes.** The executor reads the capabilities and applies the shape table
   and `independent-pods`. The controller rejects a Workload whose PodSets are on different
   flavors. Test: the table tests with both capability settings; Slurm e2e with a gang.
7. **OffloadRequest.** Done: `kueue-native.md` section 6, step 6. The executor reconciles
   OffloadRequests (`pkg/executor/request.go`), the controller creates them (`pkg/offload`), and
   `config/admission` limits who writes them. Test: the unit tests of both sides and of the
   policy (`pkg/offload/policy_test.go`); Slurm e2e.
8. **HTCondor.** The plugin changes of section 6 go upstream, and the executor gets the logs-only
   shadow without a node and the tunnel refusal. Test: e2e with the `htcondor/mini` container in
   kind.
9. **Edge, per user.** The executor is configured with a remote API server URL and a token, which
   the membership controller renders. One API server and plugin run per user on a test login host
   or in the mini container. Open an upstream issue for identity forwarding and per-caller scoping
   (section 4).
10. **`podset-create`.** Done: the Slurm plugin's `/createPodSets` and the executor's choice by
    capability (3.3). The API server route is an upstream interLink proposal on the fork's branch
    `feat/create-podsets`. Test: executor tests against the fake plugin, with and without the
    capability; plugin unit tests.
