# kueue-offload in the Kueue landscape

Where kueue-offload stands against what Kueue, MultiKueue and related projects offer for running
Kubernetes workloads on sites without a Kubernetes API, and the path to bring it upstream without
losing any of the requirements R1 to R5 of [kueue-native.md](kueue-native.md). Read on
2026-10-30: Kueue `main` at commit `c75a7f3f6e0e` and release 0.20.4, the GitHub issues and pull
requests linked below, the WG Batch notes, and each project's own documentation. Quotes are
exact. Paths with a version prefix are in the Kueue repository at that tag.

## In short

1. Nothing upstream, merged or in review, lets MultiKueue use a worker that has no Kubernetes API.
   Kueue `main` adds one KEP over 0.20.4 (`16029-metrics-access-control`), and its MultiKueue API
   is identical to 0.20.4. The WG Batch notes from 2022 to 2026 have no item on it.
2. The maintainers want the neighbouring feature, an External MultiKueue mode in which an external
   controller syncs the job: "we are open to the follow up extending MK for external controllers.
   This requires a KEP update" ([#6117](https://github.com/kubernetes-sigs/kueue/issues/6117),
   2025-10-15). It is not started.
3. Users in that issue keep private Kueue forks for the external half: "Forked kueue and modified
   the admission controller" (2025-08-20) and "I am maintaining a fork of kueue with my own
   MultiKueueAdapter implementation" (2025-10-14). They are the second users a KEP needs.
4. Every related project either runs pods on Kubernetes nodes at the site, replaces Kueue, or
   reaches the site through a virtual node. None plugs into Kueue's admission with no kubelet at
   the site, which is kueue-offload's shape.
5. Two open, prioritized Kueue issues match kueue-offload requirements:
   [#8987](https://github.com/kubernetes-sigs/kueue/issues/8987) (a MultiKueue check per flavor,
   needed for R5) and [#3526](https://github.com/kubernetes-sigs/kueue/issues/3526) (logs of
   remote workloads on the manager).

## Kueue issues, KEPs and pull requests that bear on this

| Item | State | What it says | Bearing on kueue-offload |
| --- | --- | --- | --- |
| KEP-2349 "MultiKueue external custom Job support" ([#5981](https://github.com/kubernetes-sigs/kueue/pull/5981), beta in [#7669](https://github.com/kubernetes-sigs/kueue/pull/7669)) | implementable, beta | "Supporting external controllers can be revisited in the future if there is a strong demand for this functionality." Beta criterion: "Re-evaluate the need for ControllerType field under External Frameworks struct" | the External mode is an open beta item: the entry point |
| [#6117](https://github.com/kubernetes-sigs/kueue/issues/6117) "Make MultiKueue Admission Check Controller Reusable by External Frameworks" | open, `priority/important-longterm`, `lifecycle/rotten` | "Ideally, I would like to have a design where the AC is decoupled from Job createtion and syncing. So there is only one MultiKueue admission check, and it orchestrates the prebuilt workloads." Later: "one idea we discussed in the past was to introduce a new `controllerType: External`" | the maintainers' design: one MultiKueue check, sync done outside. It is keyed by job kind, not by worker kind |
| [#8252](https://github.com/kubernetes-sigs/kueue/issues/8252) | closed 2025-12-16 | "in the future we are planning to expand that configuration to introduce external controllers for syncing MultiKueue Jobs" | the same plan, confirmed |
| [#8987](https://github.com/kubernetes-sigs/kueue/issues/8987) "Can "Cannot specify MultiKueue AdmissionCheck per flavor" constraint be relaxed?" | open, `priority/important-longterm` | "it is not that easy because the 'managedBy' field on a Job is immutable, and this field is injected at the job creation time based on the targeted CQ." Option 1: "instead of using the managedBy field use scheduling gates which are injected at admission time". "if you need it then I would suggest proposing the topic for wg-batch" | R5 under MultiKueue depends on it; kueue-offload's own check already works with scheduling gates and per-flavor checks |
| [#3526](https://github.com/kubernetes-sigs/kueue/issues/3526) "Support log retrieval on MultiKueue" | open, `priority/important-soon`, `lifecycle/rotten` | "Note that this needs for KEP, first." The author, 2025-12-11: "We're still interested in this feature and can commit some resources to get it implemented." | logs of remote jobs, which kueue-offload serves through the shadow today |
| [#5704](https://github.com/kubernetes-sigs/kueue/issues/5704) "MultiKueue: Support cluster role sharing (worker and manager inside one cluster)" | open | "Copying the job with a different name. This ensures that we can copy to the manager without mutating `managedBy` field." | the same `managedBy` problem as #8987 |
| [#17560](https://github.com/kubernetes-sigs/kueue/pull/17560) (KEP-17401) | open | "A built-in MultiKueue CapacityProvider sums the quotas of the related worker ClusterQueues" | could replace the membership controller's `ProjectQuota` |
| [#16635](https://github.com/kubernetes-sigs/kueue/pull/16635) (KEP-16471) | open, "Targeted at v0.21" | "copies the listed annotations from the job object into the Workload at creation time for every integration" | could replace kueue-offload's forwarding of owner annotations |
| [#14540](https://github.com/kubernetes-sigs/kueue/issues/14540), [#14539](https://github.com/kubernetes-sigs/kueue/pull/14539) | open | a per-Workload `kueue.x-k8s.io/multikueue-cluster-names` annotation, alpha, gate `MultiKueueClusterNames` | MultiKueue still has no flavor-to-worker binding; kueue-offload keeps `OffloadConfig.flavors` |
| [#2405](https://github.com/kubernetes-sigs/kueue/pull/2405), [#2458](https://github.com/kubernetes-sigs/kueue/pull/2458) | closed June 2024 | KEP-2349's alternative 3 rejects custom check controllers: "The external admission checks would need to largely repeat the code of the MultiKueue built-in controller" | the argument kueue-offload must answer for its own check |

Searches of the Kueue repository for "non-Kubernetes", "interlink", "slinky", "HTCondor",
"armada", "external worker" and "virtual kubelet" returned nothing relevant beyond the above.
The "Slurm" hits are kjob's Slurm mode and feature requests that cite Slurm as a reference.

WG Batch notes (the group's meeting document, August 2022 to November 2026) list "MulitiKueue for
external" among the Kueue "Higher Priority" items (2025-09-11), and record that "People coming
from Slurm world get confused by Kueue" (2026-05-21). Topics are proposed "with a specific date by
leaving a comment on the next page of this document"; meetings alternate between "Kueue
Prioritized" and "Non-Kueue Prioritized" Thursdays.

## Related projects

| Project | What it does | Plugs into Kueue? | What it gives up |
| --- | --- | --- | --- |
| [Slinky slurm-bridge](https://github.com/SlinkyProject/slurm-bridge) | "Workloads submitted from Kubernetes will have their resource requirements translated into a representative Slurm job by `slurm-bridge`." "Upon resource allocation to a K8s workload by the Slurm controller, `slurm-bridge` will bind the workload's pod(s) to the allocated node(s)." | no: a scheduler plugin, described as the "Same approach taken by Kueue, Volcano, MPI Operator" | needs kubelet on the HPC nodes and assumes it is "the only workload scheduler" for them; a gang is one Slurm job, but its pods exist |
| Slinky slurm-operator | "Slurm Operator provides for a traditional HPC environment within an overarching Kubernetes system" | no | the site runs on Kubernetes |
| [CoreWeave SUNK](https://docs.coreweave.com/products/sunk/discover_sunk/scheduler.md) | "The scheduler places Pods on the same Kubernetes Nodes where the Slurm jobs run." | no | kubelet on the nodes |
| [Armada](https://github.com/armadaproject/armada) | "A multi-cluster batch queuing system for high-throughput workloads on Kubernetes." | no: it replaces Kueue's queueing | workers are Kubernetes clusters |
| [interLink](https://github.com/interlink-hq/interLink) | "an abstraction layer for executing Kubernetes pods on remote resources capable of managing container execution lifecycles" | indirectly: Kueue admits pods that a virtual node then runs | pods exist before the gang is decided, one job per pod, identity per virtual node. kueue-offload reuses interLink's plugins and API server below the admission check |
| [Volcano, volcano-global](https://github.com/volcano-sh/volcano-global) | "`Volcano-global` is a multiple kubernetes clusters scheduler" | no; Kueue declined Volcano API dependencies ([#1985](https://github.com/kubernetes-sigs/kueue/issues/1985)) | Kubernetes clusters only |
| Flux Operator | "Because a Flux MiniCluster runs as a `batch/Job`, Kueue does not require extra components to manage a Flux MiniCluster." (Kueue docs) | yes, as a Job | HPC software inside Kubernetes, no external site |
| [kjob](https://github.com/kubernetes-sigs/kjob) Slurm mode | "built-in support for running slurm scripts inside a Kubernetes cluster" | yes | the opposite direction |
| [SkyPilot](https://docs.skypilot.ai/en/latest/reference/slurm/index.html) | "Have SSH access to a Slurm login node? Launch your first SkyPilot task on Slurm" | no | its own scheduler, no Kubernetes quota |

## MultiKueue's extension points, and a worker without a Kubernetes API

| Extension point | What it allows | For a site without a Kubernetes API | Source |
| --- | --- | --- | --- |
| `MultiKueueCluster.spec.clusterSource` | exactly one of `kubeConfig` or `clusterProfileRef` | only behind a Kubernetes API facade; `MultiKueueClusterProfile` is alpha and off ([#17133](https://github.com/kubernetes-sigs/kueue/issues/17133) proposes beta) | `0.20.4 apis/kueue/v1beta2/multikueue_types.go:82-91`, unchanged on `main` |
| dispatchers | AllAtOnce, Incremental, and External, where "the selection of worker clusters is delegated to an external controller" | chooses among MultiKueueClusters only | `site/content/en/docs/concepts/multikueue.md` |
| `multiKueue.externalFrameworks` | custom job kinds by GVK; "a Custom Resource must have a `.spec.managedBy` field" | no: the axis is the job kind, and the remote is a Kubernetes client | `site/content/en/docs/tasks/run/multikueue/external-frameworks.md` |
| `MultiKueueAdapter` | `SyncJob(..., remoteClient client.Client, ...)`, adapters registered in the binary | a custom binary, a fork in practice | `0.20.4 pkg/controller/jobframework/multikueue.go:31-55` |
| the check controller and `managedBy` defaulting | both key on the literal `kueue.MultiKueueControllerName` | a third-party check cannot opt in | `pkg/controller/admissionchecks/multikueue/admissioncheck.go:55`, `pkg/controller/jobframework/defaults.go:143` on `main` |
| `cmd/experimental` | "integrations of resources with Kueue that are not part of the kueue binary"; "Users need to build the images from source" | a home for out-of-binary controllers, not a worker API | `cmd/experimental/README.md` |

Without forking Kueue, a worker without a Kubernetes API reaches MultiKueue only through an API
facade behind `kubeConfig` or `clusterProfileRef`, which would have to serve Workloads and every
job kind. Otherwise it stays a separate check, as kueue-offload is today.

## The closest shape

Present kueue-offload as the worker-kind half of the External mode the maintainers already want
(#6117, #8252, KEP-2349's beta item): their design has one MultiKueue check that dispatches and an
external controller that syncs; kueue-offload needs the same split on a second axis, a worker
that is external too. Section 5 of [kueue-native.md](kueue-native.md) has this shape
(`clusterSource.external` and `ExternalWorkload`); it should cite #6117 and the KEP-2349 beta
criterion, and use their word, "External".

| kueue-offload today | Closest Kueue shape | Risk to a requirement |
| --- | --- | --- |
| `OffloadRequest` | `ExternalWorkload`: the Workload's PodSets and `status.admission` in the spec, the remote Workload's condition types (`QuotaReserved`, `Admitted`, `Evicted`, `Finished`) in the status | none, as long as one object carries every PodSet (R2) |
| `OffloadWorker` | a `MultiKueueCluster` with an external source: cluster scoped, an `Active` condition | none |
| `OffloadConfig.flavors` | keep: MultiKueue has no flavor-to-worker binding | dropping it loses R5 |
| owner annotations forwarded by the controller | `integrations.annotationKeysToCopy`, if KEP-16471 lands | none |
| the membership controller's `ProjectQuota` | a `CapacityProvider` (KEP-12382, KEP-17401) | none; both alpha |
| the shadow's `/getLogs` | the HPC case of #3526 | keep the shadow until #3526 has a design |

Proposals that would lose a requirement:

| Proposal | What it loses |
| --- | --- |
| a Kubernetes API facade per site behind `kubeConfig` or `clusterProfileRef` | R3: MultiKueue keeps each credential "inside the namespace in which the kueue controller manager is running", so a facade per tenant moves every tenant key there (`0.20.4 apis/kueue/v1beta2/multikueue_types.go:60-62`) |
| #6117 as designed (a remote Workload on a worker cluster, an external controller creating the job there) | a Kueue cluster at every site, and pods there break R2 |
| MultiKueue's own check before #8987 is solved | R5: bursting between local and HPC flavors in one ClusterQueue |
| moving the whole project into `cmd/experimental` | interLink dependencies inside Kueue's repository, with no released images |
| notebooks through MultiKueue's StatefulSet path | "For job types without `managedBy` support (StatefulSet, LeaderWorkerSet), the job status on the manager cluster may not reflect the actual status from the worker cluster." kueue-offload keeps notebooks on the pod path ([design.md](design.md)) |

## Path upstream

1. Join the existing threads: a short HPC use case on #8987 (supporting the scheduling-gate option)
   and on #3526 (logs). Both are open and prioritized, and #8987 already points to WG Batch.
2. One feature issue, "MultiKueue: external workers without a Kubernetes API", framed as the
   worker-kind counterpart of the External mode in #6117 and of KEP-2349's beta item, linking
   #6117, #8987, #3526 and #5704, with a one-line pointer on #6117.
3. A WG Batch slot on a "Kueue Prioritized" Thursday, proposed as a comment in the notes document.
4. A KEP in two phases: (a) an update to KEP-2349, `controllerType: External`, where MultiKueue does
   not sync the job and the external controller writes the owner's status; (b) the external worker
   kind and `ExternalWorkload`, citing KEPs 693 and 2349. Phase (a) alone runs nothing for a site
   without a Kubernetes API, which the issue should say so that (b) is not dropped.
5. Meanwhile kueue-offload keeps its own admission check, which runs today on Kueue 0.16.9 and
   Kubernetes 1.30.
