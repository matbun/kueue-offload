// Package executorapi holds the names the executor shares with the offload
// controller and the interLink API server. The offload controller and the
// executor talk through OffloadRequests; the executor's own HTTP API is GET
// /getLogs, which the shadows read, and GET /healthz.
package executorapi

// SessionHeader carries the Workload UID on every call to the interLink API
// server, for tracing.
const SessionHeader = "InterLink-Http-Session"

// IndependentPodsAnnotation, set to "true" on the owner, runs a Workload of several
// pods as one job per pod instead of one allocation. The offload controller
// forwards it in the OffloadRequest's ownerAnnotations like the owner's other
// annotations.
const IndependentPodsAnnotation = "offload.interlink.eu/independent-pods"

// ServiceAccountTokenAnnotation, set to "true" on the owner, keeps the token of the
// API server's own kube-api-access-* volume, which a plain pod carries, for a job
// that calls the cluster's API (a pipeline step's launcher). Without it, and unless
// the executor's AutomountServiceAccountToken is set, that token never leaves the
// cluster.
const ServiceAccountTokenAnnotation = "offload.interlink.eu/service-account-token"
