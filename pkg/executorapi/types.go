// Package executorapi holds the wire types of the executor HTTP API
// (offload.interlink.eu/v1alpha1) shared by the offload controller and the executor.
package executorapi

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// APIVersion is carried by every request and response body.
const APIVersion = "offload.interlink.eu/v1alpha1"

// SessionHeader carries the Workload UID on every call, for tracing.
const SessionHeader = "InterLink-Http-Session"

// Job states reported by GET /jobs/{workloadUID}.
const (
	StateQueued    = "queued"
	StateRunning   = "running"
	StateCompleted = "completed"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
	StateRejected  = "rejected"
)

// OwnerReference identifies the object that owns the Workload.
type OwnerReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
}

// PodSet is one PodSet of the Workload, resolved to what the executor submits.
type PodSet struct {
	Name          string                 `json:"name"`
	Count         int32                  `json:"count"`
	Partition     string                 `json:"partition,omitempty"`
	QOS           string                 `json:"qos,omitempty"`
	Template      corev1.PodTemplateSpec `json:"template"`
	SecretRefs    []string               `json:"secretRefs,omitempty"`
	ConfigMapRefs []string               `json:"configMapRefs,omitempty"`
}

// SubmitRequest is the body of POST /submit.
type SubmitRequest struct {
	APIVersion   string         `json:"apiVersion"`
	WorkloadUID  string         `json:"workloadUID"`
	WorkloadName string         `json:"workloadName"`
	Namespace    string         `json:"namespace"`
	Owner        OwnerReference `json:"owner"`
	Site         string         `json:"site"`
	Account      string         `json:"account"`
	PodSets      []PodSet       `json:"podSets"`
}

// SubmitResponse is the body of a 200 or 201 answer to POST /submit.
type SubmitResponse struct {
	APIVersion string `json:"apiVersion"`
	JobID      string `json:"jobID"`
	Accepted   bool   `json:"accepted"`
}

// ErrorResponse is the body of every error answer.
type ErrorResponse struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Permanent  bool   `json:"permanent"`
	Message    string `json:"message"`
}

// RankStatus is the state of one member of the job.
type RankStatus struct {
	Rank       int32                    `json:"rank"`
	PodUID     string                   `json:"podUID"`
	State      string                   `json:"state"`
	ExitCode   int32                    `json:"exitCode"`
	Containers []corev1.ContainerStatus `json:"containers,omitempty"`
}

// ShadowRef names the shadow objects the executor rendered for the job.
type ShadowRef struct {
	Deployment string `json:"deployment,omitempty"`
	Service    string `json:"service,omitempty"`
}

// JobStatus is the body of GET /jobs/{workloadUID}.
type JobStatus struct {
	APIVersion     string       `json:"apiVersion"`
	WorkloadUID    string       `json:"workloadUID"`
	JobID          string       `json:"jobID"`
	State          string       `json:"state"`
	Reason         string       `json:"reason,omitempty"`
	Message        string       `json:"message,omitempty"`
	ExitCode       int32        `json:"exitCode"`
	NodeName       string       `json:"nodeName,omitempty"`
	Nodes          []string     `json:"nodes,omitempty"`
	SocketDir      string       `json:"socketDir,omitempty"`
	StartTime      *metav1.Time `json:"startTime,omitempty"`
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	Ranks          []RankStatus `json:"ranks,omitempty"`
	Shadow         *ShadowRef   `json:"shadow,omitempty"`
}
