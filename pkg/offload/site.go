package offload

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// ControllerName is the AdmissionCheck controllerName the offload controller serves.
const ControllerName = "interlink.eu/offload"

// ExecutorEndpointAnnotation on a namespace overrides the executor URL derived from
// the OffloadConfig. It is read from the namespace only, never from the Workload,
// so users cannot redirect their own jobs.
const ExecutorEndpointAnnotation = "interlink.eu/executor-endpoint"

// site is a OffloadConfig with its defaults applied.
type site struct {
	name                string
	executorService     string
	executorPort        int32
	pollInterval        time.Duration
	executorLostTimeout time.Duration
	maxRetries          int32
	stripPrefix         string
	flavors             map[string]v1alpha1.FlavorTarget
}

func resolveSite(sc *v1alpha1.OffloadConfig) site {
	s := site{
		name:                sc.Name,
		executorService:     sc.Spec.ExecutorService,
		executorPort:        sc.Spec.ExecutorPort,
		pollInterval:        15 * time.Second,
		executorLostTimeout: 10 * time.Minute,
		maxRetries:          3,
		stripPrefix:         sc.Name + "-",
		flavors:             sc.Spec.Flavors,
	}
	if s.executorService == "" {
		s.executorService = "kueue-offload-executor"
	}
	if s.executorPort == 0 {
		s.executorPort = 8000
	}
	if d := sc.Spec.PollInterval; d != nil && d.Duration > 0 {
		s.pollInterval = d.Duration
	}
	if d := sc.Spec.ExecutorLostTimeout; d != nil && d.Duration > 0 {
		s.executorLostTimeout = d.Duration
	}
	if sc.Spec.MaxRetries != nil {
		s.maxRetries = *sc.Spec.MaxRetries
	}
	if p := sc.Spec.AccountRule.StripPrefix; p != nil {
		s.stripPrefix = *p
	}
	return s
}

// account derives the Slurm account from a ClusterQueue name.
func (s site) account(clusterQueue string) (string, error) {
	if !strings.HasPrefix(clusterQueue, s.stripPrefix) || len(clusterQueue) == len(s.stripPrefix) {
		return "", fmt.Errorf("ClusterQueue %q does not match the account rule of site %s (prefix %q)", clusterQueue, s.name, s.stripPrefix)
	}
	return strings.TrimPrefix(clusterQueue, s.stripPrefix), nil
}

// executorEndpoint is the executor URL for a namespace.
func (s site) executorEndpoint(ns *corev1.Namespace) string {
	if v := strings.TrimSpace(ns.Annotations[ExecutorEndpointAnnotation]); v != "" {
		return strings.TrimRight(v, "/")
	}
	return fmt.Sprintf("http://%s.%s.svc:%d", s.executorService, ns.Name, s.executorPort)
}
