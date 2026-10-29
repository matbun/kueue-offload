package dispatcher

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/interlink-hq/interlink-dispatch/api/v1alpha1"
)

// ControllerName is the AdmissionCheck controllerName the dispatcher serves.
const ControllerName = "interlink.eu/dispatch"

// AgentEndpointAnnotation on a namespace overrides the agent URL derived from
// the SiteConfig. It is read from the namespace only, never from the Workload,
// so users cannot redirect their own dispatch.
const AgentEndpointAnnotation = "interlink.eu/agent-endpoint"

// site is a SiteConfig with its defaults applied.
type site struct {
	name             string
	agentService     string
	agentPort        int32
	pollInterval     time.Duration
	agentLostTimeout time.Duration
	maxRetries       int32
	stripPrefix      string
	flavorPartitions map[string]string
}

func resolveSite(sc *v1alpha1.SiteConfig) site {
	s := site{
		name:             sc.Name,
		agentService:     sc.Spec.AgentService,
		agentPort:        sc.Spec.AgentPort,
		pollInterval:     15 * time.Second,
		agentLostTimeout: 10 * time.Minute,
		maxRetries:       3,
		stripPrefix:      sc.Name + "-",
		flavorPartitions: sc.Spec.FlavorPartitions,
	}
	if s.agentService == "" {
		s.agentService = "interlink-agent"
	}
	if s.agentPort == 0 {
		s.agentPort = 8000
	}
	if d := sc.Spec.PollInterval; d != nil && d.Duration > 0 {
		s.pollInterval = d.Duration
	}
	if d := sc.Spec.AgentLostTimeout; d != nil && d.Duration > 0 {
		s.agentLostTimeout = d.Duration
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

// agentEndpoint is the agent URL for a namespace.
func (s site) agentEndpoint(ns *corev1.Namespace) string {
	if v := strings.TrimSpace(ns.Annotations[AgentEndpointAnnotation]); v != "" {
		return strings.TrimRight(v, "/")
	}
	return fmt.Sprintf("http://%s.%s.svc:%d", s.agentService, ns.Name, s.agentPort)
}
