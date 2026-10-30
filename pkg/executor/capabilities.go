package executor

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/containerd/containerd/log"

	"github.com/matbun/kueue-offload/pkg/executorapi"
)

// CapabilityPodGroup is listed in a plugin's ping answer when it runs the pods of
// one Kueue pod group, created one by one, as one allocation: same
// kueue.x-k8s.io/pod-group-name label, kueue.x-k8s.io/pod-group-total-count
// annotation, rank from the completion index (backends.md 3.2).
const CapabilityPodGroup = "pod-group"

// CapabilityPodSetCreate is listed in a plugin's ping answer when it takes the
// pods of one pod group in a single /createPodSets call, by PodSet in rank order,
// and submits them as one job without buffering them (backends.md 3.3).
const CapabilityPodSetCreate = "podset-create"

// capabilitiesInterval is how often the executor reads the plugin's capabilities
// again.
var capabilitiesInterval = 5 * time.Minute

// podGroup reports whether the request's members run as one Kueue pod group:
// more than one, and not marked independent.
func podGroup(req jobRequest) bool {
	return workloadMembers(req) > 1 && !independentPods(req)
}

// independentPods reports whether every PodSet's template carries
// offload.interlink.eu/independent-pods: "true", which the owner's annotation
// gives them: one job per member.
func independentPods(req jobRequest) bool {
	for _, ps := range req.PodSets {
		if strings.TrimSpace(ps.Template.Annotations[executorapi.IndependentPodsAnnotation]) != "true" {
			return false
		}
	}
	return len(req.PodSets) > 0
}

// ping asks the API server for the plugin's ping and keeps what it answered: the
// health that /healthz reports and the plugin's capabilities.
func (d *Executor) ping(ctx context.Context) error {
	caps, err := d.InterLink.Ping(ctx)
	if err != nil {
		return err
	}
	d.healthMu.Lock()
	defer d.healthMu.Unlock()
	d.healthyUntil = time.Now().Add(healthTTL)
	if !d.capsKnown || !slices.Equal(caps, d.caps) {
		log.L.Infof("executor: the plugin lists capabilities %v", caps)
	}
	d.caps, d.capsKnown = caps, true
	return nil
}

// WatchCapabilities reads the plugin's capabilities now and every
// capabilitiesInterval until ctx ends (backends.md 3.1). A failed read keeps
// the last answer.
func (d *Executor) WatchCapabilities(ctx context.Context) {
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := d.ping(pingCtx); err != nil && ctx.Err() == nil {
			log.L.Warning("executor: cannot read the plugin's capabilities: ", err)
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-time.After(capabilitiesInterval):
		}
	}
}

// pluginCapabilities returns the capabilities of the last ping answer, and asks
// for one when there was none yet.
func (d *Executor) pluginCapabilities(ctx context.Context) ([]string, *refusal) {
	d.healthMu.Lock()
	caps, known := d.caps, d.capsKnown
	d.healthMu.Unlock()
	if known {
		return caps, nil
	}
	if err := d.ping(ctx); err != nil {
		return nil, unreachableError("asking the plugin for its capabilities: %v", err)
	}
	d.healthMu.Lock()
	defer d.healthMu.Unlock()
	return d.caps, nil
}

// checkShape refuses a pod group the plugin cannot run as one allocation
// (backends.md 3.6): its ranks would start as separate jobs, possibly hours
// apart, each holding its allocation while it waits for the others. A single pod
// and independent pods run on any plugin.
func (d *Executor) checkShape(ctx context.Context, req jobRequest) *refusal {
	if !podGroup(req) {
		return nil
	}
	caps, aerr := d.pluginCapabilities(ctx)
	if aerr != nil {
		return aerr
	}
	if slices.Contains(caps, CapabilityPodGroup) {
		return nil
	}
	return permanentError("the workload's %d pods run as one allocation, but the site's plugin does not list capability %q; "+
		"annotate the owner with %s: \"true\" to run one job per pod", workloadMembers(req), CapabilityPodGroup, executorapi.IndependentPodsAnnotation)
}

// createsPodSets reports whether submit sends the members of req in one
// /createPodSets call: a pod group, on a plugin that lists podset-create. Any
// other workload goes member by member to /create.
func (d *Executor) createsPodSets(ctx context.Context, req jobRequest) (bool, *refusal) {
	if !podGroup(req) {
		return false, nil
	}
	caps, aerr := d.pluginCapabilities(ctx)
	if aerr != nil {
		return false, aerr
	}
	return slices.Contains(caps, CapabilityPodSetCreate), nil
}
