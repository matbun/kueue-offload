package executor

import (
	"strings"

	"github.com/containerd/containerd/log"
	v1 "k8s.io/api/core/v1"

	"github.com/matbun/kueue-offload/pkg/shadow"
)

// preExecAnnotation is shell the job script runs before the workload, as the
// Slurm plugin reads it.
const preExecAnnotation = "slurm-job.vk.io/pre-exec"

// LauncherAnnotation names the site launcher that starts every rank of a gang
// from the head's container, as the Slurm plugin reads it.
const LauncherAnnotation = "interlink.eu/launcher"

func launcherName(tmpl v1.PodTemplateSpec) string {
	return strings.TrimSpace(tmpl.Annotations[LauncherAnnotation])
}

// reverseJobName is the name of the head's Slurm job, by which the srun carrier
// finds the allocation: the Slurm plugin's single-pod script names the job after
// its pod's UID, a gang after its group (the workload UID).
func reverseJobName(req jobRequest) string {
	if podGroup(req) {
		return req.WorkloadUID
	}
	return memberUID(req.WorkloadUID, 0)
}

// reverseTunnel parses the reverse annotations of the head's template, with
// the same rules as the virtual kubelet plus Shadow.Reverse.AllowedTargets. It is
// nil when the template asks for none.
func (d *Executor) reverseTunnel(req jobRequest) (*shadow.ReverseTunnel, *refusal) {
	ps := req.PodSets[0]
	if !shadow.HasReverseAnnotations(ps.Template.Annotations) {
		return nil, nil
	}
	cfg := d.Config
	if !cfg.Shadow.Enabled {
		return nil, permanentError("%s is set, but this executor runs no shadows (Shadow.Enabled) to carry the tunnel", shadow.AnnReverseForwards)
	}
	// A site launcher starts every rank from the batch shell, where the tunnel's
	// /etc/hosts would reach ranks on nodes that have no relay.
	if podGroup(req) && launcherName(ps.Template) != "" {
		return nil, permanentError("%s cannot be combined with %s: only the head rank has the tunnel", shadow.AnnReverseForwards, LauncherAnnotation)
	}
	rcfg := cfg.Shadow.Reverse
	if err := shadow.NormalizeReverseConfig(&rcfg); err != nil {
		return nil, permanentError("%v", err)
	}
	rt, err := shadow.ParseReverseTunnel(ps.Template.Annotations, memberUID(req.WorkloadUID, 0), reverseJobName(req), rcfg)
	if err != nil {
		return nil, permanentError("%v", err)
	}
	if rt.SocksPort != 0 {
		log.L.Warningf("executor: workload %s: SOCKS proxy on 127.0.0.1:%d on the compute node: every user of that node can reach through it any endpoint the shadow reaches in the cluster", req.WorkloadUID, rt.SocksPort)
	}
	return rt, nil
}

// addReversePreExec puts the tunnel's set-up before the member's own pre-exec, as
// the virtual kubelet does. The forwarded names reach the job only through it: the
// plugin does not read spec.hostAliases. Only member 0 gets it: the shadow reaches
// the head rank's node and the other ranks have no tunnel end (multi-node tunnels
// are a non-goal).
func addReversePreExec(pod *v1.Pod, rt *shadow.ReverseTunnel) {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[preExecAnnotation] = shadow.ReversePreExec(rt, pod.Namespace) + pod.Annotations[preExecAnnotation]
}
