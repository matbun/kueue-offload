package executor

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/containerd/containerd/log"
	"github.com/google/uuid"
	authv1 "k8s.io/api/authentication/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	podconstants "sigs.k8s.io/kueue/pkg/controller/jobs/pod/constants"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/executorapi"
)

// Environment every member's containers get.
const (
	RankEnv      = "INTERLINK_RANK"
	WorldSizeEnv = "INTERLINK_WORLD_SIZE"
)

// Keys of a gang member. The pod group name and size are Kueue's, the rank is the
// Kubernetes completion index; the Slurm plugin also reads its own gang keys,
// which the template may not set.
const (
	PodGroupNameLabel            = podconstants.GroupNameLabel
	PodGroupTotalCountAnnotation = podconstants.GroupTotalCountAnnotation
	JobCompletionIndexLabel      = batchv1.JobCompletionIndexAnnotation

	GangNameAnnotation = "interlink.eu/gang-name"
	GangSizeAnnotation = "interlink.eu/gang-size"
	GangRoleAnnotation = "interlink.eu/gang-role"
	GangRankAnnotation = "interlink.eu/gang-rank"
	GangRoleHead       = "head"
	GangRoleWorker     = "worker"
)

// defaultVolumeMode is the file mode the API server defaults volumes to.
const defaultVolumeMode int32 = 0o644

// ownerName is the base of the members' names: the owner's, else the workload's.
func ownerName(req jobRequest) string {
	if req.Owner.Name != "" {
		return req.Owner.Name
	}
	return req.WorkloadName
}

// memberUID is the UID of member i of a workload: a UUID derived from the
// workload's, since the interLink API server takes only UUIDs as pod UIDs.
func memberUID(workloadUID string, i int) string {
	space, err := uuid.Parse(workloadUID)
	if err != nil {
		return ""
	}
	return uuid.NewSHA1(space, []byte("member-"+strconv.Itoa(i))).String()
}

// PodSetAnnotation names the PodSet a member comes from.
const PodSetAnnotation = "interlink.eu/podset"

// workloadMembers is the number of members of the request: every PodSet's count.
func workloadMembers(req jobRequest) int {
	n := 0
	for _, ps := range req.PodSets {
		n += int(ps.Count)
	}
	return n
}

// memberPodSet returns the index of the PodSet member i comes from. Members are
// numbered across the PodSets in request order.
func memberPodSet(req jobRequest, i int) int {
	for p, ps := range req.PodSets {
		if i < int(ps.Count) {
			return p
		}
		i -= int(ps.Count)
	}
	return -1
}

// buildMember returns member i of the request as the pod the plugin's create path
// takes: its own PodSet's template, named after the owner, labelled with its
// completion index within the whole workload, with its flavor's annotations
// appended to the template's. A workload of more than one member is a Kueue pod
// group of that size, member 0 (of the first PodSet) its head, unless its pods are
// independent (podGroup). References are not resolved here.
func buildMember(req jobRequest, i int) *v1.Pod {
	ps := req.PodSets[memberPodSet(req, i)]
	count := workloadMembers(req)
	tmpl := ps.Template.DeepCopy()

	labels := map[string]string{}
	for k, v := range tmpl.Labels {
		labels[k] = v
	}
	annotations := map[string]string{}
	for k, v := range tmpl.Annotations {
		annotations[k] = v
	}
	for _, k := range []string{PodGroupNameLabel, JobCompletionIndexLabel} {
		delete(labels, k)
	}
	for _, k := range []string{PodGroupTotalCountAnnotation, GangNameAnnotation, GangSizeAnnotation, GangRankAnnotation, GangRoleAnnotation} {
		delete(annotations, k)
	}
	labels[JobCompletionIndexLabel] = strconv.Itoa(i)
	annotations[PodSetAnnotation] = string(ps.Name)
	if podGroup(req) {
		labels[PodGroupNameLabel] = req.WorkloadUID
		annotations[PodGroupTotalCountAnnotation] = strconv.Itoa(count)
		annotations[GangRankAnnotation] = strconv.Itoa(i)
		annotations[GangRoleAnnotation] = GangRoleWorker
		if i == 0 {
			annotations[GangRoleAnnotation] = GangRoleHead
		}
	}
	// The flavor's values come after the template's own, so the site's options
	// win; they are never parsed. Values of several lines (submit description
	// lines) are joined by a newline, so the template's last line cannot swallow
	// the site's first.
	for k, v := range ps.Annotations {
		switch {
		case v == "":
		case annotations[k] == "":
			annotations[k] = v
		case strings.Contains(v, "\n") || strings.Contains(annotations[k], "\n"):
			annotations[k] = strings.TrimRight(annotations[k], "\n") + "\n" + v
		default:
			annotations[k] += " " + v
		}
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s-%d", ownerName(req), i),
			Namespace:   req.Namespace,
			UID:         types.UID(memberUID(req.WorkloadUID, i)),
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: tmpl.Spec,
	}

	rankEnv := []v1.EnvVar{
		{Name: RankEnv, Value: strconv.Itoa(i)},
		{Name: WorldSizeEnv, Value: strconv.Itoa(count)},
	}
	for _, list := range [][]v1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for c := range list {
			requestsAsLimits(&list[c].Resources)
			var env []v1.EnvVar
			for _, e := range list[c].Env {
				if e.Name != RankEnv && e.Name != WorldSizeEnv {
					env = append(env, e)
				}
			}
			list[c].Env = append(env, rankEnv...)
		}
	}

	for v := range pod.Spec.Volumes {
		src := &pod.Spec.Volumes[v].VolumeSource
		switch {
		case src.Secret != nil && src.Secret.DefaultMode == nil:
			src.Secret.DefaultMode = ptrInt32(defaultVolumeMode)
		case src.ConfigMap != nil && src.ConfigMap.DefaultMode == nil:
			src.ConfigMap.DefaultMode = ptrInt32(defaultVolumeMode)
		case src.Projected != nil && src.Projected.DefaultMode == nil:
			src.Projected.DefaultMode = ptrInt32(defaultVolumeMode)
		case src.DownwardAPI != nil && src.DownwardAPI.DefaultMode == nil:
			src.DownwardAPI.DefaultMode = ptrInt32(defaultVolumeMode)
		}
	}
	// A workload of one plain pod is that pod: its downward API names it, as a workflow
	// engine's executor (Argo's) addresses its own pod by name and UID through the API
	// server, and an owner reference to a UID that does not exist is garbage collected.
	if count == 1 && req.Owner.Kind == "Pod" && req.Owner.Name != "" {
		pod.Name = req.Owner.Name
		if req.Owner.UID != "" {
			pod.Annotations[DownwardUIDAnnotation] = string(req.Owner.UID)
		}
	}
	return pod
}

// requestsAsLimits gives a resource that has a request but no limit a limit equal to
// the request. Kueue charts a Workload by its requests and the plugin sizes the job
// from limits, so without it a Job that requests 20 CPUs would ask Slurm for one.
func requestsAsLimits(r *v1.ResourceRequirements) {
	for name, q := range r.Requests {
		if _, ok := r.Limits[name]; ok {
			continue
		}
		if r.Limits == nil {
			r.Limits = v1.ResourceList{}
		}
		r.Limits[name] = q.DeepCopy()
	}
}

// resourceAsked is what r asks of a resource: its limit, else its request.
func resourceAsked(r v1.ResourceRequirements, name v1.ResourceName) (resource.Quantity, bool) {
	if q, ok := r.Limits[name]; ok {
		return q, true
	}
	q, ok := r.Requests[name]
	return q, ok
}

// templateGPUs is the GPUs a template's containers ask for, under the site's
// GPUResource or one of its aliases: the plugin adds them up into the job's.
func templateGPUs(spec v1.PodSpec, res *SiteResources) int64 {
	names := append([]string{res.GPUResource}, res.GPUAliases...)
	var n int64
	for _, c := range spec.Containers {
		for _, name := range names {
			if q, ok := resourceAsked(c.Resources, v1.ResourceName(name)); ok {
				n += q.Value()
			}
		}
	}
	return n
}

// templateCPU is the most CPU a container of the template asks for, which the
// plugin makes the job's CPUs; set is false when no container asks for any.
func templateCPU(spec v1.PodSpec) (cpu resource.Quantity, set bool) {
	for _, c := range spec.Containers {
		if q, ok := resourceAsked(c.Resources, v1.ResourceCPU); ok && (!set || q.Cmp(cpu) > 0) {
			cpu, set = q, true
		}
	}
	return cpu, set
}

// withSiteResources sizes a PodSet's template for a site with Resources, whose
// shape validateResources checked: its GPU aliases become GPUResource again, so the
// plugin asks the site for them, and a template that sets no CPU gets CPUsPerGPU
// per GPU as its first container's CPU limit, which the plugin makes the job's CPUs.
// Kueue charged the Workload by the template as it is.
func withSiteResources(ps *v1alpha1.PodSet, res *SiteResources) {
	if res == nil {
		return
	}
	spec := &ps.Template.Spec
	gpus := templateGPUs(*spec, res)
	gpu := v1.ResourceName(res.GPUResource)
	for _, list := range [][]v1.Container{spec.InitContainers, spec.Containers} {
		for c := range list {
			for _, rl := range []v1.ResourceList{list[c].Resources.Requests, list[c].Resources.Limits} {
				for _, alias := range res.GPUAliases {
					q, ok := rl[v1.ResourceName(alias)]
					if !ok {
						continue
					}
					if have, ok := rl[gpu]; ok {
						q.Add(have)
					}
					rl[gpu] = q
					delete(rl, v1.ResourceName(alias))
				}
			}
		}
	}
	if _, set := templateCPU(*spec); !set && len(spec.Containers) > 0 {
		r := &spec.Containers[0].Resources
		if r.Limits == nil {
			r.Limits = v1.ResourceList{}
		}
		r.Limits[v1.ResourceCPU] = *resource.NewQuantity(res.CPUsPerGPU*gpus, resource.DecimalSI)
	}
}

// serviceAccountDir is where the API server mounts a pod's ServiceAccount files.
const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// withServiceAccountDir does for a PodSet template what the API server's
// ServiceAccount admission does for a pod, since the workload's pods never exist:
// unless the template opts out or mounts the directory itself, every container gets
// /var/run/secrets/kubernetes.io/serviceaccount with namespace and ca.crt, and the
// token when token is true. Clients read their namespace there (Katib's SDK does).
func withServiceAccountDir(ps *v1alpha1.PodSet, token bool) {
	spec := &ps.Template.Spec
	if !token && ps.Template.Annotations[executorapi.ServiceAccountTokenAnnotation] != "true" {
		dropInjectedToken(spec)
	}
	if spec.AutomountServiceAccountToken != nil && !*spec.AutomountServiceAccountToken {
		return
	}
	for _, list := range [][]v1.Container{spec.InitContainers, spec.Containers} {
		for _, c := range list {
			for _, m := range c.VolumeMounts {
				if m.MountPath == serviceAccountDir {
					return
				}
			}
		}
	}
	const name = "kube-api-access-interlink"
	var sources []v1.VolumeProjection
	if token {
		expiration := int64(3607)
		sources = append(sources, v1.VolumeProjection{ServiceAccountToken: &v1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: &expiration}})
	}
	sources = append(sources,
		v1.VolumeProjection{ConfigMap: &v1.ConfigMapProjection{LocalObjectReference: v1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []v1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
		v1.VolumeProjection{DownwardAPI: &v1.DownwardAPIProjection{Items: []v1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}}}},
	)
	spec.Volumes = append(spec.Volumes, v1.Volume{Name: name, VolumeSource: v1.VolumeSource{Projected: &v1.ProjectedVolumeSource{Sources: sources}}})
	for _, list := range [][]v1.Container{spec.InitContainers, spec.Containers} {
		for c := range list {
			list[c].VolumeMounts = append(list[c].VolumeMounts, v1.VolumeMount{Name: name, MountPath: serviceAccountDir, ReadOnly: true})
		}
	}
	if !slices.Contains(ps.ConfigMapRefs, "kube-root-ca.crt") {
		ps.ConfigMapRefs = append(ps.ConfigMapRefs, "kube-root-ca.crt")
	}
}

// dropInjectedToken removes the token from the kube-api-access-* volumes the API
// server adds to a pod (a plain pod's template is the pod itself), keeping its
// ca.crt and namespace, so no token of the cluster is minted for the job.
func dropInjectedToken(spec *v1.PodSpec) {
	for i, vol := range spec.Volumes {
		if vol.Projected == nil || !strings.HasPrefix(vol.Name, "kube-api-access-") {
			continue
		}
		sources := slices.DeleteFunc(slices.Clone(vol.Projected.Sources), func(s v1.VolumeProjection) bool { return s.ServiceAccountToken != nil })
		spec.Volumes[i].Projected = &v1.ProjectedVolumeSource{Sources: sources, DefaultMode: vol.Projected.DefaultMode}
	}
}

// DownwardUIDAnnotation carries the UID a member's downward API reports when it is not
// the member's own (see buildMember).
const DownwardUIDAnnotation = "interlink.eu/downward-uid"

// withKubernetesAPI gives every container the API server's address, as the kubelet
// does, unless the container sets it itself.
func withKubernetesAPI(pod *v1.Pod, api ExecutorKubernetesAPI) {
	vars := []v1.EnvVar{
		{Name: "KUBERNETES_SERVICE_HOST", Value: api.Host},
		{Name: "KUBERNETES_SERVICE_PORT", Value: api.Port},
		{Name: "KUBERNETES_SERVICE_PORT_HTTPS", Value: api.Port},
	}
	for _, list := range [][]v1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for c := range list {
			for _, v := range vars {
				if !slices.ContainsFunc(list[c].Env, func(e v1.EnvVar) bool { return e.Name == v.Name }) {
					list[c].Env = append(list[c].Env, v)
				}
			}
		}
	}
}

func ptrInt32(v int32) *int32 { return &v }

// podRefs are the Secrets and ConfigMaps of a workload, as read from the cluster.
// A name that is listed but absent, and only referenced as optional, is missing.
type podRefs struct {
	secrets    map[string]*v1.Secret
	configMaps map[string]*v1.ConfigMap
	// tokens are the minted service account tokens, by tokenKey.
	tokens map[string]string
}

// defaultTokenExpirationSeconds is what the API server defaults a
// serviceAccountToken projection's expirationSeconds to.
const defaultTokenExpirationSeconds int64 = 3600

// tokenKey names one serviceAccountToken projection: its volume and its path.
func tokenKey(volume, path string) string { return volume + "/" + path }

// mintTokens requests a token through the TokenRequest API for every
// serviceAccountToken projection of the template, as the kubelet does for a pod:
// for the pod's ServiceAccount, with the projection's audience and lifetime. A
// template without one makes no request. The tokens are not bound to a pod: the
// workload's pods never exist.
func (d *Executor) mintTokens(ctx context.Context, namespace string, spec v1.PodSpec) (map[string]string, *refusal) {
	tokens := map[string]string{}
	account := spec.ServiceAccountName
	if account == "" {
		account = "default"
	}
	for _, vol := range spec.Volumes {
		if vol.Projected == nil {
			continue
		}
		for _, src := range vol.Projected.Sources {
			p := src.ServiceAccountToken
			if p == nil {
				continue
			}
			if d.Kube == nil {
				return nil, transientError("the executor has no Kubernetes client to request a token for ServiceAccount %q", account)
			}
			expiration := defaultTokenExpirationSeconds
			if p.ExpirationSeconds != nil {
				expiration = *p.ExpirationSeconds
			}
			req := &authv1.TokenRequest{Spec: authv1.TokenRequestSpec{ExpirationSeconds: &expiration}}
			if p.Audience != "" {
				req.Spec.Audiences = []string{p.Audience}
			}
			tr, err := d.Kube.CoreV1().ServiceAccounts(namespace).CreateToken(ctx, account, req, metav1.CreateOptions{})
			switch {
			case apierrors.IsNotFound(err), apierrors.IsForbidden(err):
				return nil, permanentError("volume %s: cannot request a token for ServiceAccount %q: %v", vol.Name, account, err)
			case err != nil:
				return nil, transientError("volume %s: requesting a token for ServiceAccount %q: %v", vol.Name, account, err)
			}
			tokens[tokenKey(vol.Name, p.Path)] = tr.Status.Token
		}
	}
	return tokens, nil
}

// templateRefs returns the Secrets and ConfigMaps the template references,
// each mapped to whether one of its references is not optional.
func templateRefs(spec v1.PodSpec) (secrets, configMaps map[string]bool) {
	secrets, configMaps = map[string]bool{}, map[string]bool{}
	add := func(m map[string]bool, name string, optional *bool) {
		if name == "" {
			return
		}
		m[name] = m[name] || optional == nil || !*optional
	}
	for _, vol := range spec.Volumes {
		switch {
		case vol.Secret != nil:
			add(secrets, vol.Secret.SecretName, vol.Secret.Optional)
		case vol.ConfigMap != nil:
			add(configMaps, vol.ConfigMap.Name, vol.ConfigMap.Optional)
		case vol.Projected != nil:
			for _, src := range vol.Projected.Sources {
				if src.Secret != nil {
					add(secrets, src.Secret.Name, src.Secret.Optional)
				}
				if src.ConfigMap != nil {
					add(configMaps, src.ConfigMap.Name, src.ConfigMap.Optional)
				}
			}
		}
	}
	for _, list := range [][]v1.Container{spec.InitContainers, spec.Containers} {
		for _, c := range list {
			for _, ef := range c.EnvFrom {
				if ef.SecretRef != nil {
					add(secrets, ef.SecretRef.Name, ef.SecretRef.Optional)
				}
				if ef.ConfigMapRef != nil {
					add(configMaps, ef.ConfigMapRef.Name, ef.ConfigMapRef.Optional)
				}
			}
			for _, e := range c.Env {
				if e.ValueFrom == nil {
					continue
				}
				if r := e.ValueFrom.SecretKeyRef; r != nil {
					add(secrets, r.Name, r.Optional)
				}
				if r := e.ValueFrom.ConfigMapKeyRef; r != nil {
					add(configMaps, r.Name, r.Optional)
				}
			}
		}
	}
	return secrets, configMaps
}

// fetchRefs reads the PodSet's secretRefs and configMapRefs, and only those,
// from the executor's namespace. A reference of the template that the lists leave
// out, or a required one the executor cannot read, is a permanent refusal.
func (d *Executor) fetchRefs(ctx context.Context, namespace string, ps v1alpha1.PodSet) (podRefs, *refusal) {
	refs := podRefs{secrets: map[string]*v1.Secret{}, configMaps: map[string]*v1.ConfigMap{}}
	usedSecrets, usedConfigMaps := templateRefs(ps.Template.Spec)
	for name := range usedSecrets {
		if !slices.Contains(ps.SecretRefs, name) {
			return refs, permanentError("the template references Secret %q, which is not in secretRefs", name)
		}
	}
	for name := range usedConfigMaps {
		if !slices.Contains(ps.ConfigMapRefs, name) {
			return refs, permanentError("the template references ConfigMap %q, which is not in configMapRefs", name)
		}
	}
	if len(ps.SecretRefs)+len(ps.ConfigMapRefs) > 0 && d.Kube == nil {
		return refs, transientError("the executor has no Kubernetes client to read the workload's references")
	}

	readErr := func(kind, name string, required bool, err error) *refusal {
		switch {
		case apierrors.IsNotFound(err) && !required:
			return nil
		case apierrors.IsNotFound(err), apierrors.IsForbidden(err):
			return permanentError("cannot read %s %q: %v", kind, name, err)
		}
		return transientError("reading %s %q: %v", kind, name, err)
	}
	for _, name := range ps.SecretRefs {
		if _, done := refs.secrets[name]; done {
			continue
		}
		s, err := d.Kube.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if aerr := readErr("Secret", name, usedSecrets[name], err); aerr != nil {
				return refs, aerr
			}
			continue
		}
		refs.secrets[name] = s
	}
	for _, name := range ps.ConfigMapRefs {
		if _, done := refs.configMaps[name]; done {
			continue
		}
		cm, err := d.Kube.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if aerr := readErr("ConfigMap", name, usedConfigMaps[name], err); aerr != nil {
				return refs, aerr
			}
			continue
		}
		refs.configMaps[name] = cm
	}
	return refs, nil
}

// metadataFieldRefPattern matches metadata.labels['k'] and metadata.annotations['k'].
var metadataFieldRefPattern = regexp.MustCompile(`^metadata\.(labels|annotations)\[['"]?(.+?)['"]?\]$`)

// fieldRefValue resolves a fieldRef as the kubelet would for this pod. ok is
// false for a path the executor cannot know.
func fieldRefValue(pod *v1.Pod, path string) (string, bool) {
	switch path {
	case "metadata.name":
		return pod.Name, true
	case "metadata.namespace":
		return pod.Namespace, true
	case "metadata.uid":
		if uid := pod.Annotations[DownwardUIDAnnotation]; uid != "" {
			return uid, true
		}
		return string(pod.UID), true
	case "spec.serviceAccountName":
		return pod.Spec.ServiceAccountName, true
	}
	if m := metadataFieldRefPattern.FindStringSubmatch(path); m != nil {
		if m[1] == "labels" {
			return pod.Labels[m[2]], true
		}
		return pod.Annotations[m[2]], true
	}
	return "", false
}

// resourceFieldRefValue resolves a resourceFieldRef as the kubelet would: the
// quantity divided by the divisor, rounded up. A limit that is not set falls
// back to the request.
func resourceFieldRefValue(pod *v1.Pod, self *v1.Container, ref *v1.ResourceFieldSelector) (string, error) {
	target := self
	if ref.ContainerName != "" && ref.ContainerName != self.Name {
		target = nil
		for _, list := range [][]v1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
			for c := range list {
				if list[c].Name == ref.ContainerName {
					target = &list[c]
				}
			}
		}
		if target == nil {
			return "", fmt.Errorf("resourceFieldRef: no container %q", ref.ContainerName)
		}
	}
	scope, name, ok := strings.Cut(ref.Resource, ".")
	if !ok {
		return "", fmt.Errorf("resourceFieldRef: unsupported resource %q", ref.Resource)
	}
	var qty resource.Quantity
	switch scope {
	case "requests":
		qty = target.Resources.Requests[v1.ResourceName(name)]
	case "limits":
		var set bool
		qty, set = target.Resources.Limits[v1.ResourceName(name)]
		if !set {
			qty = target.Resources.Requests[v1.ResourceName(name)]
		}
	default:
		return "", fmt.Errorf("resourceFieldRef: unsupported resource %q", ref.Resource)
	}
	divisor := ref.Divisor
	if divisor.IsZero() {
		divisor = resource.MustParse("1")
	}
	return strconv.FormatInt(int64(math.Ceil(float64(qty.MilliValue())/float64(divisor.MilliValue()))), 10), nil
}

// resolveEnv replaces every env[].valueFrom of the pod's containers by its value,
// and then every envFrom by the variables it gives, as the virtual kubelet does
// before /create (resolveEnvRefs and resolveEnvFromRefs): the API server hands a
// container only the Secrets and ConfigMaps it mounts. An optional reference to a
// missing key drops the variable; a required one is a permanent refusal.
func resolveEnv(pod *v1.Pod, refs podRefs) *refusal {
	for _, list := range [][]v1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for c := range list {
			container := &list[c]
			env := make([]v1.EnvVar, 0, len(container.Env))
			for _, e := range container.Env {
				if e.ValueFrom == nil {
					env = append(env, e)
					continue
				}
				value, keep, aerr := envFromSource(pod, container, e, refs)
				if aerr != nil {
					return aerr
				}
				if keep {
					env = append(env, v1.EnvVar{Name: e.Name, Value: value})
				}
			}
			container.Env = env
			resolveEnvFrom(container, refs)
		}
	}
	return nil
}

// resolveEnvFrom appends the variables of the container's envFrom sources to its
// env and drops envFrom, with the kubelet's precedence: a variable of env wins, and
// of two sources the later one. A source that is missing is optional: fetchRefs
// refused the others.
func resolveEnvFrom(container *v1.Container, refs podRefs) {
	if len(container.EnvFrom) == 0 {
		return
	}
	explicit := map[string]bool{}
	for _, e := range container.Env {
		explicit[e.Name] = true
	}
	index := map[string]int{}
	upsert := func(name, value string) {
		if explicit[name] {
			return
		}
		if i, ok := index[name]; ok {
			container.Env[i].Value = value
			return
		}
		index[name] = len(container.Env)
		container.Env = append(container.Env, v1.EnvVar{Name: name, Value: value})
	}
	for _, ef := range container.EnvFrom {
		if r := ef.ConfigMapRef; r != nil {
			if cm := refs.configMaps[r.Name]; cm != nil {
				for _, k := range sortedKeys(cm.Data) {
					upsert(ef.Prefix+k, cm.Data[k])
				}
			}
		}
		if r := ef.SecretRef; r != nil {
			if s := refs.secrets[r.Name]; s != nil {
				for _, k := range sortedKeys(s.Data) {
					upsert(ef.Prefix+k, string(s.Data[k]))
				}
			}
		}
	}
	container.EnvFrom = nil
}

func envFromSource(pod *v1.Pod, container *v1.Container, e v1.EnvVar, refs podRefs) (string, bool, *refusal) {
	src := e.ValueFrom
	optional := func(o *bool) bool { return o != nil && *o }
	switch {
	case src.SecretKeyRef != nil:
		r := src.SecretKeyRef
		if s := refs.secrets[r.Name]; s != nil {
			if v, ok := s.Data[r.Key]; ok {
				return string(v), true, nil
			}
			if v, ok := s.StringData[r.Key]; ok {
				return v, true, nil
			}
		}
		if optional(r.Optional) {
			return "", false, nil
		}
		return "", false, permanentError("container %s, env %s: Secret %q has no key %q", container.Name, e.Name, r.Name, r.Key)
	case src.ConfigMapKeyRef != nil:
		r := src.ConfigMapKeyRef
		if cm := refs.configMaps[r.Name]; cm != nil {
			if v, ok := cm.Data[r.Key]; ok {
				return v, true, nil
			}
		}
		if optional(r.Optional) {
			return "", false, nil
		}
		return "", false, permanentError("container %s, env %s: ConfigMap %q has no key %q", container.Name, e.Name, r.Name, r.Key)
	case src.FieldRef != nil:
		v, ok := fieldRefValue(pod, src.FieldRef.FieldPath)
		if !ok {
			log.L.Warningf("executor: container %s, env %s: fieldRef %s is unknown outside a node, left empty", container.Name, e.Name, src.FieldRef.FieldPath)
		}
		return v, true, nil
	case src.ResourceFieldRef != nil:
		v, err := resourceFieldRefValue(pod, container, src.ResourceFieldRef)
		if err != nil {
			return "", false, permanentError("container %s, env %s: %v", container.Name, e.Name, err)
		}
		return v, true, nil
	}
	return "", true, nil
}

// formatDownwardAPIMap renders labels or annotations as a downward API file does.
func formatDownwardAPIMap(m map[string]string) string {
	var b strings.Builder
	for _, k := range sortedKeys(m) {
		b.WriteString(k + "=" + strconv.Quote(m[k]) + "\n")
	}
	return b.String()
}

func downwardAPIFile(pod *v1.Pod, path string) (string, bool) {
	switch path {
	case "metadata.labels":
		return formatDownwardAPIMap(pod.Labels), true
	case "metadata.annotations":
		return formatDownwardAPIMap(pod.Annotations), true
	}
	return fieldRefValue(pod, path)
}

// podCreateRequest builds what the virtual kubelet sends to the API server's
// /create for this pod (remoteExecutionHandleVolumes): the Secrets and ConfigMaps
// of its secret and configMap volumes, and the contents of its projected and
// downward API volumes, one ConfigMap named after each volume, service account
// tokens included (mintTokens). A downward API volume becomes a projected one, as
// the virtual kubelet does. The API server hands each container the data of the
// volumes it mounts.
func podCreateRequest(pod *v1.Pod, refs podRefs) (commonIL.PodCreateRequests, *refusal) {
	req := commonIL.PodCreateRequests{}
	for v := range pod.Spec.Volumes {
		vol := &pod.Spec.Volumes[v]
		switch {
		case vol.Secret != nil:
			if s := refs.secrets[vol.Secret.SecretName]; s != nil {
				req.Secrets = append(req.Secrets, *s)
			}
			continue
		case vol.ConfigMap != nil:
			if cm := refs.configMaps[vol.ConfigMap.Name]; cm != nil {
				req.ConfigMaps = append(req.ConfigMaps, *cm)
			}
			continue
		case vol.DownwardAPI != nil:
			items := vol.DownwardAPI.Items
			vol.VolumeSource = v1.VolumeSource{Projected: &v1.ProjectedVolumeSource{
				DefaultMode: vol.DownwardAPI.DefaultMode,
				Sources:     []v1.VolumeProjection{{DownwardAPI: &v1.DownwardAPIProjection{Items: items}}},
			}}
		case vol.Projected == nil:
			continue
		}
		cm := v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: vol.Name}, Data: map[string]string{}}
		for _, src := range vol.Projected.Sources {
			switch {
			case src.Secret != nil:
				s := refs.secrets[src.Secret.Name]
				if s == nil {
					continue
				}
				data := map[string]string{}
				for k, val := range s.Data {
					data[k] = string(val)
				}
				if aerr := projectItems(cm.Data, data, src.Secret.Items, "Secret", src.Secret.Name); aerr != nil {
					return req, aerr
				}
			case src.ConfigMap != nil:
				c := refs.configMaps[src.ConfigMap.Name]
				if c == nil {
					continue
				}
				if aerr := projectItems(cm.Data, c.Data, src.ConfigMap.Items, "ConfigMap", src.ConfigMap.Name); aerr != nil {
					return req, aerr
				}
			case src.DownwardAPI != nil:
				for _, item := range src.DownwardAPI.Items {
					if item.FieldRef == nil {
						continue
					}
					if val, ok := downwardAPIFile(pod, item.FieldRef.FieldPath); ok {
						cm.Data[item.Path] = val
					}
				}
			case src.ServiceAccountToken != nil:
				token, ok := refs.tokens[tokenKey(vol.Name, src.ServiceAccountToken.Path)]
				if !ok {
					return req, transientError("volume %s: no token was requested for %s", vol.Name, src.ServiceAccountToken.Path)
				}
				cm.Data[src.ServiceAccountToken.Path] = token
			}
		}
		req.ProjectedVolumeMaps = append(req.ProjectedVolumeMaps, cm)
	}
	req.Pod = *pod
	return req, nil
}

// projectItems copies the keys items selects (all of them without items) of a
// Secret's or ConfigMap's data into a projected volume's files.
func projectItems(files, data map[string]string, items []v1.KeyToPath, kind, name string) *refusal {
	if len(items) == 0 {
		for k, v := range data {
			files[k] = v
		}
		return nil
	}
	for _, item := range items {
		v, ok := data[item.Key]
		if !ok {
			return permanentError("%s %q has no key %q", kind, name, item.Key)
		}
		files[item.Path] = v
	}
	return nil
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
