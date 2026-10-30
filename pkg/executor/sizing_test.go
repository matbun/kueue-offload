package executor

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

const (
	gpuResource = "nvidia.com/gpu"
	// gpuAlias is what an admission policy renames gpuResource to in gated pods.
	gpuAlias = "example.org/hpc-gpu"
)

// sizedSite is a site of nodes with 4 GPUs and 20 cores per GPU.
func (e *executorEnv) sizedSite() {
	site := e.d.Config.Sites[executorSite]
	site.Resources = &SiteResources{GPUResource: gpuResource, GPUAliases: []string{gpuAlias}, CPUsPerGPU: 20, GPUsPerMember: 4}
	e.d.Config.Sites[executorSite] = site
}

func resources(requests, limits map[string]string) v1.ResourceRequirements {
	r := v1.ResourceRequirements{}
	for name, q := range requests {
		if r.Requests == nil {
			r.Requests = v1.ResourceList{}
		}
		r.Requests[v1.ResourceName(name)] = resource.MustParse(q)
	}
	for name, q := range limits {
		if r.Limits == nil {
			r.Limits = v1.ResourceList{}
		}
		r.Limits[v1.ResourceName(name)] = resource.MustParse(q)
	}
	return r
}

// sizedRequest is a request whose containers ask for these resources.
func sizedRequest(containers ...v1.ResourceRequirements) *v1alpha1.OffloadRequest {
	req := testRequest(1)
	base := req.Spec.PodSets[0].Template.Spec.Containers[0]
	req.Spec.PodSets[0].Template.Spec.Containers = nil
	for i, r := range containers {
		c := *base.DeepCopy()
		if i > 0 {
			c.Name = "sidecar"
		}
		c.Resources = r
		req.Spec.PodSets[0].Template.Spec.Containers = append(req.Spec.PodSets[0].Template.Spec.Containers, c)
	}
	return req
}

// A member asks for whole GPUs only: the CPU follows from them, and GPUs a
// policy renamed go to the plugin under their own name again.
func TestSiteResources(t *testing.T) {
	tests := []struct {
		name       string
		containers []v1.ResourceRequirements
		want       []map[v1.ResourceName]string
	}{
		{"no CPU", []v1.ResourceRequirements{resources(map[string]string{"memory": "8Gi"}, map[string]string{gpuResource: "1"})},
			[]map[v1.ResourceName]string{{v1.ResourceCPU: "20", gpuResource: "1"}}},
		{"renamed GPUs", []v1.ResourceRequirements{resources(map[string]string{gpuAlias: "2"}, map[string]string{gpuAlias: "2"})},
			[]map[v1.ResourceName]string{{v1.ResourceCPU: "40", gpuResource: "2"}}},
		{"CPU as the site gives it", []v1.ResourceRequirements{resources(map[string]string{"cpu": "20000m", gpuResource: "1"}, map[string]string{gpuResource: "1"})},
			[]map[v1.ResourceName]string{{v1.ResourceCPU: "20", gpuResource: "1"}}},
		{"GPUs of two containers", []v1.ResourceRequirements{
			resources(nil, map[string]string{gpuResource: "1"}),
			resources(nil, map[string]string{gpuAlias: "1"}),
		}, []map[v1.ResourceName]string{{v1.ResourceCPU: "40", gpuResource: "1"}, {gpuResource: "1"}}},
		{"a sidecar sets the CPU", []v1.ResourceRequirements{
			resources(nil, map[string]string{gpuResource: "1"}),
			resources(map[string]string{"cpu": "20"}, nil),
		}, []map[v1.ResourceName]string{{gpuResource: "1"}, {v1.ResourceCPU: "20"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			e.sizedSite()
			req := sizedRequest(tt.containers...)
			e.submit(req)
			pod := e.plugin.createdPods()[0]
			for i, want := range tt.want {
				limits := pod.Spec.Containers[i].Resources.Limits
				if _, ok := limits[gpuAlias]; ok {
					t.Errorf("container %d keeps %s: %v", i, gpuAlias, limits)
				}
				if _, ok := pod.Spec.Containers[i].Resources.Requests[gpuAlias]; ok {
					t.Errorf("container %d keeps %s in its requests", i, gpuAlias)
				}
				for name, q := range want {
					if got := limits[name]; got.Cmp(resource.MustParse(q)) != 0 {
						t.Errorf("container %d: limit %s = %s, want %s (%v)", i, name, got.String(), q, limits)
					}
				}
				if _, ok := want[v1.ResourceCPU]; !ok {
					if _, set := limits[v1.ResourceCPU]; set {
						t.Errorf("container %d got a CPU limit: %v", i, limits)
					}
				}
			}
			// The request is the Workload's, as Kueue charged it.
			if got := e.request().Spec.PodSets[0].Template.Spec.Containers[0].Resources; !equalResources(got, tt.containers[0]) {
				t.Fatalf("the request's template was changed: %+v", got)
			}
		})
	}
}

func equalResources(a, b v1.ResourceRequirements) bool {
	same := func(x, y v1.ResourceList) bool {
		if len(x) != len(y) {
			return false
		}
		for k, q := range x {
			if p, ok := y[k]; !ok || p.Cmp(q) != 0 {
				return false
			}
		}
		return true
	}
	return same(a.Requests, b.Requests) && same(a.Limits, b.Limits)
}

func TestSiteResourcesRefusals(t *testing.T) {
	tests := []struct {
		name       string
		containers []v1.ResourceRequirements
		want       string
	}{
		{"no GPU", []v1.ResourceRequirements{resources(map[string]string{"cpu": "20"}, nil)},
			"PodSet main asks for 0 nvidia.com/gpu: a pod gets at most one node, 4 GPUs: ask for 1 to 4 nvidia.com/gpu"},
		{"more than a node", []v1.ResourceRequirements{resources(nil, map[string]string{gpuResource: "4"}), resources(nil, map[string]string{gpuAlias: "1"})},
			"PodSet main asks for 5 nvidia.com/gpu: a pod gets at most one node"},
		{"fewer CPUs", []v1.ResourceRequirements{resources(map[string]string{"cpu": "4"}, map[string]string{gpuResource: "1"})},
			"PodSet main asks for 4 CPUs with 1 nvidia.com/gpu: resources are allocated automatically, 20 cores per GPU: leave cpu out or set it to 20"},
		{"a CPU limit above the request", []v1.ResourceRequirements{resources(map[string]string{"cpu": "20"}, map[string]string{"cpu": "40", gpuResource: "1"})},
			"asks for 40 CPUs with 1 nvidia.com/gpu"},
		{"a sidecar asks for more", []v1.ResourceRequirements{resources(map[string]string{"cpu": "40"}, map[string]string{gpuResource: "2"}), resources(map[string]string{"cpu": "80"}, nil)},
			"asks for 80 CPUs with 2 nvidia.com/gpu"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			e.sizedSite()
			e.refusedWith(e.take(sizedRequest(tt.containers...)), tt.want)
			if n := len(e.plugin.createdPods()); n != 0 {
				t.Fatalf("%d /create calls", n)
			}
		})
	}
}

// A site with AllowedFlags takes no Slurm options of the workload's own beyond
// those; its Kueue flavor's options always go.
func TestAllowedFlags(t *testing.T) {
	allow := func(e *executorEnv, flags, flavors []string) {
		site := e.d.Config.Sites[executorSite]
		site.AllowedFlags = &AllowedFlags{Flags: flags, Flavors: flavors}
		e.d.Config.Sites[executorSite] = site
	}
	accepted := []struct {
		name               string
		flags, flavors     []string
		owner, tmpl, value string
	}{
		{"nothing set", nil, nil, "", "", ""},
		{"an allowed option", []string{"--constraint"}, nil, "", SlurmFlagsAnnotation, "--constraint=highmem"},
		{"an allowed option with its value apart", []string{"--constraint", "-C"}, nil, SlurmFlagsAnnotation, "", "-C highmem --constraint highmem"},
		{"an allowed flavor", nil, []string{"default"}, "", SlurmFlavorAnnotation, "default"},
		{"an empty flavor", nil, nil, "", SlurmFlavorAnnotation, ""},
	}
	for _, tt := range accepted {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			allow(e, tt.flags, tt.flavors)
			req := testRequest(1)
			if tt.owner != "" {
				req.Spec.OwnerAnnotations = map[string]string{tt.owner: tt.value}
			}
			if tt.tmpl != "" {
				req.Spec.PodSets[0].Template.Annotations = map[string]string{tt.tmpl: tt.value}
			}
			e.submit(req)
			if got := e.plugin.createdPods()[0].Annotations[flagsKey]; !strings.HasSuffix(got, flavorFlags) {
				t.Fatalf("flags %q: the flavor's are lost", got)
			}
		})
	}

	refused := []struct {
		name           string
		flags, flavors []string
		owner          map[string]string
		tmpl           map[string]string
		want           string
	}{
		{"any option", nil, nil, nil, map[string]string{SlurmFlagsAnnotation: "--nodes=2"},
			`the Slurm options come from the site, the pod's GPUs and its time limit: slurm-job.vk.io/flags may set no option; PodSet main has "--nodes=2"`},
		{"an option of the owner", []string{"--constraint"}, nil, map[string]string{SlurmFlagsAnnotation: "--constraint=x --exclusive"}, nil,
			`slurm-job.vk.io/flags may set only --constraint; PodSet main has "--constraint=x --exclusive"`},
		{"a value alone", []string{"--constraint"}, nil, nil, map[string]string{SlurmFlagsAnnotation: "highmem"}, "may set only --constraint"},
		{"a short option", []string{"--nodes"}, nil, nil, map[string]string{SlurmFlagsAnnotation: "-N 2"}, `PodSet main has "-N 2"`},
		{"a flavor", nil, []string{"default"}, nil, map[string]string{SlurmFlavorAnnotation: "bigmem"},
			`slurm-job.vk.io/flavor may name only default; PodSet main has "bigmem"`},
		{"any flavor", nil, nil, map[string]string{SlurmFlavorAnnotation: "default"}, nil, "slurm-job.vk.io/flavor may name no flavor"},
	}
	for _, tt := range refused {
		t.Run(tt.name, func(t *testing.T) {
			e := newExecutorEnv(t)
			allow(e, tt.flags, tt.flavors)
			req := testRequest(1)
			req.Spec.OwnerAnnotations = tt.owner
			req.Spec.PodSets[0].Template.Annotations = tt.tmpl
			e.refusedWith(e.take(req), tt.want)
		})
	}
}

func TestValidateSiteConfig(t *testing.T) {
	good := SiteResources{GPUResource: gpuResource, GPUAliases: []string{gpuAlias}, CPUsPerGPU: 20, GPUsPerMember: 4}
	if err := validateSiteResources(&good); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SiteResources){
		"no GPU resource":    func(r *SiteResources) { r.GPUResource = "" },
		"no CPUs per GPU":    func(r *SiteResources) { r.CPUsPerGPU = 0 },
		"no GPUs per member": func(r *SiteResources) { r.GPUsPerMember = 0 },
		"alias of itself":    func(r *SiteResources) { r.GPUAliases = []string{gpuResource} },
		"alias twice":        func(r *SiteResources) { r.GPUAliases = []string{gpuAlias, gpuAlias} },
	} {
		r := good
		r.GPUAliases = append([]string(nil), good.GPUAliases...)
		mutate(&r)
		if err := validateSiteResources(&r); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	c := Config{
		InterLink: InterLinkConfig{URL: "http://localhost:3000"},
		Executor:  ExecutorConfig{Namespace: executorNS},
		Sites:     map[string]SiteConfig{executorSite: {AllowedAccounts: []string{executorAccount}, AllowedFlags: &AllowedFlags{Flags: []string{"--constraint=x"}}}},
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "AllowedFlags") {
		t.Fatalf("an option with a value in AllowedFlags: %v", err)
	}
	c.Sites[executorSite] = SiteConfig{AllowedAccounts: []string{executorAccount}, Resources: &SiteResources{GPUResource: gpuResource}}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "Resources") {
		t.Fatalf("Resources without CPUs per GPU: %v", err)
	}
}

// The site's sizing, its flag lock and the copy-back timings as an executor.yaml
// writes them.
func TestLoadConfigSiteAndStaging(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executor.yaml")
	raw := `InterLink: {URL: http://localhost:3000}
Executor: {Namespace: alice}
Sites:
  hpc-a:
    AllowedAccounts: [proj42]
    Resources: {GPUResource: nvidia.com/gpu, GPUAliases: [example.org/hpc-gpu], CPUsPerGPU: 20, GPUsPerMember: 4}
    AllowedFlags: {Flags: [], Flavors: [default]}
Shadow: {User: alice, KeySecret: hpc-a-key}
Staging:
  Enabled: true
  Image: registry.a.example.org/stage:1
  TransferHost: transfer.hpc-a.example.org
  StageRoot: /scratch/proj42/stage
  Volumes: [{Name: shared, ClaimName: shared-claim, MountPath: /shared}]
  DeleteGrace: 0s
  StageOutTimeout: 2h
`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	site := c.Sites[executorSite]
	if r := site.Resources; r == nil || r.GPUResource != gpuResource || !slices.Equal(r.GPUAliases, []string{gpuAlias}) || r.CPUsPerGPU != 20 || r.GPUsPerMember != 4 {
		t.Fatalf("Resources %+v", site.Resources)
	}
	if f := site.AllowedFlags; f == nil || len(f.Flags) != 0 || !slices.Equal(f.Flavors, []string{"default"}) {
		t.Fatalf("AllowedFlags %+v", site.AllowedFlags)
	}
	if st := c.Staging; st.deleteGrace != 0 || st.stageOutTimeout != 2*time.Hour {
		t.Fatalf("grace %s, timeout %s", st.deleteGrace, st.stageOutTimeout)
	}

	c.Staging.DeleteGrace, c.Staging.StageOutTimeout = "", ""
	if err := validateStagingConfig(&c); err != nil || c.Staging.deleteGrace != 30*time.Second || c.Staging.stageOutTimeout != 4*time.Hour {
		t.Fatalf("defaults: grace %s, timeout %s, %v", c.Staging.deleteGrace, c.Staging.stageOutTimeout, err)
	}
	for _, bad := range []struct{ grace, timeout string }{{"-1s", ""}, {"soon", ""}, {"", "0s"}, {"", "later"}} {
		c.Staging.DeleteGrace, c.Staging.StageOutTimeout = bad.grace, bad.timeout
		if err := validateStagingConfig(&c); err == nil {
			t.Errorf("DeleteGrace %q, StageOutTimeout %q: accepted", bad.grace, bad.timeout)
		}
	}
}
