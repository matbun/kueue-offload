package membership

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

type rbacDoc struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Rules []rbacv1.PolicyRule `json:"rules"`
}

func readRBAC(t *testing.T, glob string) []rbacDoc {
	t.Helper()
	files, err := filepath.Glob(glob)
	if err != nil || len(files) == 0 {
		t.Fatalf("no RBAC files for %s: %v", glob, err)
	}
	var docs []rbacDoc
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, raw := range strings.Split(string(data), "\n---") {
			var d rbacDoc
			if err := yaml.Unmarshal([]byte(raw), &d); err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			if d.Kind == "ClusterRole" || d.Kind == "Role" {
				docs = append(docs, d)
			}
		}
	}
	return docs
}

func has(list []string, v string) bool {
	for _, x := range list {
		if x == v || x == "*" {
			return true
		}
	}
	return false
}

// TestMembershipRBAC fails when the membership controller's RBAC reaches what
// the design forbids: anything on Secrets, ConfigMap reads outside its own
// namespace, escalation, or binding roles other than the agent's.
func TestMembershipRBAC(t *testing.T) {
	docs := readRBAC(t, "../../config/rbac/membership/*.yaml")
	sawBind, sawConfigMapRead := false, false
	for _, d := range docs {
		for _, r := range d.Rules {
			for _, list := range [][]string{r.APIGroups, r.Resources, r.Verbs} {
				for _, v := range list {
					if v == "*" {
						t.Errorf("%s %s: wildcard in %v", d.Kind, d.Metadata.Name, r)
					}
				}
			}
			core := has(r.APIGroups, "")
			for _, res := range r.Resources {
				name := strings.SplitN(res, "/", 2)[0]
				if core && name == "secrets" {
					t.Errorf("%s %s grants %v on %s", d.Kind, d.Metadata.Name, r.Verbs, res)
				}
				if core && name == "configmaps" && (has(r.Verbs, "get") || has(r.Verbs, "list") || has(r.Verbs, "watch")) {
					if d.Kind != "Role" || d.Metadata.Namespace != ownNS {
						t.Errorf("%s %s reads configmaps outside %s", d.Kind, d.Metadata.Name, ownNS)
					}
					sawConfigMapRead = true
				}
				if core && name == "pods" && (has(r.Verbs, "create") || has(r.Verbs, "get")) {
					t.Errorf("%s %s grants %v on pods", d.Kind, d.Metadata.Name, r.Verbs)
				}
				if has(r.Verbs, "delete") && !(has(r.APIGroups, "kueue.x-k8s.io") && name == "localqueues") && !(d.Kind == "Role" && name == "leases") {
					t.Errorf("%s %s grants delete on %s", d.Kind, d.Metadata.Name, res)
				}
				if name == "clusterroles" || name == "roles" {
					if has(r.Verbs, "create") || has(r.Verbs, "update") || has(r.Verbs, "patch") {
						t.Errorf("%s %s may write %s", d.Kind, d.Metadata.Name, res)
					}
				}
			}
			for _, v := range []string{"escalate", "impersonate"} {
				if has(r.Verbs, v) {
					t.Errorf("%s %s grants %s", d.Kind, d.Metadata.Name, v)
				}
			}
			if has(r.Verbs, "bind") {
				names := append([]string(nil), r.ResourceNames...)
				sort.Strings(names)
				if strings.Join(names, ",") != "interlink-agent,system:auth-delegator" || strings.Join(r.Resources, ",") != "clusterroles" {
					t.Errorf("bind on %v %v, want clusterroles interlink-agent and system:auth-delegator only", r.Resources, r.ResourceNames)
				}
				sawBind = true
			}
		}
	}
	if !sawBind || !sawConfigMapRead {
		t.Errorf("bind %v, configmap read %v: the lint read the wrong files", sawBind, sawConfigMapRead)
	}
}

// TestAgentClusterRole checks the role tenant RoleBindings point at: the agent
// reads Secrets and ConfigMaps of its namespace and never writes Secrets, mints
// ServiceAccount tokens without managing ServiceAccounts, and runs its staging
// Jobs.
func TestAgentClusterRole(t *testing.T) {
	docs := readRBAC(t, "../../config/rbac/agent/*.yaml")
	if len(docs) != 1 || docs[0].Kind != "ClusterRole" || docs[0].Metadata.Name != "interlink-agent" {
		t.Fatalf("agent role docs %+v", docs)
	}
	read := map[string]bool{}
	patchPods, mintTokens := false, false
	var jobVerbs []string
	for _, r := range docs[0].Rules {
		if has(r.APIGroups, "batch") && has(r.Resources, "jobs") {
			jobVerbs = append(jobVerbs, r.Verbs...)
		}
		if !has(r.APIGroups, "") {
			continue
		}
		if has(r.Resources, "serviceaccounts/token") && has(r.Verbs, "create") {
			mintTokens = true
		}
		if has(r.Resources, "serviceaccounts") {
			t.Errorf("agent may %v serviceaccounts", r.Verbs)
		}
		if has(r.Resources, "pods") && has(r.Verbs, "patch") {
			patchPods = true
		}
		for _, res := range r.Resources {
			if res == "*" || has(r.Verbs, "*") {
				t.Errorf("wildcard rule %v", r)
			}
			if res == "secrets" && (has(r.Verbs, "create") || has(r.Verbs, "update") || has(r.Verbs, "patch") || has(r.Verbs, "delete")) {
				t.Errorf("agent may write secrets: %v", r.Verbs)
			}
			if res == "pods" && has(r.Verbs, "create") {
				t.Errorf("agent may create pods")
			}
			if has(r.Verbs, "get") {
				read[res] = true
			}
		}
	}
	for _, res := range []string{"secrets", "configmaps", "pods"} {
		if !read[res] {
			t.Errorf("agent cannot read %s", res)
		}
	}
	if !patchPods {
		t.Errorf("agent cannot patch pods: a node change reaches the shadow only at the kubelet's next ConfigMap refresh")
	}
	if !mintTokens {
		t.Errorf("agent cannot create serviceaccounts/token: pipeline steps get no token")
	}
	sort.Strings(jobVerbs)
	if got := strings.Join(jobVerbs, ","); got != "create,delete,get,list,watch" {
		t.Errorf("agent verbs on batch jobs = %s, want create,delete,get,list,watch", got)
	}
}
