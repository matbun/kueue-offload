package executor

import (
	"bytes"
	"encoding/json"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The plugin gets what a virtual kubelet sends for a pod: env resolved, envFrom
// turned into env, and each container the Secrets, ConfigMaps and projected
// files of the volumes it mounts.
func TestReferencesReachThePlugin(t *testing.T) {
	secret := &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "hf-token", Namespace: executorNS}, Data: map[string][]byte{"token": []byte("hf_s3cr3t"), "user": []byte("alice")}}
	creds := &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: executorNS}, Data: map[string][]byte{"key": []byte("AKIA"), "LR": []byte("from-secret")}}
	config := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "train-config", Namespace: executorNS}, Data: map[string]string{"lr": "0.001", "LR": "from-configmap", "EPOCHS": "3"}}
	e := newExecutorEnv(t, secret, creds, config)
	req := testRequest(1)
	ps := &req.Spec.PodSets[0]
	ps.SecretRefs = []string{"hf-token", "s3-creds"}
	ps.ConfigMapRefs = []string{"absent", "train-config"}
	ps.Template.Labels = map[string]string{"team": "ml"}
	optional := true
	ps.Template.Spec.Volumes = []v1.Volume{
		{Name: "cfg", VolumeSource: v1.VolumeSource{ConfigMap: &v1.ConfigMapVolumeSource{LocalObjectReference: v1.LocalObjectReference{Name: "train-config"}}}},
		{Name: "creds", VolumeSource: v1.VolumeSource{Secret: &v1.SecretVolumeSource{SecretName: "s3-creds"}}},
		{Name: "mix", VolumeSource: v1.VolumeSource{Projected: &v1.ProjectedVolumeSource{Sources: []v1.VolumeProjection{
			{Secret: &v1.SecretProjection{LocalObjectReference: v1.LocalObjectReference{Name: "hf-token"}, Items: []v1.KeyToPath{{Key: "token", Path: "hf/token"}}}},
			{ConfigMap: &v1.ConfigMapProjection{LocalObjectReference: v1.LocalObjectReference{Name: "absent"}, Optional: &optional}},
		}}}},
		{Name: "podinfo", VolumeSource: v1.VolumeSource{DownwardAPI: &v1.DownwardAPIVolumeSource{Items: []v1.DownwardAPIVolumeFile{
			{Path: "labels", FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.labels"}},
			{Path: "name", FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.name"}},
		}}}},
	}
	c := &ps.Template.Spec.Containers[0]
	c.VolumeMounts = []v1.VolumeMount{{Name: "cfg", MountPath: "/cfg"}, {Name: "creds", MountPath: "/creds"}, {Name: "mix", MountPath: "/mix"}, {Name: "podinfo", MountPath: "/etc/podinfo"}}
	c.Env = []v1.EnvVar{
		{Name: "HF_TOKEN", ValueFrom: &v1.EnvVarSource{SecretKeyRef: &v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "hf-token"}, Key: "token"}}},
		{Name: "LR", ValueFrom: &v1.EnvVarSource{ConfigMapKeyRef: &v1.ConfigMapKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "train-config"}, Key: "lr"}}},
		{Name: "POD", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "TEAM", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.labels['team']"}}},
		{Name: "OPT", ValueFrom: &v1.EnvVarSource{SecretKeyRef: &v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "hf-token"}, Key: "absent", Optional: ptrBool(true)}}},
	}
	c.EnvFrom = []v1.EnvFromSource{
		{ConfigMapRef: &v1.ConfigMapEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: "train-config"}}},
		{Prefix: "S3_", SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: "s3-creds"}}},
		{SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: "s3-creds"}}},
		{ConfigMapRef: &v1.ConfigMapEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: "absent"}, Optional: &optional}},
	}
	e.submit(req)

	pod := e.plugin.createdPods()[0]
	got := pod.Spec.Containers[0]
	if len(got.EnvFrom) != 0 {
		t.Errorf("envFrom reached the plugin: %v", got.EnvFrom)
	}
	env := envOf(got)
	for k, want := range map[string]string{
		"HF_TOKEN": "hf_s3cr3t", "LR": "0.001", "POD": "train-0", "TEAM": "ml", RankEnv: "0",
		"EPOCHS": "3", "S3_key": "AKIA", "S3_LR": "from-secret", "key": "AKIA",
	} {
		if env[k] != want {
			t.Errorf("env %s = %q, want %q", k, env[k], want)
		}
	}
	if _, ok := env["OPT"]; ok {
		t.Errorf("an optional missing key must leave the variable out")
	}
	for _, e := range got.Env {
		if e.ValueFrom != nil {
			t.Errorf("env %s reached the plugin unresolved", e.Name)
		}
	}

	data := e.plugin.container(t, 0, executorCtn)
	if len(data.ConfigMaps) != 1 || data.ConfigMaps[0].Name != "train-config" || data.ConfigMaps[0].Data["lr"] != "0.001" {
		t.Errorf("ConfigMaps of the container: %+v", data.ConfigMaps)
	}
	if len(data.Secrets) != 1 || data.Secrets[0].Name != "s3-creds" {
		t.Errorf("Secrets of the container: %+v", data.Secrets)
	}
	if v, ok := e.projectedFile(0, executorCtn, "mix", "hf/token"); !ok || v != "hf_s3cr3t" {
		t.Errorf("projected Secret item %q (%v)", v, ok)
	}
	if v, _ := e.projectedFile(0, executorCtn, "podinfo", "labels"); !bytes.Contains([]byte(v), []byte(`team="ml"`)) {
		t.Errorf("downward API labels %q", v)
	}
	if v, _ := e.projectedFile(0, executorCtn, "podinfo", "name"); v != "train-0" {
		t.Errorf("downward API name %q", v)
	}
	for _, vol := range pod.Spec.Volumes {
		if vol.Name == "podinfo" && (vol.DownwardAPI != nil || vol.Projected == nil) {
			t.Errorf("the downward API volume is not a projected one, as the virtual kubelet sends it: %+v", vol.VolumeSource)
		}
	}
	if raw, _ := json.Marshal(e.request()); bytes.Contains(raw, []byte("hf_s3cr3t")) {
		t.Errorf("the request holds the secret's value")
	}
}

// A container that mounts nothing gets no Secret: the API server hands each
// container only the data of its own volumes.
func TestContainerGetsOnlyItsVolumes(t *testing.T) {
	secret := &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "hf-token", Namespace: executorNS}, Data: map[string][]byte{"token": []byte("x")}}
	e := newExecutorEnv(t, secret)
	req := testRequest(1)
	ps := &req.Spec.PodSets[0]
	ps.SecretRefs = []string{"hf-token"}
	ps.Template.Spec.Volumes = []v1.Volume{{Name: "tok", VolumeSource: v1.VolumeSource{Secret: &v1.SecretVolumeSource{SecretName: "hf-token"}}}}
	ps.Template.Spec.Containers = append(ps.Template.Spec.Containers, v1.Container{Name: "sidecar", Image: busyboxImage})
	ps.Template.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{{Name: "tok", MountPath: "/tok"}}
	ps.Template.Spec.AutomountServiceAccountToken = ptrBool(false)
	e.submit(req)
	if s := e.plugin.container(t, 0, executorCtn).Secrets; len(s) != 1 {
		t.Errorf("main: %d Secrets", len(s))
	}
	if s := e.plugin.container(t, 0, "sidecar").Secrets; len(s) != 0 {
		t.Errorf("sidecar got %d Secrets it does not mount", len(s))
	}
}
