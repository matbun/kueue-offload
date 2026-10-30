package executor

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/containerd/containerd/log"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// Annotations of a template that list the paths to stage, ':'-separated.
const (
	StageInAnnotation  = "interlink.eu/stage-in"
	StageOutAnnotation = "interlink.eu/stage-out"
)

// Labels of a stage Job.
const (
	StageOfLabel        = "interlink.eu/stage-of"
	StageDirectionLabel = "interlink.eu/stage-direction"
)

const (
	stageIn  = "in"
	stageOut = "out"
)

// Where the stage Job's container finds its credentials.
const (
	stageKeyDir    = "/ssh"
	stageKeytabDir = "/keytab"
	stageCCache    = "FILE:/tmp/krb5cc"
)

// stagePaths returns the paths an annotation of the workload's templates lists,
// each once, in order.
func stagePaths(req jobRequest, annotation string) []string {
	var paths []string
	for _, ps := range req.PodSets {
		for _, p := range strings.Split(ps.Template.Annotations[annotation], ":") {
			if p = strings.TrimSpace(p); p != "" && !slices.Contains(paths, p) {
				paths = append(paths, p)
			}
		}
	}
	return paths
}

// stageVolume returns the configured volume a path lives on, nil for none.
func stageVolume(cfg StagingConfig, p string) *StagingVolume {
	for i, v := range cfg.Volumes {
		if strings.HasPrefix(p, v.MountPath+"/") {
			return &cfg.Volumes[i]
		}
	}
	return nil
}

// validateStaging refuses stage paths this executor cannot copy.
func (d *Executor) validateStaging(req jobRequest) *refusal {
	cfg := d.Config.Staging
	for _, ann := range []string{StageInAnnotation, StageOutAnnotation} {
		paths := stagePaths(req, ann)
		if len(paths) == 0 {
			continue
		}
		if !cfg.Enabled {
			return permanentError("%s is set, but this executor stages nothing (Staging.Enabled)", ann)
		}
		for _, p := range paths {
			trimmed := strings.TrimSuffix(p, "/")
			if !path.IsAbs(p) || path.Clean(trimmed) != trimmed {
				return permanentError("%s: %q is not a clean absolute path", ann, p)
			}
			if stageVolume(cfg, trimmed) == nil {
				mounts := make([]string, 0, len(cfg.Volumes))
				for _, v := range cfg.Volumes {
					mounts = append(mounts, v.MountPath)
				}
				return permanentError("%s: %q is not under a staging volume (%s)", ann, p, strings.Join(mounts, ", "))
			}
		}
	}
	return nil
}

// stageJobName is the name of the workload's stage Job in a direction.
func stageJobName(workloadUID, direction string) string {
	short := strings.ToLower(workloadUID)
	if len(short) > 8 {
		short = short[:8]
	}
	return "stage-" + direction + "-" + short
}

// stageJob is the batch Job that copies paths in a direction for the workload:
// Command in Image, with the volumes the paths live on mounted where the job
// sees them, the site key and, for a Kerberos volume, the keytab.
func (d *Executor) stageJob(req jobRequest, direction string, paths []string) *batchv1.Job {
	st := d.Config.Staging
	labels := map[string]string{
		StageOfLabel:              req.WorkloadUID,
		StageDirectionLabel:       direction,
		"sidecar.istio.io/inject": "false",
	}
	env := []v1.EnvVar{
		{Name: "STAGE_DIRECTION", Value: direction},
		{Name: "STAGE_PATHS", Value: strings.Join(paths, ":")},
		{Name: "REMOTE", Value: st.User + "@" + st.TransferHost},
		{Name: "STAGE_ROOT", Value: st.StageRoot},
		{Name: "STAGE_NAMESPACE", Value: req.Namespace},
		{Name: "SSH_KEY", Value: stageKeyDir + "/" + st.KeySecretKey},
	}
	keyMode := int32(0o400)
	volumes := []v1.Volume{{Name: "stage-key", VolumeSource: v1.VolumeSource{Secret: &v1.SecretVolumeSource{SecretName: st.KeySecret, DefaultMode: &keyMode}}}}
	mounts := []v1.VolumeMount{{Name: "stage-key", MountPath: stageKeyDir, ReadOnly: true}}
	if st.KnownHostsConfigMap != "" {
		volumes = append(volumes, v1.Volume{Name: "stage-known-hosts", VolumeSource: v1.VolumeSource{ConfigMap: &v1.ConfigMapVolumeSource{LocalObjectReference: v1.LocalObjectReference{Name: st.KnownHostsConfigMap}}}})
		mounts = append(mounts, v1.VolumeMount{Name: "stage-known-hosts", MountPath: "/etc/ssh/ssh_known_hosts", SubPath: "known_hosts", ReadOnly: true})
	}

	kerberos := false
	propagation := v1.MountPropagationHostToContainer
	var used []string
	for _, p := range paths {
		vol := stageVolume(st, strings.TrimSuffix(p, "/"))
		if vol == nil || slices.Contains(used, vol.Name) {
			continue
		}
		used = append(used, vol.Name)
		kerberos = kerberos || vol.Kerberos
		volumes = append(volumes, v1.Volume{Name: vol.Name, VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: vol.ClaimName}}})
		// A FUSE-backed claim mounts under the container after it started.
		mounts = append(mounts, v1.VolumeMount{Name: vol.Name, MountPath: vol.MountPath, MountPropagation: &propagation})
	}
	if k := st.Kerberos; kerberos && k != nil {
		env = append(env,
			v1.EnvVar{Name: "KEYTAB", Value: stageKeytabDir + "/" + k.KeytabKey},
			v1.EnvVar{Name: "PRINCIPAL", Value: k.Principal},
			v1.EnvVar{Name: "KRB5CCNAME", Value: stageCCache},
		)
		volumes = append(volumes, v1.Volume{Name: "stage-keytab", VolumeSource: v1.VolumeSource{Secret: &v1.SecretVolumeSource{SecretName: k.KeytabSecret, DefaultMode: &keyMode}}})
		mounts = append(mounts, v1.VolumeMount{Name: "stage-keytab", MountPath: stageKeytabDir, ReadOnly: true})
		if k.Krb5ConfigMap != "" {
			volumes = append(volumes, v1.Volume{Name: "stage-krb5", VolumeSource: v1.VolumeSource{ConfigMap: &v1.ConfigMapVolumeSource{LocalObjectReference: v1.LocalObjectReference{Name: k.Krb5ConfigMap}}}})
			mounts = append(mounts, v1.VolumeMount{Name: "stage-krb5", MountPath: "/etc/krb5.conf", SubPath: "krb5.conf", ReadOnly: true})
		}
	}

	backoff := int32(0)
	automount := false
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: stageJobName(req.WorkloadUID, direction), Namespace: req.Namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: v1.PodSpec{
					RestartPolicy:                v1.RestartPolicyNever,
					AutomountServiceAccountToken: &automount,
					Containers: []v1.Container{{
						Name:         "stage",
						Image:        st.Image,
						Command:      []string{st.Command},
						Env:          env,
						VolumeMounts: mounts,
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("50m"), v1.ResourceMemory: resource.MustParse("64Mi")},
							Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("2"), v1.ResourceMemory: resource.MustParse("2Gi")},
						},
					}},
					Volumes: volumes,
				},
			},
		},
	}
}

// ensureStageJob returns the workload's stage Job in a direction, created if
// there is none yet.
func (d *Executor) ensureStageJob(ctx context.Context, req jobRequest, direction string, paths []string) (*batchv1.Job, error) {
	if d.Kube == nil {
		return nil, fmt.Errorf("the executor has no Kubernetes client to run stage Jobs")
	}
	jobs := d.Kube.BatchV1().Jobs(req.Namespace)
	name := stageJobName(req.WorkloadUID, direction)
	job, err := jobs.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		job, err = jobs.Create(ctx, d.stageJob(req, direction, paths), metav1.CreateOptions{})
		if err == nil {
			log.L.Infof("executor: workload %s: staging %s with Job %s", req.WorkloadUID, direction, name)
		}
	}
	if err != nil {
		return nil, err
	}
	if job.Labels[StageOfLabel] != req.WorkloadUID {
		return nil, fmt.Errorf("Job %s: %w %q", name, errStageJobOfOther, job.Labels[StageOfLabel])
	}
	return job, nil
}

// stageJobResult reads a stage Job's outcome: done once it ended, failed with
// why when it did not succeed.
func stageJobResult(job *batchv1.Job) (done, failed bool, why string) {
	for _, c := range job.Status.Conditions {
		if c.Status != v1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return true, false, ""
		case batchv1.JobFailed:
			return true, true, strings.TrimPrefix(c.Reason+": "+c.Message, ": ")
		}
	}
	switch {
	case job.Status.Succeeded > 0:
		return true, false, ""
	case job.Status.Failed > 0:
		return true, true, fmt.Sprintf("%d pod(s) failed", job.Status.Failed)
	}
	return false, false, ""
}

// errStageJobOfOther: a stage Job of that name exists for another workload.
var errStageJobOfOther = errors.New("the name is taken by the stage Job of workload")

// stageKubeErrorIsPermanent tells a stage Job the executor will never get from a
// failure to reach the API server.
func stageKubeErrorIsPermanent(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsInvalid(err) || errors.Is(err, errStageJobOfOther)
}

// stageIn copies the workload's stage-in paths to the site before it is
// submitted, with q's StagingIn condition True while the copy runs: nil once
// that is done.
func (d *Executor) stageIn(ctx context.Context, q *v1alpha1.OffloadRequest, req jobRequest) *refusal {
	paths := stagePaths(req, StageInAnnotation)
	if len(paths) == 0 {
		return nil
	}
	job, err := d.ensureStageJob(ctx, req, stageIn, paths)
	if err != nil {
		if stageKubeErrorIsPermanent(err) {
			return permanentError("staging in: %v", err)
		}
		return transientError("staging in: %v", err)
	}
	done, failed, why := stageJobResult(job)
	switch {
	case failed:
		setCondition(q, v1alpha1.OffloadRequestStagingIn, metav1.ConditionFalse, v1alpha1.OffloadRequestFailed, why)
		return permanentError("staging in failed (Job %s): %s", job.Name, why)
	case !done:
		setCondition(q, v1alpha1.OffloadRequestStagingIn, metav1.ConditionTrue, reasonRunning, "")
		return transientError("Job %s is running", job.Name)
	}
	setCondition(q, v1alpha1.OffloadRequestStagingIn, metav1.ConditionFalse, v1alpha1.OffloadRequestSucceeded, "")
	return nil
}

// stageOut copies the stage-out paths back once the job completed or failed, and
// reports the job running and staging out until the copy ended: the outputs are
// not where the user reads them before that. A failed copy fails the job. A
// cancelled job is not copied: the offload controller deletes its request and
// submits the workload again.
func (d *Executor) stageOut(ctx context.Context, req jobRequest, st *jobState) {
	if st.State != jobCompleted && st.State != jobFailed {
		return
	}
	paths := stagePaths(req, StageOutAnnotation)
	if len(paths) == 0 {
		return
	}
	fail := func(why string) {
		st.State, st.Message = jobFailed, st.Message+"; staging out failed: "+why
		if st.Reason == "Completed" {
			st.Reason = "StageOutFailed"
		}
	}
	if !d.Config.Staging.Enabled {
		fail("Staging.Enabled is off")
		return
	}
	running := func(msg string) {
		st.State, st.StagingOut, st.Reason, st.Message, st.CompletionTime = jobRunning, true, "", msg, nil
	}
	job, err := d.ensureStageJob(ctx, req, stageOut, paths)
	if err != nil {
		if stageKubeErrorIsPermanent(err) {
			fail(err.Error())
			return
		}
		log.L.Warning("executor: workload ", req.WorkloadUID, ": staging out: ", err)
		running(err.Error())
		return
	}
	done, failed, why := stageJobResult(job)
	switch {
	case failed:
		fail("Job " + job.Name + ": " + why)
	case !done:
		running("Job " + job.Name + " is running")
	}
}

// deleteStageJobs deletes the workload's stage Jobs and their pods.
func (d *Executor) deleteStageJobs(ctx context.Context, uid string) error {
	if !d.Config.Staging.Enabled || d.Kube == nil {
		return nil
	}
	jobs := d.Kube.BatchV1().Jobs(d.Config.Executor.Namespace)
	list, err := jobs.List(ctx, metav1.ListOptions{LabelSelector: StageOfLabel + "=" + uid})
	if err != nil {
		return err
	}
	background := metav1.DeletePropagationBackground
	for _, job := range list.Items {
		if err := jobs.Delete(ctx, job.Name, metav1.DeleteOptions{PropagationPolicy: &background}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
