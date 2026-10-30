package membership

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// A Job whose pod template changed cannot be applied over the old one, so it is
// replaced; any other object's refusal is returned as it is.
func TestChangedJobTemplateReplacesTheJob(t *testing.T) {
	s := testScheme(t)
	old := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "remote", Namespace: "proj42"},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "c", Image: "old"}},
		}}},
	}
	deletes := 0
	c := fake.NewClientBuilder().WithScheme(s).WithRESTMapper(testMapper(s)).WithObjects(old).
		WithTypeConverters(managedfields.NewDeducedTypeConverter()).
		WithInterceptorFuncs(interceptor.Funcs{
			Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
				if err := c.Get(ctx, types.NamespacedName{Namespace: "proj42", Name: "remote"}, &batchv1.Job{}); err == nil {
					return apierrors.NewInvalid(schema.GroupKind{Group: "batch", Kind: "Job"}, "remote",
						field.ErrorList{field.Invalid(field.NewPath("spec", "template"), nil, "field is immutable")})
				}
				return c.Apply(ctx, obj, opts...)
			},
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				deletes++
				return c.Delete(ctx, obj, opts...)
			},
		}).Build()
	r := &Reconciler{Client: c}
	job := old.DeepCopy()
	job.ResourceVersion = ""
	job.Spec.Template.Spec.Containers[0].Image = "new"
	if err := r.apply(context.Background(), job, batchv1.SchemeGroupVersion.WithKind("Job")); err != nil {
		t.Fatal(err)
	}
	got := &batchv1.Job{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "proj42", Name: "remote"}, got); err != nil {
		t.Fatal(err)
	}
	if deletes != 1 || got.Spec.Template.Spec.Containers[0].Image != "new" {
		t.Fatalf("deletes %d, image %q", deletes, got.Spec.Template.Spec.Containers[0].Image)
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "remote", Namespace: "proj42"}}
	if err := r.apply(context.Background(), cm, corev1.SchemeGroupVersion.WithKind("ConfigMap")); !apierrors.IsInvalid(err) || deletes != 1 {
		t.Fatalf("a refused ConfigMap: err %v, deletes %d", err, deletes)
	}
}
