package offload

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/admission"
	plugincel "k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/cel/environment"
	"sigs.k8s.io/yaml"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

const policyFile = "../../config/admission/offloadrequest-policy.yaml"

func readPolicy(t *testing.T) (*admissionregistrationv1.ValidatingAdmissionPolicy, *admissionregistrationv1.ValidatingAdmissionPolicyBinding) {
	t.Helper()
	data, err := os.ReadFile(policyFile)
	if err != nil {
		t.Fatal(err)
	}
	var p *admissionregistrationv1.ValidatingAdmissionPolicy
	var b *admissionregistrationv1.ValidatingAdmissionPolicyBinding
	for _, doc := range strings.Split(string(data), "\n---") {
		var meta metav1.TypeMeta
		if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Fatal(err)
		}
		switch meta.Kind {
		case "ValidatingAdmissionPolicy":
			p = &admissionregistrationv1.ValidatingAdmissionPolicy{}
			err = yaml.UnmarshalStrict([]byte(doc), p)
		case "ValidatingAdmissionPolicyBinding":
			b = &admissionregistrationv1.ValidatingAdmissionPolicyBinding{}
			err = yaml.UnmarshalStrict([]byte(doc), b)
		}
		if err != nil {
			t.Fatalf("%s: %v", meta.Kind, err)
		}
	}
	if p == nil || b == nil {
		t.Fatalf("%s holds no policy or no binding", policyFile)
	}
	return p, b
}

// compilePolicy compiles every expression as an API server of Kubernetes 1.30
// does when the policy is created, and builds the validator the admission
// plugin would run.
func compilePolicy(t *testing.T, p *admissionregistrationv1.ValidatingAdmissionPolicy) validating.Validator {
	t.Helper()
	c, err := plugincel.NewCompositedCompiler(environment.MustBaseEnvSet(version.MajorMinor(1, 30)))
	if err != nil {
		t.Fatal(err)
	}
	opts := plugincel.OptionalVariableDeclarations{HasAuthorizer: true}
	for _, v := range p.Spec.Variables {
		if res := c.CompileAndStoreVariable(&validating.Variable{Name: v.Name, Expression: v.Expression}, opts, environment.NewExpressions); res.Error != nil {
			t.Fatalf("variable %s: %v", v.Name, res.Error)
		}
	}
	var conds []plugincel.ExpressionAccessor
	for _, v := range p.Spec.Validations {
		vc := &validating.ValidationCondition{Expression: v.Expression, Message: v.Message, Reason: v.Reason}
		if res := c.CompileCELExpression(vc, opts, environment.NewExpressions); res.Error != nil {
			t.Fatalf("validation %q: %v", v.Expression, res.Error)
		}
		conds = append(conds, vc)
	}
	none := c.CompileCondition(nil, opts, environment.NewExpressions)
	return validating.NewValidator(c.CompileCondition(conds, opts, environment.NewExpressions), nil, none, none, p.Spec.FailurePolicy)
}

func TestOffloadRequestPolicyMatches(t *testing.T) {
	p, b := readPolicy(t)
	if p.Spec.FailurePolicy == nil || *p.Spec.FailurePolicy != admissionregistrationv1.Fail {
		t.Errorf("failurePolicy %v, want Fail", p.Spec.FailurePolicy)
	}
	if p.Spec.ParamKind != nil || len(p.Spec.MatchConditions) > 0 {
		t.Errorf("paramKind %v, matchConditions %v: the rules apply to every write", p.Spec.ParamKind, p.Spec.MatchConditions)
	}
	rules := p.Spec.MatchConstraints.ResourceRules
	if len(rules) != 1 {
		t.Fatalf("resourceRules %+v", rules)
	}
	r := rules[0]
	if !slices.Equal(r.APIGroups, []string{v1alpha1.GroupVersion.Group}) || !slices.Equal(r.Resources, []string{"offloadrequests", "offloadrequests/status"}) ||
		!slices.Equal(r.Operations, []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update}) {
		t.Errorf("resourceRule %+v, want CREATE and UPDATE of offloadrequests and offloadrequests/status", r)
	}
	if b.Spec.PolicyName != p.Name || !slices.Equal(b.Spec.ValidationActions, []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny}) ||
		b.Spec.ParamRef != nil || b.Spec.MatchResources != nil {
		t.Errorf("binding %+v, want policy %s with Deny everywhere", b.Spec, p.Name)
	}
	compilePolicy(t, p)
}

func TestOffloadRequestPolicy(t *testing.T) {
	p, _ := readPolicy(t)
	v := compilePolicy(t, p)

	const (
		ns         = "team-a"
		controller = "system:serviceaccount:kueue-offload:kueue-offload"
		executor   = "system:serviceaccount:team-a:kueue-offload-executor"
		otherExec  = "system:serviceaccount:team-b:kueue-offload-executor"
		tenant     = "alice"
		gc         = "system:serviceaccount:kube-system:generic-garbage-collector"
	)
	created := &v1alpha1.OffloadRequest{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OffloadRequest"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "job-train-5d565-hpc-a-1", Namespace: ns, UID: "q1",
			Labels:          map[string]string{v1alpha1.SiteLabel: "hpc-a"},
			Finalizers:      []string{v1alpha1.AttemptFinalizer},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "kueue.x-k8s.io/v1beta2", Kind: "Workload", Name: "job-train-5d565", UID: "w1"}},
		},
		Spec: v1alpha1.OffloadRequestSpec{
			Workload: v1alpha1.WorkloadReference{Name: "job-train-5d565", UID: "w1"}, AdmissionCheck: "hpc-a", Attempt: 1,
			Site: "hpc-a", Account: "proj42", PodSets: []v1alpha1.PodSet{{Name: "main", Count: 1}},
		},
	}
	taken := created.DeepCopy()
	taken.Finalizers = append(taken.Finalizers, v1alpha1.ExecutorFinalizer)

	cases := []struct {
		name, user, sub string
		op              admission.Operation
		old             *v1alpha1.OffloadRequest
		change          func(q *v1alpha1.OffloadRequest)
		deny            string
	}{
		{name: "the controller creates", user: controller, op: admission.Create},
		{name: "a tenant creates", user: tenant, op: admission.Create, deny: "creates OffloadRequests"},
		{name: "the executor creates", user: executor, op: admission.Create, deny: "creates OffloadRequests"},
		{name: "the executor takes it", user: executor, op: admission.Update, old: created,
			change: func(q *v1alpha1.OffloadRequest) { q.Finalizers = append(q.Finalizers, v1alpha1.ExecutorFinalizer) }},
		{name: "the executor of another namespace takes it", user: otherExec, op: admission.Update, old: created,
			change: func(q *v1alpha1.OffloadRequest) { q.Finalizers = append(q.Finalizers, v1alpha1.ExecutorFinalizer) },
			deny:   "finalizer offload.interlink.eu/executor"},
		{name: "the executor writes the status", user: executor, op: admission.Update, sub: "status", old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Status.JobID = "123" }},
		{name: "the controller writes the status", user: controller, op: admission.Update, sub: "status", old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Status.JobID = "123" }, deny: "writes its status"},
		{name: "a tenant writes the status", user: tenant, op: admission.Update, sub: "status", old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Status.JobID = "123" }, deny: "writes its status"},
		{name: "the executor of another namespace writes the status", user: otherExec, op: admission.Update, sub: "status", old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Status.JobID = "123" }, deny: "writes its status"},
		{name: "a tenant changes the account", user: tenant, op: admission.Update, old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Spec.Account = "proj99" }, deny: "changes an OffloadRequest's spec"},
		{name: "the executor changes the spec", user: executor, op: admission.Update, old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Spec.PodSets[0].Count = 4 }, deny: "changes an OffloadRequest's spec"},
		{name: "a tenant moves it to another site", user: tenant, op: admission.Update, old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Labels[v1alpha1.SiteLabel] = "hpc-b" }, deny: "site label"},
		{name: "the controller releases the attempt", user: controller, op: admission.Update, old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Finalizers = []string{v1alpha1.ExecutorFinalizer} }},
		{name: "the executor releases the attempt", user: executor, op: admission.Update, old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Finalizers = []string{v1alpha1.ExecutorFinalizer} }, deny: "finalizer offload.interlink.eu/attempt"},
		{name: "the executor releases its finalizer", user: executor, op: admission.Update, old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Finalizers = []string{v1alpha1.AttemptFinalizer} }},
		{name: "the controller releases the executor's finalizer", user: controller, op: admission.Update, old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Finalizers = []string{v1alpha1.AttemptFinalizer} }, deny: "finalizer offload.interlink.eu/executor"},
		{name: "a tenant releases the executor's finalizer", user: tenant, op: admission.Update, old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.Finalizers = []string{v1alpha1.AttemptFinalizer} }, deny: "finalizer offload.interlink.eu/executor"},
		{name: "the garbage collector orphans it", user: gc, op: admission.Update, old: taken,
			change: func(q *v1alpha1.OffloadRequest) { q.OwnerReferences = nil }},
	}
	gvk := v1alpha1.GroupVersion.WithKind("OffloadRequest")
	gvr := v1alpha1.GroupVersion.WithResource("offloadrequests")
	toObject := func(q *v1alpha1.OffloadRequest) runtime.Object {
		if q == nil {
			return nil
		}
		u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(q)
		if err != nil {
			t.Fatal(err)
		}
		return &unstructured.Unstructured{Object: u}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := created.DeepCopy()
			if tc.old != nil {
				obj = tc.old.DeepCopy()
			}
			if tc.change != nil {
				tc.change(obj)
			}
			newObj, oldObj := toObject(obj), toObject(tc.old)
			attr := admission.NewAttributesRecord(newObj, oldObj, gvk, ns, obj.Name, gvr, tc.sub, tc.op, nil, false, &user.DefaultInfo{Name: tc.user})
			res := v.Validate(context.Background(), gvr, &admission.VersionedAttributes{Attributes: attr, VersionedKind: gvk, VersionedObject: newObj, VersionedOldObject: oldObj},
				nil, nil, celconfig.RuntimeCELCostBudget, nil)
			var denied []string
			for _, d := range res.Decisions {
				if d.Evaluation == validating.EvalError {
					t.Fatalf("evaluation error: %s", d.Message)
				}
				if d.Action == validating.ActionDeny {
					denied = append(denied, d.Message)
				}
			}
			switch {
			case tc.deny == "" && len(denied) > 0:
				t.Errorf("denied: %v", denied)
			case tc.deny != "" && !slices.ContainsFunc(denied, func(m string) bool { return strings.Contains(m, tc.deny) }):
				t.Errorf("denials %v, want one with %q", denied, tc.deny)
			}
		})
	}
}
