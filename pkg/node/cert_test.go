package node

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"net"
	"slices"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	ctrl "sigs.k8s.io/controller-runtime"
)

const controllerUser = "system:serviceaccount:kueue-offload:kueue-offload"

func csrPEM(t *testing.T, cn string, org ...string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: cn, Organization: org}, IPAddresses: []net.IP{net.ParseIP(testHostIP)},
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})
}

func testCSR(name, user, signer string, request []byte) *certificatesv1.CertificateSigningRequest {
	return &certificatesv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: certificatesv1.CertificateSigningRequestSpec{
			Username: user, SignerName: signer, Request: request,
			Usages: []certificatesv1.KeyUsage{certificatesv1.UsageDigitalSignature, certificatesv1.UsageServerAuth},
		},
	}
}

// Only the controller's own pending kubelet serving request for the kubelet
// API's subject is approved.
func TestApprovable(t *testing.T) {
	ours := csrPEM(t, servingCommonName, "system:nodes")
	approved := testCSR("approved", controllerUser, certificatesv1.KubeletServingSignerName, ours)
	approved.Status.Conditions = []certificatesv1.CertificateSigningRequestCondition{{Type: certificatesv1.CertificateApproved, Status: corev1.ConditionTrue}}
	cases := []struct {
		name string
		csr  *certificatesv1.CertificateSigningRequest
		want bool
	}{
		{"ours", testCSR("ours", controllerUser, certificatesv1.KubeletServingSignerName, ours), true},
		{"another user", testCSR("other-user", "system:node:worker-1", certificatesv1.KubeletServingSignerName, ours), false},
		{"another node's subject", testCSR("other-cn", controllerUser, certificatesv1.KubeletServingSignerName,
			csrPEM(t, "system:node:worker-1", "system:nodes")), false},
		{"another organization", testCSR("other-org", controllerUser, certificatesv1.KubeletServingSignerName,
			csrPEM(t, servingCommonName, "system:masters")), false},
		{"another signer", testCSR("client", controllerUser, certificatesv1.KubeAPIServerClientKubeletSignerName, ours), false},
		{"already approved", approved, false},
		{"not a request", testCSR("garbage", controllerUser, certificatesv1.KubeletServingSignerName, []byte("garbage")), false},
	}
	for _, c := range cases {
		if got := approvable(c.csr, controllerUser); got != c.want {
			t.Errorf("%s: approvable %v, want %v", c.name, got, c.want)
		}
	}
	if approvable(testCSR("ours", controllerUser, certificatesv1.KubeletServingSignerName, ours), "") {
		t.Error("approved without knowing the controller's user")
	}
}

// The approver learns its own user from a SelfSubjectReview and approves its
// request, and not another's.
func TestCSRApproverApproves(t *testing.T) {
	ours := testCSR("csr-ours", controllerUser, certificatesv1.KubeletServingSignerName, csrPEM(t, servingCommonName, "system:nodes"))
	theirs := testCSR("csr-theirs", "system:serviceaccount:alice:default", certificatesv1.KubeletServingSignerName, csrPEM(t, servingCommonName, "system:nodes"))
	kube := k8sfake.NewClientset(ours.DeepCopy(), theirs.DeepCopy())
	kube.PrependReactor("create", "selfsubjectreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authenticationv1.SelfSubjectReview{Status: authenticationv1.SelfSubjectReviewStatus{
			UserInfo: authenticationv1.UserInfo{Username: controllerUser},
		}}, nil
	})
	var calls []apiCall
	a := &CSRApprover{Client: fakeClient(t, &calls, ours, theirs), Kube: kube}
	for _, name := range []string{"csr-ours", "csr-theirs"} {
		if _, err := a.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: name}}); err != nil {
			t.Fatal(err)
		}
	}
	for name, want := range map[string]bool{"csr-ours": true, "csr-theirs": false} {
		csr, err := kube.CertificatesV1().CertificateSigningRequests().Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got := slices.ContainsFunc(csr.Status.Conditions, func(c certificatesv1.CertificateSigningRequestCondition) bool {
			return c.Type == certificatesv1.CertificateApproved && c.Status == corev1.ConditionTrue
		})
		if got != want {
			t.Errorf("%s approved %v, want %v", name, got, want)
		}
	}
}
