package node

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"

	authenticationv1 "k8s.io/api/authentication/v1"
	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/certificate"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// The subject and the DNS name of the serving certificate. One certificate
// serves every virtual node: the API server dials the nodes' common IP.
const (
	servingCommonName = "system:node:kueue-offload"
	servingDNSName    = "kueue-offload"
)

var servingOrganization = []string{"system:nodes"}

// ServingCert is the kubelet API's certificate.
type ServingCert interface {
	GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error)
	Start()
	Stop()
}

// NewServingCert asks the cluster for the kubelet API's serving certificate for
// the address nodeIP, through a CertificateSigningRequest, as a kubelet does with
// serverTLSBootstrap: the API server verifies a kubelet's certificate.
func NewServingCert(kube kubernetes.Interface, nodeIP string) (ServingCert, error) {
	ip := net.ParseIP(nodeIP)
	if ip == nil {
		return nil, fmt.Errorf("node IP %q is not an IP", nodeIP)
	}
	m, err := certificate.NewManager(&certificate.Config{
		ClientsetFn: func(*tls.Certificate) (kubernetes.Interface, error) { return kube, nil },
		Template: &x509.CertificateRequest{
			Subject:     pkix.Name{CommonName: servingCommonName, Organization: servingOrganization},
			DNSNames:    []string{servingDNSName},
			IPAddresses: []net.IP{ip},
		},
		SignerName:       certificatesv1.KubeletServingSignerName,
		GetUsages:        certificate.DefaultKubeletServingGetUsages,
		CertificateStore: &memoryStore{},
		Name:             "kubelet-serving",
	})
	if err != nil {
		return nil, err
	}
	return managedCert{m}, nil
}

type managedCert struct{ certificate.Manager }

func (m managedCert) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if c := m.Current(); c != nil {
		return c, nil
	}
	return nil, errors.New("no kubelet serving certificate yet: its CertificateSigningRequest is not approved and signed")
}

// memoryStore keeps the certificate in memory: the root filesystem is read
// only, and a restarted controller asks for a new one, as the virtual kubelet
// does.
type memoryStore struct {
	mu   sync.Mutex
	cert *tls.Certificate
}

func (s *memoryStore) Current() (*tls.Certificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cert == nil {
		e := certificate.NoCertKeyError("no kubelet serving certificate yet")
		return nil, &e
	}
	return s.cert, nil
}

func (s *memoryStore) Update(certPEM, keyPEM []byte) (*tls.Certificate, error) {
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	if pair.Leaf, err = x509.ParseCertificate(pair.Certificate[0]); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cert = &pair
	return s.cert, nil
}

// CSRApprover approves the CertificateSigningRequests of the kubelet API's own
// serving certificate: signer kubernetes.io/kubelet-serving, asked for by the
// offload controller's service account, for subject servingCommonName. The
// cluster approves no kubelet serving certificate by itself.
type CSRApprover struct {
	Client client.Client
	Kube   kubernetes.Interface

	mu   sync.Mutex
	user string
}

// SetupWithManager registers the approver for kubelet serving CSRs.
func (a *CSRApprover) SetupWithManager(mgr ctrl.Manager) error {
	serving := predicate.NewPredicateFuncs(func(o client.Object) bool {
		csr, ok := o.(*certificatesv1.CertificateSigningRequest)
		return ok && csr.Spec.SignerName == certificatesv1.KubeletServingSignerName
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("kubelet-serving-csr").
		For(&certificatesv1.CertificateSigningRequest{}, builder.WithPredicates(serving)).
		Complete(a)
}

func (a *CSRApprover) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	csr := &certificatesv1.CertificateSigningRequest{}
	if err := a.Client.Get(ctx, req.NamespacedName, csr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	user, err := a.self(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !approvable(csr, user) {
		return ctrl.Result{}, nil
	}
	csr.Status.Conditions = append(csr.Status.Conditions, certificatesv1.CertificateSigningRequestCondition{
		Type:           certificatesv1.CertificateApproved,
		Status:         corev1.ConditionTrue,
		Reason:         "KueueOffloadApproved",
		Message:        "the kubelet serving certificate of the offload controller's virtual nodes",
		LastUpdateTime: metav1.Now(),
	})
	if _, err := a.Kube.CertificatesV1().CertificateSigningRequests().UpdateApproval(ctx, csr.Name, csr, metav1.UpdateOptions{}); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.FromContext(ctx).Info("approved the kubelet serving certificate request", "csr", csr.Name)
	return ctrl.Result{}, nil
}

// self is the offload controller's own user, as the API server authenticates
// it.
func (a *CSRApprover) self(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.user != "" {
		return a.user, nil
	}
	review, err := a.Kube.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("reading the offload controller's own user: %w", err)
	}
	if review.Status.UserInfo.Username == "" {
		return "", errors.New("reading the offload controller's own user: the API server returned none")
	}
	a.user = review.Status.UserInfo.Username
	return a.user, nil
}

// approvable reports whether csr is a pending kubelet serving request of user
// for the kubelet API's subject.
func approvable(csr *certificatesv1.CertificateSigningRequest, user string) bool {
	if user == "" || csr.Spec.Username != user || csr.Spec.SignerName != certificatesv1.KubeletServingSignerName ||
		len(csr.Status.Conditions) > 0 || len(csr.Status.Certificate) > 0 {
		return false
	}
	block, _ := pem.Decode(csr.Spec.Request)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return false
	}
	req, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return false
	}
	return req.Subject.CommonName == servingCommonName && slices.Equal(req.Subject.Organization, servingOrganization)
}
