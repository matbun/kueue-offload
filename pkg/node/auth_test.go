package node

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// testCA is a CA that signs client certificates.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// client is a client certificate of user signed by ca.
func (ca *testCA) client(t *testing.T, user string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: user},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// authEnv is the kubelet API behind the delegating authentication and
// authorization of a fake API server, which knows the token "alice-token" and
// allows only the user kubernetes. It records the SubjectAccessReviews.
func authEnv(t *testing.T, ca *testCA) (*httptest.Server, func() []authorizationv1.SubjectAccessReviewSpec) {
	t.Helper()
	var mu sync.Mutex
	var sars []authorizationv1.SubjectAccessReviewSpec
	apiserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var out any
		switch r.URL.Path {
		case "/apis/authentication.k8s.io/v1/tokenreviews":
			tr := &authenticationv1.TokenReview{}
			if err := json.NewDecoder(r.Body).Decode(tr); err != nil {
				t.Error(err)
			}
			if tr.Spec.Token == "alice-token" {
				tr.Status = authenticationv1.TokenReviewStatus{Authenticated: true, User: authenticationv1.UserInfo{Username: "alice"}}
			}
			out = tr
		case "/apis/authorization.k8s.io/v1/subjectaccessreviews":
			sar := &authorizationv1.SubjectAccessReview{}
			if err := json.NewDecoder(r.Body).Decode(sar); err != nil {
				t.Error(err)
			}
			mu.Lock()
			sars = append(sars, sar.Spec)
			mu.Unlock()
			sar.Status.Allowed = sar.Spec.User == "kubernetes"
			out = sar
		default:
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(apiserver.Close)
	kube, err := kubernetes.NewForConfig(&rest.Config{Host: apiserver.URL})
	if err != nil {
		t.Fatal(err)
	}
	authn, authz, err := delegatingAuth(kube, ca.pem)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := logsEnv(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "epoch 1\n") })
	srv := httptest.NewUnstartedServer((&Server{Provider: p}).handler(authn, authz))
	srv.TLS = &tls.Config{ClientAuth: tls.RequestClientCert, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, func() []authorizationv1.SubjectAccessReviewSpec {
		mu.Lock()
		defer mu.Unlock()
		return append([]authorizationv1.SubjectAccessReviewSpec(nil), sars...)
	}
}

func get(t *testing.T, srv *httptest.Server, path, token string, cert *tls.Certificate) (int, string) {
	t.Helper()
	c := srv.Client()
	if cert != nil {
		tr := c.Transport.(*http.Transport).Clone()
		tr.TLSClientConfig.Certificates = []tls.Certificate{*cert}
		c = &http.Client{Transport: tr}
	}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(body)
}

// The kubelet API answers the API server, which presents a client certificate
// of the cluster's client CA, as a kubelet does: 401 to anonymous requests and
// unknown credentials, 403 to a user the SubjectAccessReview denies, with the
// attributes a kubelet asks about.
func TestKubeletAPIAuth(t *testing.T) {
	ca := newTestCA(t, "cluster-client-ca")
	srv, sars := authEnv(t, ca)
	logs := "/containerLogs/" + testNS + "/train-1/main"

	if code, _ := get(t, srv, logs, "", nil); code != http.StatusUnauthorized {
		t.Errorf("anonymous: %d, want 401", code)
	}
	if code, _ := get(t, srv, logs, "stolen-token", nil); code != http.StatusUnauthorized {
		t.Errorf("unknown token: %d, want 401", code)
	}
	other := newTestCA(t, "other-ca").client(t, "kubernetes")
	if code, _ := get(t, srv, logs, "", &other); code != http.StatusUnauthorized {
		t.Errorf("certificate of another CA: %d, want 401", code)
	}
	if code, body := get(t, srv, logs, "alice-token", nil); code != http.StatusForbidden ||
		!strings.Contains(body, "Forbidden (user=alice, verb=get, resource=nodes, subresource=proxy)") {
		t.Errorf("denied user: %d %q, want 403", code, body)
	}
	apiserver := ca.client(t, "kubernetes")
	if code, body := get(t, srv, logs, "", &apiserver); code != http.StatusOK || body != "epoch 1\n" {
		t.Errorf("API server: %d %q, want the log", code, body)
	}
	if code, _ := get(t, srv, "/stats/summary", "", &apiserver); code != http.StatusOK {
		t.Errorf("stats: %d", code)
	}

	got := sars()
	if len(got) != 3 {
		t.Fatalf("SubjectAccessReviews %+v, want alice's and two of the API server", got)
	}
	podLogs, stats := got[1], got[2]
	if a := podLogs.ResourceAttributes; podLogs.User != "kubernetes" || a == nil || a.Verb != "get" || a.Resource != "nodes" ||
		a.Subresource != "proxy" || a.Name != testNode {
		t.Errorf("pod log attributes %+v %+v, want get nodes/proxy %s", podLogs, a, testNode)
	}
	if a := stats.ResourceAttributes; a == nil || a.Subresource != "stats" || a.Name != "" {
		t.Errorf("stats attributes %+v, want nodes/stats of no node", a)
	}
}
