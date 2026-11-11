package node

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// The ConfigMap the API server publishes its client CA in.
const (
	authConfigMapNamespace = "kube-system"
	authConfigMapName      = "extension-apiserver-authentication"
	clientCAKey            = "client-ca-file"
)

// webhookBackoff is the retry of TokenReviews and SubjectAccessReviews, as a
// kubelet's.
var webhookBackoff = wait.Backoff{Duration: 500 * time.Millisecond, Factor: 1.5, Jitter: 0.2, Steps: 5}

// readClientCA reads the client CA the API server's own certificate, the one it
// dials kubelets with, is signed by.
func readClientCA(ctx context.Context, kube kubernetes.Interface) ([]byte, error) {
	cm, err := kube.CoreV1().ConfigMaps(authConfigMapNamespace).Get(ctx, authConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("reading the client CA: %w", err)
	}
	ca := cm.Data[clientCAKey]
	if ca == "" {
		return nil, fmt.Errorf("reading the client CA: no %s in %s/%s", clientCAKey, authConfigMapNamespace, authConfigMapName)
	}
	return []byte(ca), nil
}

// delegatingAuth authenticates as a kubelet does, by client certificates of
// clientCA and by TokenReview, and authorizes by SubjectAccessReview. Anonymous
// requests are refused.
func delegatingAuth(kube kubernetes.Interface, clientCA []byte) (authenticator.Request, authorizer.Authorizer, error) {
	ca, err := dynamiccertificates.NewStaticCAContent("client-ca", clientCA)
	if err != nil {
		return nil, nil, err
	}
	authn, _, err := authenticatorfactory.DelegatingAuthenticatorConfig{
		TokenAccessReviewClient:            kube.AuthenticationV1(),
		TokenAccessReviewTimeout:           10 * time.Second,
		WebhookRetryBackoff:                &webhookBackoff,
		CacheTTL:                           2 * time.Minute,
		ClientCertificateCAContentProvider: ca,
	}.New()
	if err != nil {
		return nil, nil, err
	}
	authz, err := authorizerfactory.DelegatingAuthorizerConfig{
		SubjectAccessReviewClient: kube.AuthorizationV1(),
		AllowCacheTTL:             5 * time.Minute,
		DenyCacheTTL:              30 * time.Second,
		WebhookRetryBackoff:       &webhookBackoff,
	}.New()
	if err != nil {
		return nil, nil, err
	}
	return authn, authz, nil
}

// authFilter lets a request through to the kubelet API once it is
// authenticated and authorized, with the attributes a kubelet asks for.
type authFilter struct {
	authn authenticator.Request
	authz authorizer.Authorizer
	// podNode is the node of a pod, empty when it is not known.
	podNode func(ctx context.Context, namespace, name string) string
	next    http.Handler
}

func (f *authFilter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	res, ok, err := f.authn.AuthenticateRequest(r)
	if err != nil || !ok {
		if err != nil {
			log.FromContext(r.Context()).V(1).Info("the kubelet API refused a request it cannot authenticate", "error", err.Error())
		}
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	attrs := f.attributes(r, res.User)
	decision, _, err := f.authz.Authorize(r.Context(), attrs)
	if err != nil {
		msg := fmt.Sprintf("Authorization error (user=%s, verb=%s, resource=%s, subresource=%s)", attrs.User.GetName(), attrs.Verb, attrs.Resource, attrs.Subresource)
		log.FromContext(r.Context()).Error(err, msg)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	if decision != authorizer.DecisionAllow {
		http.Error(w, fmt.Sprintf("Forbidden (user=%s, verb=%s, resource=%s, subresource=%s)", attrs.User.GetName(), attrs.Verb, attrs.Resource, attrs.Subresource), http.StatusForbidden)
		return
	}
	f.next.ServeHTTP(w, r)
}

// podRoutes are the kubelet API's routes of one pod, /<route>/<namespace>/<pod>/...
var podRoutes = []string{"containerLogs", "exec", "attach", "portForward", "run"}

// attributes are a kubelet's for r: resource nodes, subresource stats, metrics,
// log or spec by path, else proxy. The name is the target pod's node for the
// routes of a pod, and empty for the others, which every virtual node shares.
func (f *authFilter) attributes(r *http.Request, u user.Info) authorizer.AttributesRecord {
	verb := ""
	switch r.Method {
	case http.MethodGet:
		verb = "get"
	case http.MethodPost:
		verb = "create"
	case http.MethodPut:
		verb = "update"
	case http.MethodPatch:
		verb = "patch"
	case http.MethodDelete:
		verb = "delete"
	}
	attrs := authorizer.AttributesRecord{
		User:            u,
		Verb:            verb,
		APIVersion:      "v1",
		Resource:        "nodes",
		Subresource:     "proxy",
		ResourceRequest: true,
		Path:            r.URL.Path,
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	switch parts[0] {
	case "stats":
		attrs.Subresource = "stats"
	case "metrics":
		attrs.Subresource = "metrics"
	case "logs":
		attrs.Subresource = "log"
	case "spec":
		attrs.Subresource = "spec"
	}
	for _, route := range podRoutes {
		if parts[0] == route && len(parts) >= 3 && f.podNode != nil {
			attrs.Name = f.podNode(r.Context(), parts[1], parts[2])
		}
	}
	return attrs
}
