package node

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Server is the kubelet API of every virtual node: one HTTPS listener on Port
// of the controller's pod, which the nodes all give as their address. It runs
// on every replica, not only the leader, whose address the nodes carry.
type Server struct {
	Port     int32
	Kube     kubernetes.Interface
	Provider *Provider
	Cert     ServingCert
}

// NeedLeaderElection is false: the serving certificate is per replica, and a
// new leader serves at once.
func (s *Server) NeedLeaderElection() bool { return false }

// Start serves until ctx ends. It waits for the client CA, so that a missing
// grant stops only the kubelet API, not the admission checks.
func (s *Server) Start(ctx context.Context) error {
	var ca []byte
	err := wait.PollUntilContextCancel(ctx, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		var err error
		if ca, err = readClientCA(ctx, s.Kube); err != nil {
			log.FromContext(ctx).Error(err, "the kubelet API waits for the client CA")
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return nil
	}
	authn, authz, err := delegatingAuth(s.Kube, ca)
	if err != nil {
		return err
	}
	s.Cert.Start()
	defer s.Cert.Stop()
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", s.Port),
		Handler:           s.handler(authn, authz),
		ReadHeaderTimeout: 30 * time.Second,
		TLSConfig: &tls.Config{
			ClientAuth:     tls.RequestClientCert,
			MinVersion:     tls.VersionTLS12,
			GetCertificate: s.Cert.GetCertificate,
		},
		// HTTP/1.1 only: exec, attach and port-forward upgrade the connection.
		TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){},
		BaseContext:  func(net.Listener) context.Context { return ctx },
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServeTLS("", "") }()
	log.FromContext(ctx).Info("serving the kubelet API of the virtual nodes", "port", s.Port)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdown); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func (s *Server) handler(authn authenticator.Request, authz authorizer.Authorizer) http.Handler {
	pods := vkapi.PodHandler(vkapi.PodHandlerConfig{
		GetContainerLogs:   s.Provider.GetContainerLogs,
		RunInContainer:     s.Provider.RunInContainer,
		GetStatsSummary:    s.Provider.GetStatsSummary,
		GetMetricsResource: s.Provider.GetMetricsResource,
	}, false)
	return &authFilter{authn: authn, authz: authz, podNode: s.podNode, next: pods}
}

// podNode is the node of a pod in the cache, empty when it is not there.
func (s *Server) podNode(ctx context.Context, namespace, name string) string {
	pod := &corev1.Pod{}
	if err := s.Provider.Reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pod); err != nil {
		return ""
	}
	return pod.Spec.NodeName
}
