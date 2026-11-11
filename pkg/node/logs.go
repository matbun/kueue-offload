package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	"github.com/virtual-kubelet/virtual-kubelet/node/api/statsv1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/matbun/kueue-offload/api/v1alpha1"
	"github.com/matbun/kueue-offload/pkg/executorapi"
)

// Defaults of the OffloadConfig's executorService and executorPort, which the
// API server sets.
const (
	defaultExecutorService = "kueue-offload-executor"
	defaultExecutorPort    = 8000
)

// execUnsupported is what kubectl exec prints.
const execUnsupported = "exec into an offloaded container is not supported: the interLink plugin API has no exec call"

// Provider answers the kubelet API of the virtual nodes from the cache and the
// executors.
type Provider struct {
	// Reader reads pods, Workloads and OffloadConfigs, from the cache.
	Reader client.Reader
	// HTTP reads logs from the executors; nil is a client whose only timeout is
	// for the answer's headers, since a followed log streams for as long as the
	// job runs.
	HTTP *http.Client
	// executorURL is the base URL of the executor of namespace, for tests.
	executorURL func(cfg *v1alpha1.OffloadConfig, namespace string) string
}

func (p *Provider) httpClient() *http.Client {
	if p.HTTP != nil {
		return p.HTTP
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy, t.ResponseHeaderTimeout = nil, 5*time.Minute
	p.HTTP = &http.Client{Transport: t}
	return p.HTTP
}

// GetContainerLogs streams the log of container of the member a pod stands
// for from the executor of the pod's namespace (executorapi.GetLogsPath). An
// answer of 404 is NotFound and another 4xx InvalidInput, which the kubelet API
// answers with 404 and 400: kubectl prints the executor's message, for example
// that the container waits to start.
func (p *Provider) GetContainerLogs(ctx context.Context, namespace, podName, container string, opts vkapi.ContainerLogOpts) (io.ReadCloser, error) {
	if opts.Previous {
		return nil, errdefs.InvalidInput("an offloaded container has no previous instance")
	}
	pod, wl, cfg, err := p.member(ctx, namespace, podName)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	q.Set(executorapi.LogsWorkloadUID, string(wl.UID))
	q.Set(executorapi.LogsRank, strconv.Itoa(rankAnnotation(pod)))
	q.Set(executorapi.LogsContainer, container)
	if opts.Follow {
		q.Set(executorapi.LogsFollow, "true")
	}
	if opts.Tail > 0 {
		q.Set(executorapi.LogsTail, strconv.Itoa(opts.Tail))
	}
	if opts.SinceSeconds > 0 {
		q.Set(executorapi.LogsSinceSeconds, strconv.Itoa(opts.SinceSeconds))
	}
	if !opts.SinceTime.IsZero() {
		q.Set(executorapi.LogsSinceTime, opts.SinceTime.UTC().Format(time.RFC3339))
	}
	if opts.LimitBytes > 0 {
		q.Set(executorapi.LogsLimitBytes, strconv.Itoa(opts.LimitBytes))
	}
	if opts.Timestamps {
		q.Set(executorapi.LogsTimestamps, "true")
	}
	base := p.baseURL(cfg, namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+executorapi.GetLogsPath+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	res, err := p.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("the executor of namespace %s cannot be reached: %w", namespace, err)
	}
	if res.StatusCode == http.StatusOK {
		return res.Body, nil
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	msg := strings.TrimSpace(string(body))
	switch {
	case res.StatusCode == http.StatusNotFound:
		return nil, errdefs.NotFound(msg)
	case res.StatusCode >= 400 && res.StatusCode < 500:
		return nil, errdefs.InvalidInput(msg)
	}
	return nil, fmt.Errorf("the executor of namespace %s answered %d: %s", namespace, res.StatusCode, msg)
}

func (p *Provider) baseURL(cfg *v1alpha1.OffloadConfig, namespace string) string {
	if p.executorURL != nil {
		return p.executorURL(cfg, namespace)
	}
	svc, port := cfg.Spec.ExecutorService, cfg.Spec.ExecutorPort
	if svc == "" {
		svc = defaultExecutorService
	}
	if port == 0 {
		port = defaultExecutorPort
	}
	return fmt.Sprintf("http://%s.%s.svc:%d", svc, namespace, port)
}

// member resolves the pod podName on a virtual node to its Workload and the
// OffloadConfig of its node.
func (p *Provider) member(ctx context.Context, namespace, podName string) (*corev1.Pod, *kueue.Workload, *v1alpha1.OffloadConfig, error) {
	pod := &corev1.Pod{}
	if err := p.Reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: podName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil, errdefs.NotFoundf("pod %s/%s is not on a virtual node", namespace, podName)
		}
		return nil, nil, nil, err
	}
	if !onVirtualNode(pod) {
		return nil, nil, nil, errdefs.NotFoundf("pod %s/%s is not on a virtual node", namespace, podName)
	}
	name := pod.Annotations[v1alpha1.WorkloadAnnotation]
	if name == "" || rankAnnotation(pod) < 0 {
		return nil, nil, nil, errdefs.InvalidInputf("pod %s/%s was not offloaded: it has no %s or %s annotation", namespace, podName,
			v1alpha1.WorkloadAnnotation, v1alpha1.RankAnnotation)
	}
	wl := &kueue.Workload{}
	if err := p.Reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, wl); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil, errdefs.NotFoundf("the Workload %s of pod %s/%s is gone", name, namespace, podName)
		}
		return nil, nil, nil, err
	}
	cfg, err := p.config(ctx, pod.Spec.NodeName)
	if err != nil {
		return nil, nil, nil, err
	}
	return pod, wl, cfg, nil
}

// config is the OffloadConfig whose virtual node is node.
func (p *Provider) config(ctx context.Context, node string) (*v1alpha1.OffloadConfig, error) {
	list := &v1alpha1.OffloadConfigList{}
	if err := p.Reader.List(ctx, list); err != nil {
		return nil, err
	}
	for i := range list.Items {
		if v1alpha1.NodeName(list.Items[i].Name) == node {
			return &list.Items[i], nil
		}
	}
	return nil, errdefs.NotFoundf("no OffloadConfig has node %s", node)
}

// RunInContainer refuses: interLink's plugin API has no exec call.
func (p *Provider) RunInContainer(context.Context, string, string, string, []string, vkapi.AttachIO) error {
	return errors.New(execUnsupported)
}

// GetStatsSummary has no pod statistics: the jobs' usage is the site's. The
// virtual nodes share one address, so the request does not say which node it
// is for; the summary names the node when the controller runs only one.
func (p *Provider) GetStatsSummary(ctx context.Context) (*statsv1alpha1.Summary, error) {
	list := &v1alpha1.OffloadConfigList{}
	if err := p.Reader.List(ctx, list); err != nil {
		return nil, err
	}
	s := &statsv1alpha1.Summary{}
	if len(list.Items) == 1 {
		s.Node.NodeName = v1alpha1.NodeName(list.Items[0].Name)
	}
	return s, nil
}

// GetMetricsResource has no samples, for the same reason.
func (p *Provider) GetMetricsResource(context.Context) ([]*dto.MetricFamily, error) {
	return nil, nil
}
