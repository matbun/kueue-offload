package offload

import (
	"time"

	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/admissioncheck"

	"github.com/matbun/kueue-offload/api/v1alpha1"
)

// ControllerName is the AdmissionCheck controllerName the offload controller serves.
const ControllerName = "interlink.eu/offload"

// configHelper resolves an AdmissionCheck's OffloadConfig, as Kueue's own check
// controllers resolve their parameters.
type configHelper = admissioncheck.ConfigHelper[*v1alpha1.OffloadConfig, v1alpha1.OffloadConfig]

// NewConfigHelper returns the helper for the OffloadConfigs of c's scheme.
func NewConfigHelper(c client.Client) (*configHelper, error) {
	return admissioncheck.NewConfigHelper[*v1alpha1.OffloadConfig](c)
}

// site is a check's OffloadConfig with its defaults applied.
type site struct {
	// config is the OffloadConfig's name.
	config string
	// name is the OffloadWorker the check submits to.
	name                string
	pollInterval        time.Duration
	executorLostTimeout time.Duration
	// maxQueueTime is 0 without a limit.
	maxQueueTime       time.Duration
	backoffLimitCount  int32
	backoffBaseSeconds int32
	backoffMaxSeconds  int32
	flavors            map[string]v1alpha1.FlavorTarget
}

func resolveSite(sc *v1alpha1.OffloadConfig) site {
	s := site{
		config:              sc.Name,
		pollInterval:        15 * time.Second,
		executorLostTimeout: 10 * time.Minute,
		flavors:             sc.Spec.Flavors,
	}
	if len(sc.Spec.Workers) > 0 {
		s.name = sc.Spec.Workers[0]
	}
	if d := sc.Spec.PollInterval; d != nil && d.Duration > 0 {
		s.pollInterval = d.Duration
	}
	if d := sc.Spec.ExecutorLostTimeout; d != nil && d.Duration > 0 {
		s.executorLostTimeout = d.Duration
	}
	if d := sc.Spec.MaxQueueTime; d != nil && d.Duration > 0 {
		s.maxQueueTime = d.Duration
	}
	rs := ptr.Deref(sc.Spec.RetryStrategy, kueue.ProvisioningRequestRetryStrategy{})
	s.backoffLimitCount = max(ptr.Deref(rs.BackoffLimitCount, 3), 0)
	s.backoffBaseSeconds = max(ptr.Deref(rs.BackoffBaseSeconds, 60), 0)
	s.backoffMaxSeconds = max(ptr.Deref(rs.BackoffMaxSeconds, 1800), 0)
	return s
}
