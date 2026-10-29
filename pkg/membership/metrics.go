package membership

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	syncs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "interlink_membership_syncs_total",
		Help: "Membership sync passes, by site and outcome.",
	}, []string{"site", "outcome"})

	membershipCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "interlink_membership_memberships",
		Help: "Memberships (namespace, account) applied in the last sync, by site.",
	}, []string{"site"})
)

func init() {
	metrics.Registry.MustRegister(syncs, membershipCount)
}
