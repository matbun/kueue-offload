package executor

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/matbun/kueue-offload/pkg/shadow"
)

// Config is the executor's configuration file.
type Config struct {
	// Listen is the address the executor's HTTP API, for the shadows, listens on.
	// Empty: ":8000".
	Listen    string          `yaml:"Listen"`
	InterLink InterLinkConfig `yaml:"InterLink"`
	Executor  ExecutorConfig  `yaml:"Executor"`
	// Sites holds the executor's only site, by name.
	Sites   map[string]SiteConfig `yaml:"Sites"`
	Shadow  ShadowConfig          `yaml:"Shadow"`
	Staging StagingConfig         `yaml:"Staging"`
}

// InterLinkConfig is the interLink API server the executor drives the plugin
// through, the one a virtual kubelet would call.
type InterLinkConfig struct {
	// URL is the API server's address, e.g. http://localhost:3000.
	URL string `yaml:"URL"`
}

// ExecutorConfig is the executor's own behaviour.
type ExecutorConfig struct {
	// Namespace is the only namespace the executor takes OffloadRequests in.
	// Empty: the namespace of its ServiceAccount.
	Namespace string `yaml:"Namespace"`
	// PollInterval is how often the executor reads the state of each job and
	// writes it, with its heartbeat, to the job's OffloadRequest. It must not be
	// longer than the OffloadConfig's pollInterval. Empty: 15s.
	PollInterval string `yaml:"PollInterval"`
	// KubernetesAPI is where a job that carries a ServiceAccount token reaches the API
	// server, through its reverse tunnel: the address the kubelet would have put in
	// KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT. Empty Host: not set.
	KubernetesAPI ExecutorKubernetesAPI `yaml:"KubernetesAPI"`
	// AutomountServiceAccountToken puts a token of the template's ServiceAccount in the
	// service account directory the executor adds to templates that do not opt out (see
	// withServiceAccountDir). False: the directory holds the namespace and the
	// cluster's CA only, so no cluster credential reaches the site unless the template
	// asks for one with its own projected volume.
	AutomountServiceAccountToken bool `yaml:"AutomountServiceAccountToken"`
	// LostTimeout is how long a submitted member may stay missing from the plugin's
	// status before its job counts as vanished. Empty: 10m.
	LostTimeout string `yaml:"LostTimeout"`

	pollInterval time.Duration
	lostTimeout  time.Duration
}

// ExecutorKubernetesAPI is the API server's address as a job sees it.
type ExecutorKubernetesAPI struct {
	Host string `yaml:"Host"`
	Port string `yaml:"Port"`
}

// SiteConfig is one site: which accounts this executor may charge. How the plugin
// reaches the site is the plugin's configuration.
type SiteConfig struct {
	AllowedAccounts []string `yaml:"AllowedAccounts"`
}

// ShadowConfig is what the shadow manager renders a workload's shadow with.
type ShadowConfig struct {
	Enabled bool `yaml:"Enabled"`
	// AlwaysShadow gives a shadow, for its logs, to jobs that expose no port.
	// Unset: true.
	AlwaysShadow *bool  `yaml:"AlwaysShadow"`
	Image        string `yaml:"Image"`
	// ExecutorImage runs the job-logs container. Empty: env EXECUTOR_IMAGE.
	ExecutorImage string `yaml:"ExecutorImage"`
	// ExecutorURL is where job-logs reaches the executor.
	ExecutorURL string `yaml:"ExecutorURL"`
	// LoginHost, User and Port are the site's login node, which the tunnel
	// reaches with the key of KeySecret. Empty LoginHost: the shadows only stream
	// logs, and requests with exposed ports or reverse forwards are refused.
	LoginHost           string `yaml:"LoginHost"`
	User                string `yaml:"User"`
	Port                int    `yaml:"Port"`
	KeySecret           string `yaml:"KeySecret"`
	KeySecretKey        string `yaml:"KeySecretKey"`
	KnownHostsConfigMap string `yaml:"KnownHostsConfigMap"`
	NodeHostKeys        string `yaml:"NodeHostKeys"`
	ForwardTarget       string `yaml:"ForwardTarget"`
	ForwardMode         string `yaml:"ForwardMode"`
	// ExecConnectCommand relays a connection in ForwardMode exec. Empty: nc.
	ExecConnectCommand string `yaml:"ExecConnectCommand"`
	NodeWaitTimeout    string `yaml:"NodeWaitTimeout"`
	// Reverse lets jobs open connections to listed cluster endpoints through the
	// shadow, as the virtual kubelet's Network.SSH.Reverse, plus AllowedTargets.
	Reverse shadow.ReverseConfig `yaml:"Reverse"`
}

// StagingConfig copies the paths of a workload's interlink.eu/stage-in annotation
// to the site before its job is submitted, and those of interlink.eu/stage-out
// back once it ended, each with a batch Job in the executor's namespace that runs
// Command. The job finds the staged inputs at their own paths only through binds
// of the site's stage directory in the plugin's configuration (for Slurm,
// SingularityDefaultOptions): that is deployment configuration, the executor does
// not add them.
//
// Command's contract is its environment: STAGE_DIRECTION (in or out),
// STAGE_PATHS (':'-separated), REMOTE (<User>@<TransferHost>), STAGE_ROOT,
// STAGE_NAMESPACE, SSH_KEY (the mounted key), and for a path on a Kerberos volume
// KEYTAB, PRINCIPAL and KRB5CCNAME.
type StagingConfig struct {
	Enabled bool   `yaml:"Enabled"`
	Image   string `yaml:"Image"`
	// Command runs the copy. Empty: /usr/local/bin/stage-rsync.
	Command      string `yaml:"Command"`
	TransferHost string `yaml:"TransferHost"`
	// User is the site user the copy logs in as. Empty: Shadow.User.
	User string `yaml:"User"`
	// StageRoot is the site's stage directory; Command keeps each namespace's
	// paths under it.
	StageRoot string `yaml:"StageRoot"`
	// KeySecret, KeySecretKey and KnownHostsConfigMap default to the Shadow's.
	KeySecret           string `yaml:"KeySecret"`
	KeySecretKey        string `yaml:"KeySecretKey"`
	KnownHostsConfigMap string `yaml:"KnownHostsConfigMap"`
	// Volumes are the claims the staged paths live on: a path must be under
	// one of their mount paths.
	Volumes []StagingVolume `yaml:"Volumes"`
	// Kerberos is used for the paths on a volume marked Kerberos.
	Kerberos *StagingKerberos `yaml:"Kerberos"`
}

// StagingVolume is a PersistentVolumeClaim of the namespace, mounted in the
// stage Job at MountPath.
type StagingVolume struct {
	Name      string `yaml:"Name"`
	ClaimName string `yaml:"ClaimName"`
	MountPath string `yaml:"MountPath"`
	// Kerberos: reading or writing the volume needs a ticket.
	Kerberos bool `yaml:"Kerberos"`
}

// StagingKerberos is the keytab the stage Job gets its ticket with.
type StagingKerberos struct {
	KeytabSecret string `yaml:"KeytabSecret"`
	KeytabKey    string `yaml:"KeytabKey"`
	Principal    string `yaml:"Principal"`
	// Krb5ConfigMap holds krb5.conf. Empty: the image's.
	Krb5ConfigMap string `yaml:"Krb5ConfigMap"`
}

const defaultStageCommand = "/usr/local/bin/stage-rsync"

const (
	defaultListen       = ":8000"
	defaultPollInterval = 15 * time.Second
	defaultLostTimeout  = 10 * time.Minute
)

// serviceAccountNamespaceFile is where a pod reads its own namespace.
var serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// AlwaysShadowEnabled reports Shadow.AlwaysShadow, true when unset.
func (s ShadowConfig) AlwaysShadowEnabled() bool {
	return s.AlwaysShadow == nil || *s.AlwaysShadow
}

// Site returns the executor's only site.
func (c Config) Site() (string, SiteConfig) {
	for name, site := range c.Sites {
		return name, site
	}
	return "", SiteConfig{}
}

// LoadConfig reads the configuration file at path, refusing unknown keys, and
// checks it with Validate.
func LoadConfig(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

// Validate fills the defaults and refuses a configuration the executor cannot run
// with.
func (c *Config) Validate() error {
	if c.Listen == "" {
		c.Listen = defaultListen
	}
	u, err := url.Parse(c.InterLink.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("InterLink.URL %q is not an http(s) URL", c.InterLink.URL)
	}
	c.InterLink.URL = strings.TrimRight(c.InterLink.URL, "/")

	if len(c.Sites) != 1 {
		return fmt.Errorf("Sites needs exactly one entry, found %d: run one executor per site", len(c.Sites))
	}
	for name, site := range c.Sites {
		if len(site.AllowedAccounts) == 0 {
			return fmt.Errorf("Sites.%s: AllowedAccounts is empty, every OffloadRequest would be refused", name)
		}
	}

	if c.Executor.Namespace == "" {
		raw, err := os.ReadFile(serviceAccountNamespaceFile)
		if err != nil {
			return fmt.Errorf("Executor.Namespace is empty and the ServiceAccount namespace cannot be read: %w", err)
		}
		c.Executor.Namespace = strings.TrimSpace(string(raw))
	}
	for _, t := range []struct {
		key, value string
		into       *time.Duration
		def        time.Duration
	}{
		{"PollInterval", c.Executor.PollInterval, &c.Executor.pollInterval, defaultPollInterval},
		{"LostTimeout", c.Executor.LostTimeout, &c.Executor.lostTimeout, defaultLostTimeout},
	} {
		*t.into = t.def
		if t.value == "" {
			continue
		}
		d, err := time.ParseDuration(t.value)
		if err != nil || d <= 0 {
			return fmt.Errorf("Executor.%s %q: a positive duration is required", t.key, t.value)
		}
		*t.into = d
	}

	if c.Shadow.Enabled {
		cfg := c.shadowConfig()
		if err := shadow.NormalizeConfig(&cfg); err != nil {
			return err
		}
	}
	if c.Shadow.Reverse.Enabled && !c.Shadow.Enabled {
		return fmt.Errorf("Shadow.Reverse.Enabled needs Shadow.Enabled: the shadow carries the tunnel")
	}
	return validateStagingConfig(c)
}

// validateStagingConfig fills the Staging block's defaults and checks it.
func validateStagingConfig(c *Config) error {
	st := &c.Staging
	if !st.Enabled {
		return nil
	}
	if st.Command == "" {
		st.Command = defaultStageCommand
	}
	if st.User == "" {
		st.User = c.Shadow.User
	}
	if st.KeySecret == "" {
		st.KeySecret = c.Shadow.KeySecret
	}
	if st.KeySecretKey == "" {
		st.KeySecretKey = c.Shadow.KeySecretKey
	}
	if st.KeySecretKey == "" {
		st.KeySecretKey = shadow.DefaultKeySecretKey
	}
	if st.KnownHostsConfigMap == "" {
		st.KnownHostsConfigMap = c.Shadow.KnownHostsConfigMap
	}
	switch {
	case st.Image == "":
		return fmt.Errorf("Staging.Image is required")
	case st.TransferHost == "":
		return fmt.Errorf("Staging.TransferHost is required")
	case st.User == "":
		return fmt.Errorf("Staging.User is required (or Shadow.User)")
	case st.StageRoot == "":
		return fmt.Errorf("Staging.StageRoot is required")
	case st.KeySecret == "":
		return fmt.Errorf("Staging.KeySecret is required (or Shadow.KeySecret)")
	case len(st.Volumes) == 0:
		return fmt.Errorf("Staging.Volumes is empty: every staged path would be refused")
	}
	names := map[string]bool{}
	for _, v := range st.Volumes {
		switch {
		case v.Name == "" || v.ClaimName == "":
			return fmt.Errorf("Staging.Volumes: Name and ClaimName are required")
		case names[v.Name]:
			return fmt.Errorf("Staging.Volumes: %s appears twice", v.Name)
		case !path.IsAbs(v.MountPath) || path.Clean(v.MountPath) != v.MountPath || v.MountPath == "/":
			return fmt.Errorf("Staging.Volumes.%s: MountPath %q is not a clean absolute path below /", v.Name, v.MountPath)
		case v.Kerberos && st.Kerberos == nil:
			return fmt.Errorf("Staging.Volumes.%s needs Kerberos, but Staging.Kerberos is not set", v.Name)
		}
		names[v.Name] = true
	}
	if k := st.Kerberos; k != nil && (k.KeytabSecret == "" || k.KeytabKey == "" || k.Principal == "") {
		return fmt.Errorf("Staging.Kerberos: KeytabSecret, KeytabKey and Principal are required")
	}
	return nil
}
