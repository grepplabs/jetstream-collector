package openbaoprovider

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
)

const (
	authMethodToken = "token"
	envConfig       = "BAO_CONFIG"
)

const environmentBootstrapConfig = `
address: ${env:BAO_ADDR}
namespace: ${env:BAO_NAMESPACE:-}
auth:
  method: ${env:BAO_AUTH_METHOD:-token}
  token: ${env:BAO_TOKEN}
tls:
  ca_cert: ${env:BAO_CACERT:-}
  client_cert: ${env:BAO_CLIENT_CERT:-}
  client_key: ${env:BAO_CLIENT_KEY:-}
watch:
  enabled: ${env:BAO_WATCH_ENABLED:-false}
  interval: ${env:BAO_WATCH_INTERVAL:-30s}
`

// Config contains the bootstrap configuration used to connect to OpenBao.
type Config struct {
	Address   string      `mapstructure:"address"`
	Namespace string      `mapstructure:"namespace"`
	Auth      AuthConfig  `mapstructure:"auth"`
	TLS       TLSConfig   `mapstructure:"tls"`
	Watch     WatchConfig `mapstructure:"watch"`
}

// AuthConfig contains OpenBao authentication settings.
type AuthConfig struct {
	Method string `mapstructure:"method"`
	Token  string `mapstructure:"token"`
}

// TLSConfig contains files used to establish a TLS connection to OpenBao.
type TLSConfig struct {
	CACert     string `mapstructure:"ca_cert"`
	ClientCert string `mapstructure:"client_cert"`
	ClientKey  string `mapstructure:"client_key"`
}

// WatchConfig controls optional OpenBao KV v2 change watching.
type WatchConfig struct {
	Enabled  bool          `mapstructure:"enabled"`
	Interval time.Duration `mapstructure:"interval"`
}

// NewDefaultConfig returns the default OpenBao provider configuration.
func NewDefaultConfig() Config {
	return Config{
		Auth:  AuthConfig{Method: authMethodToken},
		Watch: WatchConfig{Interval: defaultWatchInterval},
	}
}

// Validate checks whether the bootstrap configuration is usable.
func (cfg Config) Validate() error {
	var errs error
	if cfg.Address == "" {
		errs = errors.Join(errs, errors.New("address is required"))
	} else if parsed, err := url.Parse(cfg.Address); err != nil || parsed.Scheme == "" || parsed.Host == "" {
		errs = errors.Join(errs, errors.New("address must be an absolute URL"))
	} else if parsed.Scheme != "http" && parsed.Scheme != "https" {
		errs = errors.Join(errs, errors.New("address scheme must be http or https"))
	}
	if cfg.Auth.Method != authMethodToken {
		errs = errors.Join(errs, fmt.Errorf("auth.method must be %q for this version", "token"))
	}
	if cfg.Auth.Token == "" {
		errs = errors.Join(errs, errors.New("auth.token is required for token authentication"))
	}
	if (cfg.TLS.ClientCert == "") != (cfg.TLS.ClientKey == "") {
		errs = errors.Join(errs, errors.New("tls.client_cert and tls.client_key must be set together"))
	}
	if cfg.Watch.Enabled && cfg.Watch.Interval <= 0 {
		errs = errors.Join(errs, errors.New("watch.interval must be greater than zero when watching is enabled"))
	}
	return errs
}

func ConfigFromEnvironment() (Config, error) {
	uri := "yaml:" + environmentBootstrapConfig
	if path := os.Getenv(envConfig); path != "" {
		uri = "file:" + filepath.Clean(path)
	}
	return loadConfig(uri)
}

func loadConfig(uri string) (Config, error) {
	ctx := context.Background()
	resolver, err := confmap.NewResolver(confmap.ResolverSettings{
		URIs: []string{uri},
		ProviderFactories: []confmap.ProviderFactory{
			fileprovider.NewFactory(),
			envprovider.NewFactory(),
			yamlprovider.NewFactory(),
		},
		DefaultScheme: "file",
	})
	if err != nil {
		return Config{}, fmt.Errorf("create bootstrap config resolver: %w", err)
	}

	resolved, resolveErr := resolver.Resolve(ctx)
	shutdownErr := resolver.Shutdown(ctx)
	if resolveErr != nil {
		return Config{}, fmt.Errorf("resolve bootstrap config: %w", resolveErr)
	}
	if shutdownErr != nil {
		return Config{}, fmt.Errorf("shutdown bootstrap config resolver: %w", shutdownErr)
	}

	cfg := NewDefaultConfig()
	if err := resolved.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode bootstrap config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
