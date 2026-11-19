package s3jetstreamreceiver

import (
	"fmt"
	"strings"
	"time"

	shared "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	s3client "github.com/grepplabs/jetstream-collector/pkg/s3"
	protocol "github.com/grepplabs/jetstream-collector/pkg/s3jetstream"
	"go.opentelemetry.io/collector/component"
)

type CredentialsConfig = s3client.S3CredentialsConfig
type S3Config struct {
	Endpoint       string            `mapstructure:"endpoint"`
	Region         string            `mapstructure:"region"`
	Bucket         string            `mapstructure:"bucket"`
	Prefix         string            `mapstructure:"prefix"`
	Secure         bool              `mapstructure:"secure"`
	ForcePathStyle bool              `mapstructure:"force_path_style"`
	Credentials    CredentialsConfig `mapstructure:"credentials"`
}
type JetStreamConfig struct {
	URL        string                  `mapstructure:"url"`
	Stream     string                  `mapstructure:"stream"`
	Consumer   string                  `mapstructure:"consumer"`
	Subject    string                  `mapstructure:"subject"`
	TLS        shared.TLSConfig        `mapstructure:"tls"`
	Auth       shared.AuthConfig       `mapstructure:"auth"`
	Connection shared.ConnectionConfig `mapstructure:"connection"`
}
type ProcessingConfig struct {
	Workers       int           `mapstructure:"workers"`
	MaxObjectSize int64         `mapstructure:"max_object_size"`
	Timeout       time.Duration `mapstructure:"timeout"`
	RetryBackoff  time.Duration `mapstructure:"retry_backoff"`
	AckProgress   bool          `mapstructure:"ack_progress"`
}
type CleanupConfig struct {
	Subject string `mapstructure:"subject"`
}
type Config struct {
	S3         S3Config               `mapstructure:"s3"`
	JetStream  JetStreamConfig        `mapstructure:"jetstream"`
	Processing ProcessingConfig       `mapstructure:"processing"`
	Cleanup    CleanupConfig          `mapstructure:"cleanup"`
	Bootstrap  shared.BootstrapConfig `mapstructure:"bootstrap"`
	Signal     protocol.Signal        `mapstructure:"signal"`
}

const defaultMaxObjectSize int64 = 32 * 1024 * 1024

var _ component.Config = (*Config)(nil)

func NewDefaultConfig() *Config {
	return &Config{S3: S3Config{Region: s3client.DefaultRegion, Secure: true}, JetStream: JetStreamConfig{URL: shared.DefaultURL}, Processing: ProcessingConfig{Workers: 1, MaxObjectSize: defaultMaxObjectSize, Timeout: 2 * time.Minute, RetryBackoff: time.Second}}
}
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}
	if strings.TrimSpace(c.S3.Bucket) == "" {
		return fmt.Errorf("s3.bucket is required")
	}
	if (c.S3.Credentials.AccessKeyID == "") != (c.S3.Credentials.SecretAccessKey == "") {
		return fmt.Errorf("S3 access_key_id and secret_access_key must be set together")
	}
	if c.S3.Credentials.AccessKeyID == "" && c.S3.Credentials.Provider == (component.ID{}) {
		return fmt.Errorf("S3 credentials or credentials.provider are required")
	}
	if err := protocol.ValidatePrefix(c.S3.Prefix); err != nil {
		return fmt.Errorf("s3.prefix: %w", err)
	}
	if strings.TrimSpace(c.JetStream.URL) == "" || strings.TrimSpace(c.JetStream.Stream) == "" || strings.TrimSpace(c.JetStream.Consumer) == "" || strings.TrimSpace(c.JetStream.Subject) == "" {
		return fmt.Errorf("jetstream.url, stream, consumer, and subject are required")
	}
	if strings.TrimSpace(c.Cleanup.Subject) == "" {
		return fmt.Errorf("cleanup.subject is required")
	}
	switch c.Signal {
	case protocol.SignalLogs, protocol.SignalMetrics, protocol.SignalTraces, protocol.SignalProfiles:
	default:
		return fmt.Errorf("signal must be one of traces, metrics, logs, or profiles")
	}
	if c.Processing.Workers < 1 {
		return fmt.Errorf("processing.workers must be greater than zero")
	}
	if c.Processing.MaxObjectSize < 0 {
		return fmt.Errorf("processing.max_object_size must not be negative")
	}
	if c.Processing.MaxObjectSize == 0 {
		c.Processing.MaxObjectSize = defaultMaxObjectSize
	}
	if c.Processing.Timeout <= 0 {
		return fmt.Errorf("processing.timeout must be greater than zero")
	}
	if c.Processing.RetryBackoff < 0 {
		return fmt.Errorf("processing.retry_backoff must not be negative")
	}
	c.Bootstrap.ApplyReceiverDefaults(c.JetStream.Subject, c.JetStream.Consumer)
	if err := c.Bootstrap.Validate(); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	return nil
}
