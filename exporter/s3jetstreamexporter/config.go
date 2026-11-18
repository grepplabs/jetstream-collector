package s3jetstreamexporter

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
type TenantConfig struct {
	ID          string `mapstructure:"id"`
	MetadataKey string `mapstructure:"metadata_key"`
}
type JetStreamConfig struct {
	URL                    string                  `mapstructure:"url"`
	Subject                string                  `mapstructure:"subject"`
	SubjectFromMetadataKey string                  `mapstructure:"subject_from_metadata_key"`
	TLS                    shared.TLSConfig        `mapstructure:"tls"`
	Auth                   shared.AuthConfig       `mapstructure:"auth"`
	Connection             shared.ConnectionConfig `mapstructure:"connection"`
}
type ObjectConfig struct {
	Compression string `mapstructure:"compression"`
}
type Config struct {
	S3        S3Config        `mapstructure:"s3"`
	Tenant    TenantConfig    `mapstructure:"tenant"`
	JetStream JetStreamConfig `mapstructure:"jetstream"`
	Object    ObjectConfig    `mapstructure:"object"`
	Signal    protocol.Signal `mapstructure:"signal"`
	Timeout   time.Duration   `mapstructure:"timeout"`
}

var _ component.Config = (*Config)(nil)

func NewDefaultConfig() *Config {
	return &Config{S3: S3Config{Region: s3client.DefaultRegion, Secure: true}, JetStream: JetStreamConfig{URL: shared.DefaultURL}, Object: ObjectConfig{Compression: shared.CompressionNone}, Timeout: 30 * time.Second}
}

func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}
	if strings.TrimSpace(c.S3.Bucket) == "" {
		return fmt.Errorf("s3.bucket is required")
	}
	if (c.S3.Credentials.AccessKeyID == "") != (c.S3.Credentials.SecretAccessKey == "") {
		return fmt.Errorf("s3.credentials.access_key_id and secret_access_key must be set together")
	}
	if c.S3.Credentials.AccessKeyID == "" && c.S3.Credentials.Provider == (component.ID{}) {
		return fmt.Errorf("s3 credentials or credentials.provider are required")
	}
	if (strings.TrimSpace(c.Tenant.ID) == "") == (strings.TrimSpace(c.Tenant.MetadataKey) == "") {
		return fmt.Errorf("exactly one of tenant.id and tenant.metadata_key is required")
	}
	if c.Tenant.ID != "" {
		if err := protocol.ValidateTenant(c.Tenant.ID); err != nil {
			return fmt.Errorf("tenant.id: %w", err)
		}
	}
	if strings.TrimSpace(c.Tenant.MetadataKey) != c.Tenant.MetadataKey {
		return fmt.Errorf("tenant.metadata_key must not be empty or padded with whitespace")
	}
	if err := protocol.ValidatePrefix(c.S3.Prefix); err != nil {
		return fmt.Errorf("s3.prefix: %w", err)
	}
	if strings.TrimSpace(c.JetStream.URL) == "" {
		return fmt.Errorf("jetstream.url is required")
	}
	if strings.TrimSpace(c.JetStream.Subject) == "" {
		return fmt.Errorf("jetstream.subject is required")
	}
	if err := validatePublishSubject(c.JetStream.Subject); err != nil {
		return fmt.Errorf("jetstream.subject: %w", err)
	}
	if strings.TrimSpace(c.JetStream.SubjectFromMetadataKey) != c.JetStream.SubjectFromMetadataKey {
		return fmt.Errorf("jetstream.subject_from_metadata_key must not be padded with whitespace")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be greater than zero")
	}
	switch c.Signal {
	case protocol.SignalLogs, protocol.SignalMetrics, protocol.SignalTraces, protocol.SignalProfiles:
	default:
		return fmt.Errorf("signal must be one of traces, metrics, logs, or profiles")
	}
	compression, err := shared.NormalizeCompression(c.Object.Compression)
	if err != nil {
		return fmt.Errorf("object.compression: %w", err)
	}
	c.Object.Compression = compression
	return nil
}
