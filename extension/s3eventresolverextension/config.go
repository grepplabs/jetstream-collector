package s3eventresolverextension

import (
	"fmt"
	"mime"
	"strings"

	s3client "github.com/grepplabs/jetstream-collector/pkg/s3"
	"github.com/grepplabs/jetstream-collector/pkg/s3event"
	"go.opentelemetry.io/collector/component"
)

type CredentialsConfig = s3client.S3CredentialsConfig

type Config struct {
	Endpoint             string            `mapstructure:"endpoint"`
	Region               string            `mapstructure:"region"`
	Bucket               string            `mapstructure:"bucket"`
	Secure               bool              `mapstructure:"secure"`
	ForcePathStyle       bool              `mapstructure:"force_path_style"`
	Credentials          CredentialsConfig `mapstructure:"credentials"`
	MaxObjectSize        int64             `mapstructure:"max_object_size"`
	CloudEventsMediaType string            `mapstructure:"cloud_events_media_type"`
}

var _ component.Config = (*Config)(nil)

const defaultMaxObjectSize int64 = 32 * 1024 * 1024

func NewDefaultConfig() *Config {
	return &Config{
		Region:               s3client.DefaultRegion,
		Secure:               true,
		MaxObjectSize:        defaultMaxObjectSize,
		CloudEventsMediaType: s3event.CloudEventsMediaType,
	}
}

func (cfg *Config) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if cfg.CloudEventsMediaType == "" {
		cfg.CloudEventsMediaType = s3event.CloudEventsMediaType
	}
	mediaType, _, err := mime.ParseMediaType(cfg.CloudEventsMediaType)
	if err != nil {
		return fmt.Errorf("cloud_events_media_type: %w", err)
	}
	cfg.CloudEventsMediaType = strings.ToLower(mediaType)
	if strings.TrimSpace(cfg.Bucket) == "" {
		return fmt.Errorf("bucket is required")
	}
	if (cfg.Credentials.AccessKeyID == "") != (cfg.Credentials.SecretAccessKey == "") {
		return fmt.Errorf("credentials.access_key_id and credentials.secret_access_key must be set together")
	}
	if cfg.Credentials.AccessKeyID == "" && cfg.Credentials.Provider == (component.ID{}) {
		return fmt.Errorf("credentials or credentials.provider are required")
	}
	if cfg.MaxObjectSize < 0 {
		return fmt.Errorf("max_object_size must not be negative")
	}
	if cfg.MaxObjectSize == 0 {
		cfg.MaxObjectSize = defaultMaxObjectSize
	}
	return nil
}
