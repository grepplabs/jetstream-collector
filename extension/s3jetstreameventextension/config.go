package s3jetstreameventextension

import (
	"fmt"
	"mime"
	"strings"
	"time"
	"unicode"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/grepplabs/jetstream-collector/pkg/s3event"
	"go.opentelemetry.io/collector/component"
)

type Config struct {
	URL                  string                           `mapstructure:"url"`
	Subject              string                           `mapstructure:"subject"`
	Timeout              time.Duration                    `mapstructure:"timeout"`
	MsgID                bool                             `mapstructure:"msg_id"`
	Headers              map[string]string                `mapstructure:"headers"`
	CloudEventsMediaType string                           `mapstructure:"cloud_events_media_type"`
	Bootstrap            sharedjetstream.BootstrapConfig  `mapstructure:"bootstrap"`
	Connection           sharedjetstream.ConnectionConfig `mapstructure:"connection"`
	TLS                  sharedjetstream.TLSConfig        `mapstructure:"tls"`
	Auth                 sharedjetstream.AuthConfig       `mapstructure:"auth"`
}

var _ component.Config = (*Config)(nil)

func (cfg *Config) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if strings.TrimSpace(cfg.URL) == "" {
		return fmt.Errorf("url is required")
	}
	if err := validateSubject(cfg.Subject); err != nil {
		return fmt.Errorf("subject: %w", err)
	}
	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be greater than zero")
	}
	if cfg.CloudEventsMediaType == "" {
		cfg.CloudEventsMediaType = s3event.CloudEventsMediaType
	}
	mediaType, _, err := mime.ParseMediaType(cfg.CloudEventsMediaType)
	if err != nil {
		return fmt.Errorf("cloud_events_media_type: %w", err)
	}
	cfg.CloudEventsMediaType = strings.ToLower(mediaType)
	if err := cfg.Bootstrap.Validate(); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	return nil
}

func NewDefaultConfig() *Config {
	return &Config{
		URL:                  sharedjetstream.DefaultURL,
		Subject:              "s3.object.created",
		Timeout:              5 * time.Second,
		MsgID:                true,
		Headers:              map[string]string{},
		CloudEventsMediaType: s3event.CloudEventsMediaType,
	}
}

func validateSubject(subject string) error {
	if subject == "" || strings.TrimSpace(subject) != subject {
		return fmt.Errorf("must be nonempty and have no surrounding whitespace")
	}
	for _, r := range subject {
		if unicode.IsSpace(r) || unicode.IsControl(r) || r == '*' || r == '>' {
			return fmt.Errorf("contains an invalid publish subject character")
		}
	}
	for token := range strings.SplitSeq(subject, ".") {
		if token == "" {
			return fmt.Errorf("must not contain empty tokens")
		}
	}
	return nil
}
