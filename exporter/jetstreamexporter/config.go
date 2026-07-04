package jetstreamexporter

import (
	"fmt"
	"time"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type MetricsBucketsConfig struct {
	PublishDuration []float64 `mapstructure:"publish_duration"`
	PayloadSize     []float64 `mapstructure:"payload_size"`
}

type Config struct {
	URL             string                                                   `mapstructure:"url"`
	Subject         string                                                   `mapstructure:"subject"`
	IncludeSubject  bool                                                     `mapstructure:"include_subject"`
	Compression     string                                                   `mapstructure:"compression"`
	ContentType     string                                                   `mapstructure:"content_type"`
	MsgID           bool                                                     `mapstructure:"msg_id"`
	Headers         map[string]string                                        `mapstructure:"headers"`
	SubjectPattern  string                                                   `mapstructure:"subject_pattern"`
	MetadataHeaders []string                                                 `mapstructure:"metadata_headers"`
	MetricsBuckets  MetricsBucketsConfig                                     `mapstructure:"metrics_buckets"`
	Bootstrap       sharedjetstream.BootstrapConfig                          `mapstructure:"bootstrap"`
	TimeoutSettings exporterhelper.TimeoutConfig                             `mapstructure:",squash"`
	RetryOnFailure  configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
	SendingQueue    configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	TLS             sharedjetstream.TLSConfig                                `mapstructure:"tls"`
	Auth            sharedjetstream.AuthConfig                               `mapstructure:"auth"`
}

var _ component.Config = (*Config)(nil)

func defaultMetricsBucketsConfig() MetricsBucketsConfig {
	return MetricsBucketsConfig{
		PublishDuration: append([]float64(nil), defaultPublishDurationBuckets...),
		PayloadSize:     append([]float64(nil), defaultPayloadSizeBuckets...),
	}
}

func (cfg MetricsBucketsConfig) withDefaults() MetricsBucketsConfig {
	defaults := defaultMetricsBucketsConfig()
	if len(cfg.PublishDuration) == 0 {
		cfg.PublishDuration = defaults.PublishDuration
	}
	if len(cfg.PayloadSize) == 0 {
		cfg.PayloadSize = defaults.PayloadSize
	}
	return cfg
}

func (cfg *Config) metricsBuckets() MetricsBucketsConfig {
	if cfg == nil {
		return defaultMetricsBucketsConfig()
	}
	return cfg.MetricsBuckets.withDefaults()
}

func NewDefaultConfig() *Config {
	return &Config{
		URL:             sharedjetstream.DefaultURL,
		Compression:     sharedjetstream.CompressionNone,
		ContentType:     sharedjetstream.ContentTypeProto,
		MetricsBuckets:  defaultMetricsBucketsConfig(),
		TimeoutSettings: exporterhelper.NewDefaultTimeoutConfig(),
		RetryOnFailure: configretry.BackOffConfig{
			Enabled:             false,
			InitialInterval:     1 * time.Second,
			RandomizationFactor: 0.2,
			Multiplier:          1.5,
			MaxInterval:         5 * time.Second,
			// Keep JetStream publish retry windows short so republished messages do not wait too long for consumer AckWait handling.
			MaxElapsedTime: 10 * time.Second,
		},
		SendingQueue:    configoptional.None[exporterhelper.QueueBatchConfig](),
		Headers:         map[string]string{},
		MetadataHeaders: []string{},
	}
}

func validateHistogramBuckets(buckets []float64, field string) error {
	if len(buckets) == 0 {
		return nil
	}
	prev := buckets[0]
	if prev <= 0 {
		return fmt.Errorf("%s must contain positive bucket boundaries", field)
	}
	for i := 1; i < len(buckets); i++ {
		if buckets[i] <= 0 {
			return fmt.Errorf("%s must contain positive bucket boundaries", field)
		}
		if buckets[i] <= prev {
			return fmt.Errorf("%s must be strictly increasing", field)
		}
		prev = buckets[i]
	}
	return nil
}

func (cfg *Config) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if cfg.URL == "" {
		return fmt.Errorf("url is required")
	}
	if cfg.Subject == "" && cfg.SubjectPattern == "" {
		return fmt.Errorf("subject or subject_pattern is required")
	}
	if cfg.SubjectPattern != "" {
		if _, err := newSubjectTemplate(cfg.SubjectPattern); err != nil {
			return fmt.Errorf("subject_pattern: %w", err)
		}
	}
	if err := cfg.Bootstrap.Validate(); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	if err := cfg.RetryOnFailure.Validate(); err != nil {
		return fmt.Errorf("retry_on_failure: %w", err)
	}
	if err := cfg.SendingQueue.Validate(); err != nil {
		return fmt.Errorf("sending_queue: %w", err)
	}

	compression, err := sharedjetstream.NormalizeCompression(cfg.Compression)
	if err != nil {
		return fmt.Errorf("compression: %w", err)
	}
	cfg.Compression = compression

	contentType, err := sharedjetstream.NormalizeContentType(cfg.ContentType)
	if err != nil {
		return fmt.Errorf("content_type: %w", err)
	}
	cfg.ContentType = contentType
	buckets := cfg.metricsBuckets()
	if err := validateHistogramBuckets(buckets.PublishDuration, "metrics_buckets.publish_duration"); err != nil {
		return err
	}
	if err := validateHistogramBuckets(buckets.PayloadSize, "metrics_buckets.payload_size"); err != nil {
		return err
	}

	return nil
}
