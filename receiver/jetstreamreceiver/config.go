package jetstreamreceiver

import (
	"fmt"
	"strings"
	"time"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"go.opentelemetry.io/collector/component"
)

type processingMode string

const (
	processingModeSingle processingMode = "single"
	processingModeBatch  processingMode = "batch"

	defaultBatchMaxMessages int32 = 16
	defaultBatchMaxWait           = 500 * time.Millisecond
)

type MetricsBucketsConfig struct {
	ConsumeDuration []float64 `mapstructure:"consume_duration"`
	PayloadSize     []float64 `mapstructure:"payload_size"`
}

type Config struct {
	URL                 string                          `mapstructure:"url"`
	Stream              string                          `mapstructure:"stream"`
	Subject             string                          `mapstructure:"subject"`
	IncludeSubject      bool                            `mapstructure:"include_subject"`
	ConsumerName        string                          `mapstructure:"consumer_name"`
	ProcessingMode      string                          `mapstructure:"processing_mode"`
	Workers             int                             `mapstructure:"workers"`
	BatchMaxMessages    int32                           `mapstructure:"batch_max_messages"`
	BatchMaxWait        time.Duration                   `mapstructure:"batch_max_wait"`
	BatchGroupBySubject bool                            `mapstructure:"batch_group_by_subject"`
	Compression         string                          `mapstructure:"compression"`
	ConsumeRetryDelay   time.Duration                   `mapstructure:"consume_retry_delay"`
	MetricsBuckets      MetricsBucketsConfig            `mapstructure:"metrics_buckets"`
	Bootstrap           sharedjetstream.BootstrapConfig `mapstructure:"bootstrap"`
	TLS                 sharedjetstream.TLSConfig       `mapstructure:"tls"`
	Auth                sharedjetstream.AuthConfig      `mapstructure:"auth"`
}

func (cfg *Config) consumerName() (string, error) {
	name := strings.TrimSpace(cfg.ConsumerName)
	if name == "" {
		return "", fmt.Errorf("consumer_name is required")
	}
	return name, nil
}

func (cfg *Config) processingMode() processingMode {
	mode := strings.ToLower(strings.TrimSpace(cfg.ProcessingMode))
	if mode == "" {
		return processingModeSingle
	}
	return processingMode(mode)
}

var _ component.Config = (*Config)(nil)

func defaultMetricsBucketsConfig() MetricsBucketsConfig {
	return MetricsBucketsConfig{
		ConsumeDuration: append([]float64(nil), defaultConsumeDurationBuckets...),
		PayloadSize:     append([]float64(nil), defaultPayloadSizeBuckets...),
	}
}

func (cfg MetricsBucketsConfig) withDefaults() MetricsBucketsConfig {
	defaults := defaultMetricsBucketsConfig()
	if len(cfg.ConsumeDuration) == 0 {
		cfg.ConsumeDuration = defaults.ConsumeDuration
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
		URL:                 sharedjetstream.DefaultURL,
		ProcessingMode:      string(processingModeSingle),
		BatchMaxMessages:    defaultBatchMaxMessages,
		BatchMaxWait:        defaultBatchMaxWait,
		BatchGroupBySubject: false,
		Compression:         sharedjetstream.CompressionNone,
		ConsumeRetryDelay:   time.Second,
		MetricsBuckets:      defaultMetricsBucketsConfig(),
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
	if cfg.URL == "" {
		return fmt.Errorf("url is required")
	}
	if cfg.Stream == "" {
		return fmt.Errorf("stream is required")
	}
	if cfg.Subject == "" {
		return fmt.Errorf("subject is required")
	}
	if _, err := cfg.consumerName(); err != nil {
		return err
	}

	mode := cfg.processingMode()
	switch mode {
	case processingModeSingle, processingModeBatch:
	default:
		return fmt.Errorf("processing_mode must be one of %q or %q", processingModeSingle, processingModeBatch)
	}

	cfg.Bootstrap.ApplyReceiverDefaults(cfg.Subject, cfg.ConsumerName)
	if err := cfg.Bootstrap.Validate(); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	if cfg.Workers < 0 {
		return fmt.Errorf("workers must be greater than or equal to zero")
	}
	if mode == processingModeBatch {
		if cfg.BatchMaxMessages <= 0 {
			return fmt.Errorf("batch_max_messages must be greater than zero in batch mode")
		}
		if cfg.BatchMaxWait <= 0 {
			return fmt.Errorf("batch_max_wait must be greater than zero in batch mode")
		}
	}
	if cfg.ConsumeRetryDelay < 0 {
		return fmt.Errorf("consume_retry_delay must be greater than or equal to zero")
	}
	if _, err := sharedjetstream.NormalizeCompression(cfg.Compression); err != nil {
		return fmt.Errorf("compression: %w", err)
	}
	buckets := cfg.metricsBuckets()
	if err := validateHistogramBuckets(buckets.ConsumeDuration, "metrics_buckets.consume_duration"); err != nil {
		return err
	}
	if err := validateHistogramBuckets(buckets.PayloadSize, "metrics_buckets.payload_size"); err != nil {
		return err
	}
	return nil
}
