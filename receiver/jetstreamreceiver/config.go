package jetstreamreceiver

import (
	"fmt"
	"math"
	"strings"
	"time"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"go.opentelemetry.io/collector/component"
)

type processingMode string

const (
	processingModeSingle processingMode = "single"
	processingModeBatch  processingMode = "batch"

	consumeRetryStrategyNone        = "none"
	consumeRetryStrategyConstant    = "constant"
	consumeRetryStrategyExponential = "exponential"

	defaultBatchMaxMessages int32 = 16
	defaultBatchMaxWait           = 500 * time.Millisecond
)

type MetricsBucketsConfig struct {
	ConsumeDuration []float64 `mapstructure:"consume_duration"`
	PayloadSize     []float64 `mapstructure:"payload_size"`
}

type ConsumeRetryConfig struct {
	Strategy     string        `mapstructure:"strategy"`
	InitialDelay time.Duration `mapstructure:"initial_delay"`
	Multiplier   float64       `mapstructure:"multiplier"`
	MaxDelay     time.Duration `mapstructure:"max_delay"`
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
	ConsumeRetry        ConsumeRetryConfig              `mapstructure:"consume_retry"`
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
		ConsumeRetry: ConsumeRetryConfig{
			Strategy:     consumeRetryStrategyConstant,
			InitialDelay: time.Second,
			Multiplier:   2,
			MaxDelay:     30 * time.Second,
		},
		MetricsBuckets: defaultMetricsBucketsConfig(),
	}
}

func (cfg ConsumeRetryConfig) delayForAttempt(attempt uint64) time.Duration {
	if attempt <= 1 {
		return cfg.InitialDelay
	}
	delay := float64(cfg.InitialDelay) * math.Pow(cfg.Multiplier, float64(attempt-1))
	if math.IsInf(delay, 0) || delay >= float64(cfg.MaxDelay) {
		return cfg.MaxDelay
	}
	return time.Duration(delay)
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
	switch cfg.ConsumeRetry.Strategy {
	case "":
		if cfg.ConsumeRetry.InitialDelay < 0 {
			return fmt.Errorf("consume_retry.initial_delay must be greater than or equal to zero")
		}
	case consumeRetryStrategyNone:
	case consumeRetryStrategyConstant:
		if cfg.ConsumeRetry.InitialDelay < 0 {
			return fmt.Errorf("consume_retry.initial_delay must be greater than or equal to zero")
		}
	case consumeRetryStrategyExponential:
		if cfg.ConsumeRetry.InitialDelay <= 0 {
			return fmt.Errorf("consume_retry.initial_delay must be greater than zero for strategy %q", consumeRetryStrategyExponential)
		}
		if cfg.ConsumeRetry.Multiplier < 1 {
			return fmt.Errorf("consume_retry.multiplier must be greater than or equal to one")
		}
		if math.IsNaN(cfg.ConsumeRetry.Multiplier) || math.IsInf(cfg.ConsumeRetry.Multiplier, 0) {
			return fmt.Errorf("consume_retry.multiplier must be finite")
		}
		if cfg.ConsumeRetry.MaxDelay < cfg.ConsumeRetry.InitialDelay {
			return fmt.Errorf("consume_retry.max_delay must be greater than or equal to consume_retry.initial_delay")
		}
	default:
		return fmt.Errorf("consume_retry.strategy must be one of %q, %q, or %q", consumeRetryStrategyNone, consumeRetryStrategyConstant, consumeRetryStrategyExponential)
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
