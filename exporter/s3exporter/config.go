package s3exporter

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	defaultRegion            = "eu-central-1"
	defaultFilenameExtension = "log"
	compressionNone          = "none"
	compressionGzip          = "gzip"
	marshalerTypeProto       = "proto"
	marshalerTypeJSON        = "json"
	mimeContentTypeProto     = "application/x-protobuf"
	mimeContentTypeJSON      = "application/json"
	contentEncodingGzip      = "gzip"
)

type CredentialsConfig struct {
	AccessKeyID     string       `mapstructure:"access_key_id"`
	SecretAccessKey string       `mapstructure:"secret_access_key"`
	SessionToken    string       `mapstructure:"session_token"`
	Provider        component.ID `mapstructure:"provider"`
}

type MetricsBucketsConfig struct {
	UploadDuration []float64 `mapstructure:"upload_duration"`
	PayloadSize    []float64 `mapstructure:"payload_size"`
}

type Config struct {
	Endpoint           string                                                   `mapstructure:"endpoint"`
	Region             string                                                   `mapstructure:"region"`
	Bucket             string                                                   `mapstructure:"bucket"`
	Secure             bool                                                     `mapstructure:"secure"`
	ForcePathStyle     bool                                                     `mapstructure:"force_path_style"`
	Credentials        CredentialsConfig                                        `mapstructure:"credentials"`
	FilenameTemplate   string                                                   `mapstructure:"filename_template"`
	FilenameAppendUUID bool                                                     `mapstructure:"filename_append_uuid"`
	FilenameExtension  string                                                   `mapstructure:"filename_extension"`
	MarshalerType      string                                                   `mapstructure:"marshaler"`
	Compression        string                                                   `mapstructure:"compression"`
	MetricsBuckets     MetricsBucketsConfig                                     `mapstructure:"metrics_buckets"`
	TimeoutSettings    exporterhelper.TimeoutConfig                             `mapstructure:",squash"`
	RetryOnFailure     configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
	SendingQueue       configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
}

var _ component.Config = (*Config)(nil)

func defaultMetricsBucketsConfig() MetricsBucketsConfig {
	return MetricsBucketsConfig{
		UploadDuration: append([]float64(nil), defaultUploadDurationBuckets...),
		PayloadSize:    append([]float64(nil), defaultPayloadSizeBuckets...),
	}
}

func (cfg MetricsBucketsConfig) withDefaults() MetricsBucketsConfig {
	defaults := defaultMetricsBucketsConfig()
	if len(cfg.UploadDuration) == 0 {
		cfg.UploadDuration = defaults.UploadDuration
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
		Region:             defaultRegion,
		Secure:             true,
		FilenameAppendUUID: true,
		FilenameExtension:  defaultFilenameExtension,
		MarshalerType:      marshalerTypeProto,
		Compression:        compressionNone,
		MetricsBuckets:     defaultMetricsBucketsConfig(),
		TimeoutSettings:    exporterhelper.NewDefaultTimeoutConfig(),
		RetryOnFailure: configretry.BackOffConfig{
			Enabled:             false,
			InitialInterval:     1 * time.Second,
			RandomizationFactor: 0.2,
			Multiplier:          1.5,
			MaxInterval:         5 * time.Second,
			MaxElapsedTime:      10 * time.Second,
		},
		SendingQueue: configoptional.None[exporterhelper.QueueBatchConfig](),
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

func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}

	var errs error
	if c.Bucket == "" {
		errs = errors.Join(errs, errors.New("bucket is required"))
	}

	if c.Credentials.AccessKeyID == "" && c.Credentials.SecretAccessKey == "" && c.Credentials.Provider == (component.ID{}) {
		errs = errors.Join(errs, errors.New("credentials or credentials.provider are required"))
	}

	if (c.Credentials.AccessKeyID == "") != (c.Credentials.SecretAccessKey == "") {
		errs = errors.Join(errs, errors.New("credentials.access_key_id and credentials.secret_access_key must be set together"))
	}
	if c.Compression != compressionNone && c.Compression != compressionGzip {
		errs = errors.Join(errs, fmt.Errorf("invalid compression %q", c.Compression))
	}

	if err := c.RetryOnFailure.Validate(); err != nil {
		errs = errors.Join(errs, fmt.Errorf("retry_on_failure: %w", err))
	}

	if err := c.SendingQueue.Validate(); err != nil {
		errs = errors.Join(errs, fmt.Errorf("sending_queue: %w", err))
	}

	if c.FilenameTemplate != "" {
		if _, err := newKeyPrefixTemplate(c.FilenameTemplate); err != nil {
			errs = errors.Join(errs, fmt.Errorf("filename_template: %w", err))
		}
	}

	if c.MarshalerType != marshalerTypeProto && c.MarshalerType != marshalerTypeJSON {
		errs = errors.Join(errs, fmt.Errorf("unsupported marshaler type %q", c.MarshalerType))
	}

	buckets := c.metricsBuckets()
	if err := validateHistogramBuckets(buckets.UploadDuration, "metrics_buckets.upload_duration"); err != nil {
		errs = errors.Join(errs, err)
	}
	if err := validateHistogramBuckets(buckets.PayloadSize, "metrics_buckets.payload_size"); err != nil {
		errs = errors.Join(errs, err)
	}

	return errs
}
