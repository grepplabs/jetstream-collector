package jetstreamreceiver

import (
	"testing"
	"time"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

func TestNewDefaultConfig(t *testing.T) {
	cfg := NewDefaultConfig()

	require.Equal(t, time.Second, cfg.ConsumeRetryDelay)
	require.Equal(t, sharedjetstream.DefaultURL, cfg.URL)
	require.Equal(t, sharedjetstream.CompressionNone, cfg.Compression)
	require.False(t, cfg.IncludeSubject)
	require.Equal(t, string(processingModeSingle), cfg.ProcessingMode)
	require.Equal(t, defaultBatchMaxMessages, cfg.BatchMaxMessages)
	require.Equal(t, defaultBatchMaxWait, cfg.BatchMaxWait)
	require.False(t, cfg.BatchGroupBySubject)
	require.Equal(t, defaultConsumeDurationBuckets, cfg.MetricsBuckets.ConsumeDuration)
	require.Equal(t, defaultPayloadSizeBuckets, cfg.MetricsBuckets.PayloadSize)
}

func TestConfigBucketsPartialUnmarshal(t *testing.T) {
	cfg := NewDefaultConfig()
	conf := confmap.NewFromStringMap(map[string]any{
		"metrics_buckets": map[string]any{
			"consume_duration": []any{0.01, 0.1, 1.0},
		},
	})

	require.NoError(t, conf.Unmarshal(cfg))

	require.Equal(t, []float64{0.01, 0.1, 1.0}, cfg.MetricsBuckets.ConsumeDuration)
	require.Equal(t, defaultPayloadSizeBuckets, cfg.MetricsBuckets.PayloadSize)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name: "valid consumer name",
			cfg: &Config{
				URL:          "nats://localhost:4222",
				Stream:       "otel",
				Subject:      "otel.logs",
				ConsumerName: "shared",
			},
		},
		{
			name: "valid worker pool",
			cfg: &Config{
				URL:          "nats://localhost:4222",
				Stream:       "otel",
				Subject:      "otel.logs",
				ConsumerName: "shared",
				Workers:      1,
			},
		},
		{
			name: "valid batch mode",
			cfg: &Config{
				URL:                 "nats://localhost:4222",
				Stream:              "otel",
				Subject:             "otel.logs",
				ConsumerName:        "shared",
				ProcessingMode:      "batch",
				Workers:             2,
				BatchMaxMessages:    16,
				BatchMaxWait:        500 * time.Millisecond,
				BatchGroupBySubject: true,
			},
		},
		{
			name: "custom metrics buckets",
			cfg: &Config{
				URL:          "nats://localhost:4222",
				Stream:       "otel",
				Subject:      "otel.logs",
				ConsumerName: "shared",
				MetricsBuckets: MetricsBucketsConfig{
					ConsumeDuration: []float64{0.01, 0.1, 1},
					PayloadSize:     []float64{256, 1024, 4096},
				},
			},
		},
		{
			name: "zero consume retry delay",
			cfg: &Config{
				URL:               "nats://localhost:4222",
				Stream:            "otel",
				Subject:           "otel.logs",
				ConsumerName:      "shared",
				ConsumeRetryDelay: 0,
			},
		},
		{
			name: "missing consumer name",
			cfg: &Config{
				URL:     "nats://localhost:4222",
				Stream:  "otel",
				Subject: "otel.logs",
			},
			wantErr: true,
		},
		{
			name: "invalid compression",
			cfg: &Config{
				URL:         "nats://localhost:4222",
				Stream:      "otel",
				Subject:     "otel.logs",
				Compression: "brotli",
			},
			wantErr: true,
		},
		{
			name: "negative workers",
			cfg: &Config{
				URL:          "nats://localhost:4222",
				Stream:       "otel",
				Subject:      "otel.logs",
				ConsumerName: "shared",
				Workers:      -1,
			},
			wantErr: true,
		},
		{
			name: "invalid metrics buckets",
			cfg: &Config{
				URL:          "nats://localhost:4222",
				Stream:       "otel",
				Subject:      "otel.logs",
				ConsumerName: "shared",
				MetricsBuckets: MetricsBucketsConfig{
					ConsumeDuration: []float64{0.1, 0.05},
				},
			},
			wantErr: true,
		},
		{
			name: "negative consume retry delay",
			cfg: &Config{
				URL:               "nats://localhost:4222",
				Stream:            "otel",
				Subject:           "otel.logs",
				ConsumerName:      "shared",
				ConsumeRetryDelay: -1 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "invalid processing mode",
			cfg: &Config{
				URL:            "nats://localhost:4222",
				Stream:         "otel",
				Subject:        "otel.logs",
				ConsumerName:   "shared",
				ProcessingMode: "merge",
			},
			wantErr: true,
		},
		{
			name: "invalid batch settings in batch mode",
			cfg: &Config{
				URL:              "nats://localhost:4222",
				Stream:           "otel",
				Subject:          "otel.logs",
				ConsumerName:     "shared",
				ProcessingMode:   "batch",
				BatchMaxMessages: 0,
				BatchMaxWait:     0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
