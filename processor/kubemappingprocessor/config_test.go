package kubemappingprocessor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

func validConfig() *Config {
	return &Config{Mappings: []MappingConfig{{Resource: ResourceConfig{Version: "v1", Kind: "Router"}, Selector: SelectorConfig{Labels: []string{"token=${header:Token}"}}, Value: ValueConfig{Field: "spec.routerId"}, Target: "X-Tenant-ID"}}}
}

func TestConfigUnmarshalKubeConfig(t *testing.T) {
	cfg := NewDefaultConfig()
	conf := confmap.NewFromStringMap(map[string]any{
		"kubeconfig": "/tmp/kubeconfig",
		"context":    "production",
	})

	require.NoError(t, conf.Unmarshal(cfg))
	require.Equal(t, "/tmp/kubeconfig", cfg.Kubeconfig)
	require.Equal(t, "production", cfg.Context)
}

func TestConfigUnmarshalLogger(t *testing.T) {
	cfg := NewDefaultConfig()
	conf := confmap.NewFromStringMap(map[string]any{
		"logger": map[string]any{
			"development":      true,
			"encoder":          "console",
			"level":            "debug",
			"stacktrace_level": "warn",
		},
	})

	require.NoError(t, conf.Unmarshal(cfg))
	require.True(t, cfg.Logger.Development)
	require.Equal(t, "console", cfg.Logger.Encoder)
	require.Equal(t, "debug", cfg.Logger.Level)
	require.Equal(t, "warn", cfg.Logger.StacktraceLevel)
}

func TestLoggerConfigValidate(t *testing.T) {
	require.NoError(t, (LoggerConfig{}).Validate())
	require.NoError(t, (LoggerConfig{Encoder: "json", Level: "info", StacktraceLevel: "error"}).Validate())
	require.ErrorContains(t, (LoggerConfig{Encoder: "text"}).Validate(), "logger.encoder")
	require.ErrorContains(t, (LoggerConfig{Level: "trace"}).Validate(), "logger.level")
	require.ErrorContains(t, (LoggerConfig{StacktraceLevel: "panic"}).Validate(), "logger.stacktrace_level")
}

func TestConfigCache(t *testing.T) {
	cfg := NewDefaultConfig()
	require.Equal(t, ErrorModePropagate, cfg.ErrorMode)
	require.Equal(t, time.Minute, cfg.Cache.TTL)
	require.Equal(t, uint64(10_000), cfg.Cache.Capacity)
	require.True(t, cfg.Cache.CacheMisses)

	conf := confmap.NewFromStringMap(map[string]any{"cache": map[string]any{"ttl": "2m", "capacity": 42, "cache_misses": false}})
	require.NoError(t, conf.Unmarshal(cfg))
	require.Equal(t, 2*time.Minute, cfg.Cache.TTL)
	require.Equal(t, uint64(42), cfg.Cache.Capacity)
	require.False(t, cfg.Cache.CacheMisses)
}

func TestConfigValidateErrorMode(t *testing.T) {
	for _, mode := range []ErrorMode{"", ErrorModePropagate, ErrorModeIgnore} {
		cfg := validConfig()
		cfg.ErrorMode = mode
		require.NoError(t, cfg.Validate())
	}
	cfg := validConfig()
	cfg.ErrorMode = "invalid"
	require.ErrorContains(t, cfg.Validate(), "error_mode")
}

func TestConfigValidateCache(t *testing.T) {
	cfg := validConfig()
	cfg.Cache.TTL = -time.Second
	require.ErrorContains(t, cfg.Validate(), "cache.ttl")

	cfg.Cache.TTL = time.Minute
	require.ErrorContains(t, cfg.Validate(), "cache.capacity")

	cfg.Cache.TTL = 0
	require.NoError(t, cfg.Validate())
}

func TestConfigValidate(t *testing.T) {
	require.NoError(t, validConfig().Validate())
	tests := []struct {
		name    string
		mutate  func(*MappingConfig)
		message string
	}{
		{"version", func(m *MappingConfig) { m.Resource.Version = "" }, "resource.version"},
		{"kind", func(m *MappingConfig) { m.Resource.Kind = "" }, "resource.kind"},
		{"selector", func(m *MappingConfig) { m.Selector = SelectorConfig{} }, "at least one selector"},
		{"both values", func(m *MappingConfig) { m.Value.Label = "tenant" }, "exactly one"},
		{"no value", func(m *MappingConfig) { m.Value = ValueConfig{} }, "exactly one"},
		{"target", func(m *MappingConfig) { m.Target = "" }, "target is required"},
		{"template", func(m *MappingConfig) { m.Selector.Labels = []string{"x=${metadata.X}"} }, "invalid placeholder"},
		{"field path", func(m *MappingConfig) { m.Value.Field = "spec..id" }, "invalid field path"},
		{"field selector operator", func(m *MappingConfig) { m.Selector.Fields = []string{"metadata.name!=router"} }, "must use equality"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg.Mappings[0])
			require.ErrorContains(t, cfg.Validate(), tt.message)
		})
	}
}
