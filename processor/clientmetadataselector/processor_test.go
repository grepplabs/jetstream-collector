package clientmetadataselector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{name: "valid defaults", cfg: &Config{MetadataKey: "shard", Values: []string{"0"}}},
		{name: "valid random", cfg: &Config{MetadataKey: "shard", Values: []string{"0", "1"}, Strategy: strategyRandom}},
		{name: "empty metadata key", cfg: &Config{Values: []string{"0"}}, wantErr: true},
		{name: "metadata key whitespace", cfg: &Config{MetadataKey: " shard", Values: []string{"0"}}, wantErr: true},
		{name: "no values", cfg: &Config{MetadataKey: "shard"}, wantErr: true},
		{name: "empty value", cfg: &Config{MetadataKey: "shard", Values: []string{" "}}, wantErr: true},
		{name: "value whitespace", cfg: &Config{MetadataKey: "shard", Values: []string{"0 "}}, wantErr: true},
		{name: "unsupported strategy", cfg: &Config{MetadataKey: "shard", Values: []string{"0"}, Strategy: "least_loaded"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfigValidateDoesNotMutateConfig(t *testing.T) {
	cfg := &Config{MetadataKey: "shard", Values: []string{"only"}}

	require.NoError(t, cfg.Validate())
	require.Equal(t, "shard", cfg.MetadataKey)
	require.Empty(t, cfg.Strategy)
}

func TestSelectorRoundRobinAddsMetadata(t *testing.T) {
	p := newSelector(&Config{
		MetadataKey: "shard",
		Values:      []string{"0", "1", "2"},
		Strategy:    strategyRoundRobin,
	})
	ctx := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{
		"existing": {"keep"},
		"shard":    {"old"},
	})})

	for i, want := range []string{"0", "1", "2", "0"} {
		out := p.addMetadata(ctx)
		got := client.FromContext(out).Metadata
		require.Equalf(t, []string{want}, got.Get("shard"), "call %d", i)
		require.Equalf(t, []string{"keep"}, got.Get("existing"), "call %d", i)
	}
	require.Equal(t, []string{"old"}, client.FromContext(ctx).Metadata.Get("shard"))
}

func TestSelectorSingleValueSkipsStrategySelection(t *testing.T) {
	p := newSelector(&Config{
		MetadataKey: "shard",
		Values:      []string{"only"},
		Strategy:    strategyRandom,
	})

	for range 3 {
		got := client.FromContext(p.addMetadata(context.Background())).Metadata.Get("shard")
		require.Equal(t, []string{"only"}, got)
	}
	require.Zero(t, p.index.Load())
}

func TestSelectorRandomChoosesConfiguredValue(t *testing.T) {
	values := []string{"a", "b", "c"}
	p := newSelector(&Config{MetadataKey: "shard", Values: values, Strategy: strategyRandom})
	allowed := map[string]bool{"a": true, "b": true, "c": true}

	for range 100 {
		got := client.FromContext(p.addMetadata(context.Background())).Metadata.Get("shard")
		require.Len(t, got, 1)
		require.Truef(t, allowed[got[0]], "shard %q is not one of %v", got[0], values)
	}
}
