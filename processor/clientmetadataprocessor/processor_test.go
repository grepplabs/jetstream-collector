package clientmetadataprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	client "go.opentelemetry.io/collector/client"
)

func TestConfigValidate(t *testing.T) {
	t.Run("normalizes and validates entries", func(t *testing.T) {
		cfg := &Config{
			ClientMetadata: []string{" resource:service.name ", "telemetry:acme.resource.id"},
		}

		require.NoError(t, cfg.Validate())
		require.Equal(t, []string{" resource:service.name ", "telemetry:acme.resource.id"}, cfg.ClientMetadata)
	})

	t.Run("rejects invalid entries and duplicate keys", func(t *testing.T) {
		cfg := &Config{
			ClientMetadata: []string{"", "header:X-Tenant", "resource:service.name", "telemetry:service.name"},
		}

		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot be empty")
		require.Contains(t, err.Error(), "unsupported client_metadata source")
		require.Contains(t, err.Error(), "duplicate key")
	})
}

func TestAddClientMetadataOverwritesConfiguredKeys(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"service.name": {"old"},
			"keep":         {"value"},
		}),
	})

	out := addClientMetadata(ctx, map[string]string{
		"service.name":     "new",
		"acme.resource.id": "resource-1",
	})

	info := client.FromContext(out)
	require.Equal(t, []string{"new"}, info.Metadata.Get("service.name"))
	require.Equal(t, []string{"value"}, info.Metadata.Get("keep"))
	require.Equal(t, []string{"resource-1"}, info.Metadata.Get("acme.resource.id"))
}
