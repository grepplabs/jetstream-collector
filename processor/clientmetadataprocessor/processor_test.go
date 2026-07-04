package clientmetadataprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	client "go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
)

type captureLogsConsumer struct {
	ctx context.Context
	ld  plog.Logs
}

func (c *captureLogsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

func (c *captureLogsConsumer) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	c.ctx = ctx
	c.ld = ld
	return nil
}

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

func TestConsumeLogsAddsClientMetadataToContext(t *testing.T) {
	next := &captureLogsConsumer{}
	proc, err := newClientMetadataProcessor(&Config{
		ClientMetadata: []string{"resource:service.name", "telemetry:acme.resource.id"},
	}, next)
	require.NoError(t, err)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc-a")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Attributes().PutStr("acme.resource.id", "resource-1")

	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"existing": []string{"keep"},
		}),
	})

	require.NoError(t, proc.ConsumeLogs(ctx, ld))
	require.NotNil(t, next.ctx)

	info := client.FromContext(next.ctx)
	require.Equal(t, []string{"keep"}, info.Metadata.Get("existing"))
	require.Equal(t, []string{"svc-a"}, info.Metadata.Get("service.name"))
	require.Equal(t, []string{"resource-1"}, info.Metadata.Get("acme.resource.id"))
}

func TestConsumeLogsFailsOnConflictingBatchValues(t *testing.T) {
	next := &captureLogsConsumer{}
	proc, err := newClientMetadataProcessor(&Config{
		ClientMetadata: []string{"resource:service.name"},
	}, next)
	require.NoError(t, err)

	ld := plog.NewLogs()
	rl1 := ld.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("service.name", "svc-a")
	rl1.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	rl2 := ld.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("service.name", "svc-b")
	rl2.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	err = proc.ConsumeLogs(context.Background(), ld)
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflicting client_metadata values")
}

func TestAddClientMetadataOverwritesConfiguredKeys(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"service.name": []string{"old"},
			"keep":         []string{"value"},
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
