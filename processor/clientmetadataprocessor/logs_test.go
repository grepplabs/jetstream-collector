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

func (c *captureLogsConsumer) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (c *captureLogsConsumer) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	c.ctx, c.ld = ctx, ld
	return nil
}

func TestConsumeLogsAddsClientMetadataToContext(t *testing.T) {
	next := &captureLogsConsumer{}
	proc, err := newLogsClientMetadataProcessor(&Config{
		ClientMetadata: []string{"resource:service.name", "telemetry:acme.resource.id"},
	}, next)
	require.NoError(t, err)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc-a")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Attributes().PutStr("acme.resource.id", "resource-1")
	ctx := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"existing": {"keep"}})})

	require.NoError(t, proc.ConsumeLogs(ctx, ld))
	info := client.FromContext(next.ctx)
	require.Equal(t, []string{"keep"}, info.Metadata.Get("existing"))
	require.Equal(t, []string{"svc-a"}, info.Metadata.Get("service.name"))
	require.Equal(t, []string{"resource-1"}, info.Metadata.Get("acme.resource.id"))
	require.Equal(t, ld, next.ld)
}

func TestConsumeLogsFailsOnConflictingBatchValues(t *testing.T) {
	next := &captureLogsConsumer{}
	proc, err := newLogsClientMetadataProcessor(&Config{ClientMetadata: []string{"resource:service.name"}}, next)
	require.NoError(t, err)
	ld := plog.NewLogs()
	for _, name := range []string{"svc-a", "svc-b"} {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", name)
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	}

	err = proc.ConsumeLogs(context.Background(), ld)
	require.ErrorContains(t, err, "conflicting client_metadata values")
	require.Nil(t, next.ctx)
}
