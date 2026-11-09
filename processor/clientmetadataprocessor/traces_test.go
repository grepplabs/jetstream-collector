package clientmetadataprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	client "go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type captureTracesConsumer struct {
	ctx context.Context
	td  ptrace.Traces
}

func (c *captureTracesConsumer) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (c *captureTracesConsumer) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	c.ctx, c.td = ctx, td
	return nil
}

func TestConsumeTracesAddsClientMetadataToContext(t *testing.T) {
	next := &captureTracesConsumer{}
	proc, err := newTracesClientMetadataProcessor(&Config{
		ClientMetadata: []string{"resource:service.name", "telemetry:tenant.id"},
	}, next)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc-traces")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.Attributes().PutStr("tenant.id", "tenant-a")
	ctx := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"existing": {"keep"}})})

	require.NoError(t, proc.ConsumeTraces(ctx, td))
	info := client.FromContext(next.ctx)
	require.Equal(t, []string{"keep"}, info.Metadata.Get("existing"))
	require.Equal(t, []string{"svc-traces"}, info.Metadata.Get("service.name"))
	require.Equal(t, []string{"tenant-a"}, info.Metadata.Get("tenant.id"))
	require.Equal(t, td, next.td)
}

func TestConsumeTracesFailsOnConflictingBatchValues(t *testing.T) {
	next := &captureTracesConsumer{}
	proc, err := newTracesClientMetadataProcessor(&Config{ClientMetadata: []string{"resource:service.name"}}, next)
	require.NoError(t, err)
	td := ptrace.NewTraces()
	for _, name := range []string{"svc-a", "svc-b"} {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", name)
		rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	}

	err = proc.ConsumeTraces(context.Background(), td)
	require.ErrorContains(t, err, "conflicting client_metadata values")
	require.Nil(t, next.ctx)
}
