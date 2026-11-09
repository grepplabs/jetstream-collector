package clientmetadataprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	client "go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type captureMetricsConsumer struct {
	ctx context.Context
	md  pmetric.Metrics
}

func (c *captureMetricsConsumer) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (c *captureMetricsConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	c.ctx, c.md = ctx, md
	return nil
}

func TestConsumeMetricsAddsClientMetadataToContext(t *testing.T) {
	next := &captureMetricsConsumer{}
	proc, err := newMetricsClientMetadataProcessor(&Config{
		ClientMetadata: []string{"resource:service.name", "telemetry:tenant.id"},
	}, next)
	require.NoError(t, err)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc-metrics")
	dp := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("tenant.id", "tenant-a")
	ctx := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"existing": {"keep"}})})

	require.NoError(t, proc.ConsumeMetrics(ctx, md))
	info := client.FromContext(next.ctx)
	require.Equal(t, []string{"keep"}, info.Metadata.Get("existing"))
	require.Equal(t, []string{"svc-metrics"}, info.Metadata.Get("service.name"))
	require.Equal(t, []string{"tenant-a"}, info.Metadata.Get("tenant.id"))
	require.Equal(t, md, next.md)
}

func TestConsumeMetricsFailsOnConflictingBatchValues(t *testing.T) {
	next := &captureMetricsConsumer{}
	proc, err := newMetricsClientMetadataProcessor(&Config{ClientMetadata: []string{"resource:service.name"}}, next)
	require.NoError(t, err)
	md := pmetric.NewMetrics()
	for _, name := range []string{"svc-a", "svc-b"} {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", name)
		rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()
	}

	err = proc.ConsumeMetrics(context.Background(), md)
	require.ErrorContains(t, err, "conflicting client_metadata values")
	require.Nil(t, next.ctx)
}
