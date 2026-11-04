package celpartitionprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type captureMetricsConsumer struct{ batches []pmetric.Metrics }

func (c *captureMetricsConsumer) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (c *captureMetricsConsumer) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	c.batches = append(c.batches, md)
	return nil
}

func TestMetricsPartitionDataPoints(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.PartitionBy.Metrics = []string{"datapoint.attributes['tenant']", "datapoint.resource.attributes['service.name']"}
	compiled, err := compileConfig(cfg)
	require.NoError(t, err)
	next := &captureMetricsConsumer{}
	proc := &metricsProcessor{compiled: compiled, next: next}
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "api")
	metric := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("requests")
	gauge := metric.SetEmptyGauge()
	for _, tenant := range []string{"a", "b", "a"} {
		point := gauge.DataPoints().AppendEmpty()
		point.Attributes().PutStr("tenant", tenant)
	}
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))
	require.Len(t, next.batches, 2)
	require.ElementsMatch(t, []int{2, 1}, []int{countDataPoints(next.batches[0]), countDataPoints(next.batches[1])})
	for _, batch := range next.batches {
		require.Equal(t, "requests", batch.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name())
	}
}

func TestMetricsMissingAttributeCanDropDataPoint(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.PartitionBy.Metrics = []string{"datapoint.attributes['tenant']"}
	cfg.OnError = OnErrorDrop
	compiled, err := compileConfig(cfg)
	require.NoError(t, err)
	next := &captureMetricsConsumer{}
	proc := &metricsProcessor{compiled: compiled, next: next}
	md := pmetric.NewMetrics()
	metric := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	gauge := metric.SetEmptyGauge()
	good := gauge.DataPoints().AppendEmpty()
	good.Attributes().PutStr("tenant", "a")
	gauge.DataPoints().AppendEmpty()
	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))
	require.Len(t, next.batches, 1)
	require.Equal(t, 1, countDataPoints(next.batches[0]))
}

func countDataPoints(md pmetric.Metrics) int {
	total := 0
	for ri := 0; ri < md.ResourceMetrics().Len(); ri++ {
		rm := md.ResourceMetrics().At(ri)
		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)
			for mi := 0; mi < sm.Metrics().Len(); mi++ {
				metric := sm.Metrics().At(mi)
				switch metric.Type() {
				case pmetric.MetricTypeEmpty:
					// Empty metrics have no data points.
				case pmetric.MetricTypeGauge:
					total += metric.Gauge().DataPoints().Len()
				case pmetric.MetricTypeSum:
					total += metric.Sum().DataPoints().Len()
				case pmetric.MetricTypeHistogram:
					total += metric.Histogram().DataPoints().Len()
				case pmetric.MetricTypeExponentialHistogram:
					total += metric.ExponentialHistogram().DataPoints().Len()
				case pmetric.MetricTypeSummary:
					total += metric.Summary().DataPoints().Len()
				}
			}
		}
	}
	return total
}
