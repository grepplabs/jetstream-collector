package jetstreamexporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const testExporterName = "jetstream/metrics-input"

func TestJetstreamMetricsRecordPublishSuccess(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	set := exportertest.NewNopSettings(typ)
	set.ID = component.MustNewIDWithName(typ.String(), "metrics-input")
	set.MeterProvider = provider
	exp, err := newExporter(set, &Config{Subject: "otel.logs", ContentType: "proto"})
	require.NoError(t, err)
	exp.js = &fakePublisher{}

	require.NoError(t, exp.ConsumeLogs(context.Background(), plog.NewLogs()))

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "jetstream_exporter_publish_attempts", 1, attrExporter, testExporterName, attrSignal, signalLogs)
	assertCounterValue(t, rm, "jetstream_exporter_publish_successes", 1, attrExporter, testExporterName, attrSignal, signalLogs)
	assertAggregationRecorded(t, rm, "jetstream_exporter_publish_duration", attrExporter, testExporterName, attrSignal, signalLogs)
	assertAggregationRecorded(t, rm, "jetstream_exporter_payload_size", attrExporter, testExporterName, attrSignal, signalLogs)
	assertHistogramBounds(t, rm, "jetstream_exporter_publish_duration", []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}, attrExporter, testExporterName, attrSignal, signalLogs)
	assertHistogramBounds(t, rm, "jetstream_exporter_payload_size", []float64{128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}, attrExporter, testExporterName, attrSignal, signalLogs)
}

func TestJetstreamMetricsRecordPublishFailure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	set := exportertest.NewNopSettings(typ)
	set.ID = component.MustNewIDWithName(typ.String(), "metrics-input")
	set.MeterProvider = provider
	exp, err := newExporter(set, &Config{ContentType: "proto"})
	require.NoError(t, err)
	exp.js = &fakePublisher{}

	err = exp.ConsumeMetrics(context.Background(), pmetric.NewMetrics())
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolve subject: subject is required")
	require.True(t, consumererror.IsPermanent(err))

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "jetstream_exporter_publish_failures", 1, attrExporter, testExporterName, attrSignal, signalMetrics, attrStage, stageResolveSubject)
	assertNoMetric(t, rm, "jetstream_exporter_publish_successes")
	assertNoMetric(t, rm, "jetstream_exporter_payload_size")
}

func TestJetstreamMetricsRecordStartupCounters(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	metrics, err := newJetstreamMetrics(provider, testExporterName, false, MetricsBucketsConfig{})
	require.NoError(t, err)
	metrics.recordStartupSuccess(context.Background())
	metrics.recordStartupFailure(context.Background(), stageBootstrap)

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "jetstream_exporter_startup_successes", 1, attrExporter, testExporterName)
	assertCounterValue(t, rm, "jetstream_exporter_startup_failures", 1, attrExporter, testExporterName, attrStage, stageBootstrap)
}

func TestJetstreamMetricsIncludeSubjectWhenEnabled(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	metrics, err := newJetstreamMetrics(provider, testExporterName, true, MetricsBucketsConfig{})
	require.NoError(t, err)
	metrics.recordPublishAttempt(context.Background(), signalLogs, "otel.logs")
	metrics.recordPublishSuccess(context.Background(), signalLogs, "otel.logs")
	metrics.recordPublishFailure(context.Background(), signalLogs, stagePublish, "otel.logs")
	metrics.recordPublishDuration(context.Background(), signalLogs, "otel.logs", time.Second)
	metrics.recordPayloadSize(context.Background(), signalLogs, "otel.logs", 42)

	rm := collectMetrics(t, reader)
	assertCounterValue(t, rm, "jetstream_exporter_publish_attempts", 1, attrExporter, testExporterName, attrSignal, signalLogs, attrSubject, "otel.logs")
	assertCounterValue(t, rm, "jetstream_exporter_publish_successes", 1, attrExporter, testExporterName, attrSignal, signalLogs, attrSubject, "otel.logs")
	assertCounterValue(t, rm, "jetstream_exporter_publish_failures", 1, attrExporter, testExporterName, attrSignal, signalLogs, attrStage, stagePublish, attrSubject, "otel.logs")
	assertAggregationRecorded(t, rm, "jetstream_exporter_publish_duration", attrExporter, testExporterName, attrSignal, signalLogs, attrSubject, "otel.logs")
	assertAggregationRecorded(t, rm, "jetstream_exporter_payload_size", attrExporter, testExporterName, attrSignal, signalLogs, attrSubject, "otel.logs")
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

func assertCounterValue(t *testing.T, rm metricdata.ResourceMetrics, name string, want int64, attrs ...string) {
	t.Helper()
	metric := findMetric(t, rm, name)
	sum, ok := metric.Data.(metricdata.Sum[int64])
	require.True(t, ok, "metric %q is not a sum", name)
	require.NotEmpty(t, sum.DataPoints, "metric %q has no data points", name)
	require.Equal(t, want, sum.DataPoints[0].Value, "metric %q value", name)
	assertAttributes(t, sum.DataPoints[0].Attributes, attrs...)
}

func assertAggregationRecorded(t *testing.T, rm metricdata.ResourceMetrics, name string, attrs ...string) {
	t.Helper()
	metric := findMetric(t, rm, name)
	switch data := metric.Data.(type) {
	case metricdata.Sum[int64]:
		require.NotEmpty(t, data.DataPoints, "metric %q has no data points", name)
		assertAttributes(t, data.DataPoints[0].Attributes, attrs...)
	case metricdata.Sum[float64]:
		require.NotEmpty(t, data.DataPoints, "metric %q has no data points", name)
		assertAttributes(t, data.DataPoints[0].Attributes, attrs...)
	case metricdata.Histogram[int64]:
		require.NotEmpty(t, data.DataPoints, "metric %q has no data points", name)
		require.Greater(t, data.DataPoints[0].Count, uint64(0), "metric %q count", name)
		assertAttributes(t, data.DataPoints[0].Attributes, attrs...)
	case metricdata.Histogram[float64]:
		require.NotEmpty(t, data.DataPoints, "metric %q has no data points", name)
		require.Greater(t, data.DataPoints[0].Count, uint64(0), "metric %q count", name)
		assertAttributes(t, data.DataPoints[0].Attributes, attrs...)
	default:
		t.Fatalf("metric %q has unexpected aggregation type %T", name, metric.Data)
	}
}

func assertHistogramBounds(t *testing.T, rm metricdata.ResourceMetrics, name string, want []float64, attrs ...string) {
	t.Helper()
	metric := findMetric(t, rm, name)
	switch data := metric.Data.(type) {
	case metricdata.Histogram[int64]:
		require.NotEmpty(t, data.DataPoints, "metric %q has no data points", name)
		require.Equal(t, want, data.DataPoints[0].Bounds, "metric %q bounds", name)
		assertAttributes(t, data.DataPoints[0].Attributes, attrs...)
	case metricdata.Histogram[float64]:
		require.NotEmpty(t, data.DataPoints, "metric %q has no data points", name)
		require.Equal(t, want, data.DataPoints[0].Bounds, "metric %q bounds", name)
		assertAttributes(t, data.DataPoints[0].Attributes, attrs...)
	default:
		t.Fatalf("metric %q is not a histogram", name)
	}
}

func assertNoMetric(t *testing.T, rm metricdata.ResourceMetrics, name string) {
	t.Helper()
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				t.Fatalf("unexpected metric %q present", name)
			}
		}
	}
}

func findMetric(t *testing.T, rm metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return metric
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

func assertAttributes(t *testing.T, set attribute.Set, attrs ...string) {
	t.Helper()
	require.Equal(t, 0, len(attrs)%2)
	for i := 0; i < len(attrs); i += 2 {
		key := attrs[i]
		want := attrs[i+1]
		value, ok := set.Value(attribute.Key(key))
		require.True(t, ok, "attribute %q missing", key)
		require.Equal(t, want, value.AsString(), "attribute %q", key)
	}
}
