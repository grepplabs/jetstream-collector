package jetstreamreceiver

import (
	"context"
	"testing"
	"time"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const testReceiverName = "jetstream/metrics-input"
const testConsumerName = "shared"

func TestJetstreamReceiverMetricsRecordConsumeSuccess(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	set := receivertest.NewNopSettings(typeStr)
	set.ID = component.MustNewIDWithName(typeStr.String(), "metrics-input")
	set.MeterProvider = provider

	called := false
	r, err := newReceiver(set, &Config{Compression: sharedjetstream.CompressionNone, ConsumerName: "shared"}, payloadMetrics, nil, mustMetricsConsumer(t, &called), nil, nil)
	require.NoError(t, err)

	msg := encodedOTLPMessage(t, mustMarshalProto(t, pmetricotlp.NewExportRequest()), "application/x-protobuf", "", false)
	require.NoError(t, r.handleMessage(context.Background(), msg))
	require.True(t, called)
	require.True(t, msg.acked)

	rm := collectReceiverMetrics(t, reader)
	assertCounterValue(t, rm, "jetstream_receiver_consume_attempts", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics)
	assertCounterValue(t, rm, "jetstream_receiver_consume_successes", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics)
	assertAggregationRecorded(t, rm, "jetstream_receiver_consume_duration", attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics)
	assertAggregationRecorded(t, rm, "jetstream_receiver_payload_size", attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics)
	assertHistogramBounds(t, rm, "jetstream_receiver_consume_duration", []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics)
	assertHistogramBounds(t, rm, "jetstream_receiver_payload_size", []float64{128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics)
}

func TestJetstreamReceiverMetricsRecordConsumeFailure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	set := receivertest.NewNopSettings(typeStr)
	set.ID = component.MustNewIDWithName(typeStr.String(), "metrics-input")
	set.MeterProvider = provider

	r, err := newReceiver(set, &Config{Compression: sharedjetstream.CompressionNone, ConsumerName: "shared"}, payloadMetrics, nil, mustMetricsConsumer(t, new(bool)), nil, nil)
	require.NoError(t, err)

	msg := encodedOTLPMessage(t, mustMarshalProto(t, pmetricotlp.NewExportRequest()), "application/yaml", "", false)

	err = r.handleMessage(context.Background(), msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported content-type")
	require.False(t, msg.acked)
	require.Equal(t, reasonUnsupportedContentType, msg.termReason)

	rm := collectReceiverMetrics(t, reader)
	assertCounterValue(t, rm, "jetstream_receiver_consume_attempts", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics)
	assertCounterValue(t, rm, "jetstream_receiver_consume_failures", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics, attrStage, failureStageParseContentType)
	assertNoMetric(t, rm, "jetstream_receiver_consume_successes")
	assertNoMetric(t, rm, "jetstream_receiver_payload_size")
}

func TestJetstreamReceiverMetricsRecordRetryableConsumeFailure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	set := receivertest.NewNopSettings(typeStr)
	set.ID = component.MustNewIDWithName(typeStr.String(), "metrics-input")
	set.MeterProvider = provider

	r, err := newReceiver(set, &Config{Compression: sharedjetstream.CompressionNone, ConsumerName: "shared"}, payloadMetrics, nil, mustFailingMetricsConsumer(t), nil, nil)
	require.NoError(t, err)

	msg := encodedOTLPMessage(t, mustMarshalProto(t, pmetricotlp.NewExportRequest()), "application/x-protobuf", "", false)

	err = r.handleMessage(context.Background(), msg)
	require.NoError(t, err)
	require.False(t, msg.acked)
	require.True(t, msg.nacked)
	require.Equal(t, r.cfg.ConsumeRetryDelay, msg.nakDelay)
	require.Empty(t, msg.termReason)

	rm := collectReceiverMetrics(t, reader)
	assertCounterValue(t, rm, "jetstream_receiver_consume_attempts", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics)
	assertCounterValue(t, rm, "jetstream_receiver_consume_failures", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics, attrStage, failureStageConsumeRetryable)
	assertNoMetric(t, rm, "jetstream_receiver_consume_successes")
	assertNoMetric(t, rm, "jetstream_receiver_payload_size")
}

func TestJetstreamReceiverMetricsRecordPermanentConsumeFailure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	set := receivertest.NewNopSettings(typeStr)
	set.ID = component.MustNewIDWithName(typeStr.String(), "metrics-input")
	set.MeterProvider = provider

	r, err := newReceiver(set, &Config{Compression: sharedjetstream.CompressionNone, ConsumerName: "shared"}, payloadMetrics, nil, mustPermanentFailingMetricsConsumer(t), nil, nil)
	require.NoError(t, err)

	msg := encodedOTLPMessage(t, mustMarshalProto(t, pmetricotlp.NewExportRequest()), "application/x-protobuf", "", false)

	err = r.handleMessage(context.Background(), msg)
	require.Error(t, err)
	require.False(t, msg.acked)
	require.False(t, msg.nacked)
	require.Equal(t, reasonPermanentConsumerError, msg.termReason)

	rm := collectReceiverMetrics(t, reader)
	assertCounterValue(t, rm, "jetstream_receiver_consume_attempts", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics)
	assertCounterValue(t, rm, "jetstream_receiver_consume_failures", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics, attrStage, failureStageConsumePermanent)
	assertNoMetric(t, rm, "jetstream_receiver_consume_successes")
	assertNoMetric(t, rm, "jetstream_receiver_payload_size")
}

func TestJetstreamReceiverMetricsRecordStartupCounters(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	metrics, err := newJetstreamMetrics(provider, testReceiverName, testConsumerName, false, MetricsBucketsConfig{})
	require.NoError(t, err)
	metrics.recordStartupSuccess(context.Background())
	metrics.recordStartupFailure(context.Background(), stageBootstrap)

	rm := collectReceiverMetrics(t, reader)
	assertCounterValue(t, rm, "jetstream_receiver_startup_successes", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName)
	assertCounterValue(t, rm, "jetstream_receiver_startup_failures", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrStage, stageBootstrap)
}

func TestJetstreamReceiverMetricsIncludeSubjectWhenEnabled(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	metrics, err := newJetstreamMetrics(provider, testReceiverName, testConsumerName, true, MetricsBucketsConfig{})
	require.NoError(t, err)
	metrics.recordConsumeAttempt(context.Background(), signalMetrics, "otel.logs")
	metrics.recordConsumeSuccess(context.Background(), signalMetrics, "otel.logs")
	metrics.recordConsumeFailure(context.Background(), signalMetrics, failureStageAck, "otel.logs")
	metrics.recordConsumeDuration(context.Background(), signalMetrics, "otel.logs", time.Second)
	metrics.recordPayloadSize(context.Background(), signalMetrics, "otel.logs", 42)

	rm := collectReceiverMetrics(t, reader)
	assertCounterValue(t, rm, "jetstream_receiver_consume_attempts", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics, attrSubject, "otel.logs")
	assertCounterValue(t, rm, "jetstream_receiver_consume_successes", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics, attrSubject, "otel.logs")
	assertCounterValue(t, rm, "jetstream_receiver_consume_failures", 1, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics, attrStage, failureStageAck, attrSubject, "otel.logs")
	assertAggregationRecorded(t, rm, "jetstream_receiver_consume_duration", attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics, attrSubject, "otel.logs")
	assertAggregationRecorded(t, rm, "jetstream_receiver_payload_size", attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics, attrSubject, "otel.logs")
}

func TestJetstreamReceiverMetricsUsesConfiguredBuckets(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	metrics, err := newJetstreamMetrics(provider, testReceiverName, testConsumerName, false, MetricsBucketsConfig{ConsumeDuration: []float64{0.01, 0.1, 1}, PayloadSize: []float64{256, 1024, 4096}})
	require.NoError(t, err)
	metrics.recordConsumeDuration(context.Background(), signalMetrics, "otel.logs", 250*time.Millisecond)
	metrics.recordPayloadSize(context.Background(), signalMetrics, "otel.logs", 512)

	rm := collectReceiverMetrics(t, reader)
	assertHistogramBounds(t, rm, "jetstream_receiver_consume_duration", []float64{0.01, 0.1, 1}, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics)
	assertHistogramBounds(t, rm, "jetstream_receiver_payload_size", []float64{256, 1024, 4096}, attrReceiver, testReceiverName, attrConsumerName, testConsumerName, attrSignal, signalMetrics)
}

func collectReceiverMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
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
