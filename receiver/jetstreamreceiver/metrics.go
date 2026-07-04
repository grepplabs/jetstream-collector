package jetstreamreceiver

import (
	"context"
	"time"

	"github.com/grepplabs/jetstream-collector/receiver/jetstreamreceiver/internal/metadata"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

const (
	signalLogs     = "logs"
	signalMetrics  = "metrics"
	signalTraces   = "traces"
	signalProfiles = "profiles"

	// Metrics semantic conventions (https://opentelemetry.io/docs/specs/semconv/general/metrics/)
	// Prometheus and OpenMetrics compatibility (https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/)
	// Units should follow the Unified Code for Units of Measure (https://ucum.org/ucum)
	unitSeconds = "s"
	unitBytes   = "By"

	attrSignal       = "signal"
	attrStage        = "stage"
	attrReceiver     = "receiver"
	attrConsumerName = "consumer_name"
	attrSubject      = "subject"

	failureStageConsumeRetryable = "consume_retryable"
	failureStageConsumePermanent = "consume_permanent"
	failureStageUnmarshal        = "unmarshal"
	failureStageParseContentType = "parse_content_type"
	failureStageUnsupportedKind  = "unsupported_kind"
	failureStageAck              = "ack"

	stageConnect            = "connect"
	stageBootstrap          = "bootstrap"
	stageLookupStream       = "lookup_stream"
	stageBootstrapConsumers = "bootstrap_consumers"
	stageConsumerName       = "consumer_name"
	stageLookupConsumer     = "lookup_consumer"
	stageStartConsumer      = "start_consumer"

	batchStageConsume = "consume"
)

var (
	// Receiver processing latency is usually sub-second.
	defaultConsumeDurationBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	// Incoming OTLP requests are usually from sub-kilobyte to few kilobytes.
	defaultPayloadSizeBuckets = []float64{128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}
)

type jetstreamMetrics struct {
	receiverName    string
	consumerName    string
	includeSubject  bool
	consumeAttempts metric.Int64Counter
	consumeSuccess  metric.Int64Counter
	consumeFailures metric.Int64Counter
	consumeDuration metric.Float64Histogram
	payloadSize     metric.Int64Histogram
	startupSuccess  metric.Int64Counter
	startupFailures metric.Int64Counter
	batchAttempts   metric.Int64Counter
	batchSuccess    metric.Int64Counter
	batchFailures   metric.Int64Counter
	batchDuration   metric.Float64Histogram
	batchSize       metric.Int64Histogram
	batchDropped    metric.Int64Counter
}

func newJetstreamMetrics(mp metric.MeterProvider, receiverName, consumerName string, includeSubject bool, buckets MetricsBucketsConfig) (*jetstreamMetrics, error) {
	if mp == nil {
		mp = metricnoop.NewMeterProvider()
	}

	meter := mp.Meter(metadata.ScopeName)
	jm := &jetstreamMetrics{receiverName: receiverName, consumerName: consumerName, includeSubject: includeSubject}
	var err error

	if jm.consumeAttempts, err = meter.Int64Counter(
		"jetstream_receiver_consume_attempts",
		metric.WithDescription("Number of JetStream receive attempts."),
	); err != nil {
		return nil, err
	}
	if jm.consumeSuccess, err = meter.Int64Counter(
		"jetstream_receiver_consume_successes",
		metric.WithDescription("Number of JetStream receive attempts that were processed successfully."),
	); err != nil {
		return nil, err
	}
	if jm.consumeFailures, err = meter.Int64Counter(
		"jetstream_receiver_consume_failures",
		metric.WithDescription("Number of JetStream receive attempts that failed before acknowledgment."),
	); err != nil {
		return nil, err
	}
	if jm.consumeDuration, err = meter.Float64Histogram(
		"jetstream_receiver_consume_duration",
		metric.WithDescription("Time spent handling a JetStream receive attempt."),
		metric.WithUnit(unitSeconds),
		metric.WithExplicitBucketBoundaries(effectiveHistogramBuckets(buckets.ConsumeDuration, defaultConsumeDurationBuckets)...),
	); err != nil {
		return nil, err
	}
	if jm.payloadSize, err = meter.Int64Histogram(
		"jetstream_receiver_payload_size",
		metric.WithDescription("Size of the JetStream payload actually received."),
		metric.WithUnit(unitBytes),
		metric.WithExplicitBucketBoundaries(effectiveHistogramBuckets(buckets.PayloadSize, defaultPayloadSizeBuckets)...),
	); err != nil {
		return nil, err
	}
	if jm.startupSuccess, err = meter.Int64Counter(
		"jetstream_receiver_startup_successes",
		metric.WithDescription("Number of JetStream receiver startups that completed successfully."),
	); err != nil {
		return nil, err
	}
	if jm.startupFailures, err = meter.Int64Counter(
		"jetstream_receiver_startup_failures",
		metric.WithDescription("Number of JetStream receiver startup failures."),
	); err != nil {
		return nil, err
	}
	//batch metrics
	if jm.batchAttempts, err = meter.Int64Counter(
		"jetstream_receiver_batch_attempts",
		metric.WithDescription("Number of JetStream batch receive attempts."),
	); err != nil {
		return nil, err
	}
	if jm.batchSuccess, err = meter.Int64Counter(
		"jetstream_receiver_batch_successes",
		metric.WithDescription("Number of JetStream batches processed successfully."),
	); err != nil {
		return nil, err
	}
	if jm.batchFailures, err = meter.Int64Counter(
		"jetstream_receiver_batch_failures",
		metric.WithDescription("Number of JetStream batches that failed before acknowledgment."),
	); err != nil {
		return nil, err
	}
	if jm.batchDuration, err = meter.Float64Histogram(
		"jetstream_receiver_batch_duration",
		metric.WithDescription("Time spent handling a JetStream batch receive attempt."),
		metric.WithUnit(unitSeconds),
		metric.WithExplicitBucketBoundaries(effectiveHistogramBuckets(buckets.ConsumeDuration, defaultConsumeDurationBuckets)...),
	); err != nil {
		return nil, err
	}
	if jm.batchSize, err = meter.Int64Histogram(
		"jetstream_receiver_batch_size",
		metric.WithDescription("Number of messages contained in a JetStream batch."),
		metric.WithExplicitBucketBoundaries(effectiveHistogramBuckets(buckets.PayloadSize, defaultPayloadSizeBuckets)...),
	); err != nil {
		return nil, err
	}
	if jm.batchDropped, err = meter.Int64Counter(
		"jetstream_receiver_batch_dropped_messages",
		metric.WithDescription("Number of messages skipped during batch processing."),
	); err != nil {
		return nil, err
	}

	return jm, nil
}

func effectiveHistogramBuckets(buckets, defaults []float64) []float64 {
	if len(buckets) == 0 {
		return append([]float64(nil), defaults...)
	}
	return append([]float64(nil), buckets...)
}

func signalForKind(kind payloadKind) string {
	switch kind {
	case payloadLogs:
		return signalLogs
	case payloadMetrics:
		return signalMetrics
	case payloadTraces:
		return signalTraces
	case payloadProfiles:
		return signalProfiles
	default:
		return "unknown"
	}
}

func (m *jetstreamMetrics) consumeAttrs(signal, subject string, attrs ...attribute.KeyValue) []attribute.KeyValue {
	if m == nil {
		return nil
	}
	result := make([]attribute.KeyValue, 0, 4+len(attrs))
	result = append(result, attribute.String(attrReceiver, m.receiverName))
	result = append(result, attribute.String(attrConsumerName, m.consumerName))
	result = append(result, attribute.String(attrSignal, signal))
	if m.includeSubject && subject != "" {
		result = append(result, attribute.String(attrSubject, subject))
	}
	result = append(result, attrs...)
	return result
}

func (m *jetstreamMetrics) recordConsumeAttempt(ctx context.Context, signal, subject string) {
	if m == nil {
		return
	}
	m.consumeAttempts.Add(ctx, 1, metric.WithAttributes(m.consumeAttrs(signal, subject)...))
}

func (m *jetstreamMetrics) recordConsumeSuccess(ctx context.Context, signal, subject string) {
	if m == nil {
		return
	}
	m.consumeSuccess.Add(ctx, 1, metric.WithAttributes(m.consumeAttrs(signal, subject)...))
}

func (m *jetstreamMetrics) recordConsumeFailure(ctx context.Context, signal, stage, subject string) {
	if m == nil {
		return
	}
	m.consumeFailures.Add(ctx, 1, metric.WithAttributes(m.consumeAttrs(signal, subject, attribute.String(attrStage, stage))...))
}

func (m *jetstreamMetrics) recordConsumeDuration(ctx context.Context, signal, subject string, duration time.Duration) {
	if m == nil {
		return
	}
	m.consumeDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(m.consumeAttrs(signal, subject)...))
}

func (m *jetstreamMetrics) recordPayloadSize(ctx context.Context, signal, subject string, size int) {
	if m == nil {
		return
	}
	m.payloadSize.Record(ctx, int64(size), metric.WithAttributes(m.consumeAttrs(signal, subject)...))
}

func (m *jetstreamMetrics) recordStartupSuccess(ctx context.Context) {
	if m == nil {
		return
	}
	m.startupSuccess.Add(ctx, 1, metric.WithAttributes(
		attribute.String(attrReceiver, m.receiverName),
		attribute.String(attrConsumerName, m.consumerName),
	))
}

func (m *jetstreamMetrics) recordStartupFailure(ctx context.Context, stage string) {
	if m == nil {
		return
	}
	m.startupFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String(attrReceiver, m.receiverName),
		attribute.String(attrConsumerName, m.consumerName),
		attribute.String(attrStage, stage),
	))
}

func (m *jetstreamMetrics) recordBatchAttempt(ctx context.Context, signal string, size int) {
	if m == nil {
		return
	}
	attrs := m.consumeAttrs(signal, "")
	m.batchAttempts.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.batchSize.Record(ctx, int64(size), metric.WithAttributes(attrs...))
}

func (m *jetstreamMetrics) recordBatchSuccess(ctx context.Context, signal string, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := m.consumeAttrs(signal, "")
	m.batchSuccess.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.batchDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

func (m *jetstreamMetrics) recordBatchFailure(ctx context.Context, signal, stage string, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := m.consumeAttrs(signal, "", attribute.String(attrStage, stage))
	m.batchFailures.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.batchDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

func (m *jetstreamMetrics) recordBatchDropped(ctx context.Context, signal string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.batchDropped.Add(ctx, int64(count), metric.WithAttributes(m.consumeAttrs(signal, "")...))
}
