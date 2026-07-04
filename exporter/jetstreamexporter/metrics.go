package jetstreamexporter

import (
	"context"
	"time"

	"github.com/grepplabs/jetstream-collector/exporter/jetstreamexporter/internal/metadata"
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

	attrSignal   = "signal"
	attrStage    = "stage"
	attrExporter = "exporter"
	attrSubject  = "subject"

	stageConnect        = "connect"
	stageBootstrap      = "bootstrap"
	stageResolveSubject = "resolve_subject"
	stageMarshal        = "marshal"
	stageCompress       = "compress"
	stagePublish        = "publish"
)

var (
	// Exporter publish latency is usually sub-second.
	defaultPublishDurationBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	// Payload sizes for OTLP batches are usually from sub-kilobyte to few kilobytes.
	defaultPayloadSizeBuckets = []float64{128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}
)

type jetstreamMetrics struct {
	exporterName    string
	includeSubject  bool
	publishAttempts metric.Int64Counter
	publishSuccess  metric.Int64Counter
	publishFailures metric.Int64Counter
	publishDuration metric.Float64Histogram
	payloadSize     metric.Int64Histogram
	startupSuccess  metric.Int64Counter
	startupFailures metric.Int64Counter
}

func newJetstreamMetrics(mp metric.MeterProvider, exporterName string, includeSubject bool, buckets MetricsBucketsConfig) (*jetstreamMetrics, error) {
	if mp == nil {
		mp = metricnoop.NewMeterProvider()
	}

	meter := mp.Meter(metadata.ScopeName)
	jm := &jetstreamMetrics{exporterName: exporterName, includeSubject: includeSubject}
	var err error

	if jm.publishAttempts, err = meter.Int64Counter(
		"jetstream_exporter_publish_attempts",
		metric.WithDescription("Number of JetStream export attempts."),
	); err != nil {
		return nil, err
	}
	if jm.publishSuccess, err = meter.Int64Counter(
		"jetstream_exporter_publish_successes",
		metric.WithDescription("Number of JetStream export attempts that were published successfully."),
	); err != nil {
		return nil, err
	}
	if jm.publishFailures, err = meter.Int64Counter(
		"jetstream_exporter_publish_failures",
		metric.WithDescription("Number of JetStream export attempts that failed before publishing."),
	); err != nil {
		return nil, err
	}
	if jm.publishDuration, err = meter.Float64Histogram(
		"jetstream_exporter_publish_duration",
		metric.WithDescription("Time spent handling a JetStream export attempt."),
		metric.WithUnit(unitSeconds),
		metric.WithExplicitBucketBoundaries(effectiveHistogramBuckets(buckets.PublishDuration, defaultPublishDurationBuckets)...),
	); err != nil {
		return nil, err
	}
	if jm.payloadSize, err = meter.Int64Histogram(
		"jetstream_exporter_payload_size",
		metric.WithDescription("Size of the JetStream payload actually published."),
		metric.WithUnit(unitBytes),
		metric.WithExplicitBucketBoundaries(effectiveHistogramBuckets(buckets.PayloadSize, defaultPayloadSizeBuckets)...),
	); err != nil {
		return nil, err
	}
	if jm.startupSuccess, err = meter.Int64Counter(
		"jetstream_exporter_startup_successes",
		metric.WithDescription("Number of JetStream exporter startups that completed successfully."),
	); err != nil {
		return nil, err
	}
	if jm.startupFailures, err = meter.Int64Counter(
		"jetstream_exporter_startup_failures",
		metric.WithDescription("Number of JetStream exporter startup failures."),
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

func (m *jetstreamMetrics) publishAttrs(signal, subject string, attrs ...attribute.KeyValue) []attribute.KeyValue {
	if m == nil {
		return nil
	}
	result := make([]attribute.KeyValue, 0, 3+len(attrs))
	result = append(result, attribute.String(attrExporter, m.exporterName))
	result = append(result, attribute.String(attrSignal, signal))
	if m.includeSubject && subject != "" {
		result = append(result, attribute.String(attrSubject, subject))
	}
	result = append(result, attrs...)
	return result
}

func (m *jetstreamMetrics) recordPublishAttempt(ctx context.Context, signal, subject string) {
	if m == nil {
		return
	}
	m.publishAttempts.Add(ctx, 1, metric.WithAttributes(m.publishAttrs(signal, subject)...))
}

func (m *jetstreamMetrics) recordPublishSuccess(ctx context.Context, signal, subject string) {
	if m == nil {
		return
	}
	m.publishSuccess.Add(ctx, 1, metric.WithAttributes(m.publishAttrs(signal, subject)...))
}

func (m *jetstreamMetrics) recordPublishFailure(ctx context.Context, signal, stage, subject string) {
	if m == nil {
		return
	}
	m.publishFailures.Add(ctx, 1, metric.WithAttributes(m.publishAttrs(signal, subject, attribute.String(attrStage, stage))...))
}

func (m *jetstreamMetrics) recordPublishDuration(ctx context.Context, signal, subject string, duration time.Duration) {
	if m == nil {
		return
	}
	m.publishDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(m.publishAttrs(signal, subject)...))
}

func (m *jetstreamMetrics) recordPayloadSize(ctx context.Context, signal, subject string, size int) {
	if m == nil {
		return
	}
	m.payloadSize.Record(ctx, int64(size), metric.WithAttributes(m.publishAttrs(signal, subject)...))
}

func (m *jetstreamMetrics) recordStartupSuccess(ctx context.Context) {
	if m == nil {
		return
	}
	m.startupSuccess.Add(ctx, 1, metric.WithAttributes(attribute.String(attrExporter, m.exporterName)))
}

func (m *jetstreamMetrics) recordStartupFailure(ctx context.Context, stage string) {
	if m == nil {
		return
	}
	m.startupFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String(attrExporter, m.exporterName),
		attribute.String(attrStage, stage),
	))
}
