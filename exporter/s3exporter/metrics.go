package s3exporter

import (
	"context"
	"strconv"
	"time"

	"github.com/grepplabs/jetstream-collector/exporter/s3exporter/internal/metadata"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

const (
	attrExporter   = "exporter"
	attrBucket     = "bucket"
	attrStage      = "stage"
	attrCode       = "code"
	attrStatusCode = "status_code"

	stageConnect         = "connect"
	stageResolveFilename = "resolve_filename_template"
	stageMarshal         = "marshal"
	stageCompress        = "compress"
	stageBuildObjectName = "build_object_name"
	stageUpload          = "upload"
)

var (
	defaultUploadDurationBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	defaultPayloadSizeBuckets    = []float64{128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}
)

type s3Metrics struct {
	exporterName string
	bucket       string

	uploadAttempts  metric.Int64Counter
	uploadSuccess   metric.Int64Counter
	uploadFailures  metric.Int64Counter
	uploadDuration  metric.Float64Histogram
	payloadSize     metric.Int64Histogram
	startupSuccess  metric.Int64Counter
	startupFailures metric.Int64Counter
}

func newS3Metrics(mp metric.MeterProvider, exporterName, bucket string, buckets MetricsBucketsConfig) (*s3Metrics, error) {
	if mp == nil {
		mp = metricnoop.NewMeterProvider()
	}

	meter := mp.Meter(metadata.ScopeName)
	sm := &s3Metrics{exporterName: exporterName, bucket: bucket}
	var err error

	if sm.uploadAttempts, err = meter.Int64Counter(
		"s3_exporter_upload_attempts",
		metric.WithDescription("Number of S3 upload attempts."),
	); err != nil {
		return nil, err
	}
	if sm.uploadSuccess, err = meter.Int64Counter(
		"s3_exporter_upload_successes",
		metric.WithDescription("Number of S3 upload attempts that were uploaded successfully."),
	); err != nil {
		return nil, err
	}
	if sm.uploadFailures, err = meter.Int64Counter(
		"s3_exporter_upload_failures",
		metric.WithDescription("Number of S3 upload attempts that failed before upload completed."),
	); err != nil {
		return nil, err
	}
	if sm.uploadDuration, err = meter.Float64Histogram(
		"s3_exporter_upload_duration",
		metric.WithDescription("Time spent handling an S3 upload attempt."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(effectiveHistogramBuckets(buckets.UploadDuration, defaultUploadDurationBuckets)...),
	); err != nil {
		return nil, err
	}
	if sm.payloadSize, err = meter.Int64Histogram(
		"s3_exporter_payload_size",
		metric.WithDescription("Size of the S3 payload actually uploaded."),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(effectiveHistogramBuckets(buckets.PayloadSize, defaultPayloadSizeBuckets)...),
	); err != nil {
		return nil, err
	}
	if sm.startupSuccess, err = meter.Int64Counter(
		"s3_exporter_startup_successes",
		metric.WithDescription("Number of S3 exporter startups that completed successfully."),
	); err != nil {
		return nil, err
	}
	if sm.startupFailures, err = meter.Int64Counter(
		"s3_exporter_startup_failures",
		metric.WithDescription("Number of S3 exporter startup failures."),
	); err != nil {
		return nil, err
	}

	return sm, nil
}

func effectiveHistogramBuckets(buckets, defaults []float64) []float64 {
	if len(buckets) == 0 {
		return append([]float64(nil), defaults...)
	}
	return append([]float64(nil), buckets...)
}

func (m *s3Metrics) uploadAttrs(attrs ...attribute.KeyValue) []attribute.KeyValue {
	if m == nil {
		return nil
	}
	result := make([]attribute.KeyValue, 0, 2+len(attrs))
	result = append(result, attribute.String(attrExporter, m.exporterName))
	result = append(result, attribute.String(attrBucket, m.bucket))
	result = append(result, attrs...)
	return result
}

func (m *s3Metrics) recordUploadAttempt(ctx context.Context) {
	if m == nil {
		return
	}
	m.uploadAttempts.Add(ctx, 1, metric.WithAttributes(m.uploadAttrs()...))
}

func (m *s3Metrics) recordUploadSuccess(ctx context.Context) {
	if m == nil {
		return
	}
	m.uploadSuccess.Add(ctx, 1, metric.WithAttributes(m.uploadAttrs()...))
}

func (m *s3Metrics) recordUploadFailure(ctx context.Context, stage, code string, statusCode int) {
	if m == nil {
		return
	}
	if code == "" {
		code = "unknown"
	}
	status := strconv.Itoa(statusCode)
	if statusCode <= 0 {
		status = "0"
	}
	m.uploadFailures.Add(ctx, 1, metric.WithAttributes(m.uploadAttrs(
		attribute.String(attrStage, stage),
		attribute.String(attrCode, code),
		attribute.String(attrStatusCode, status),
	)...))
}

func (m *s3Metrics) recordUploadDuration(ctx context.Context, duration time.Duration) {
	if m == nil {
		return
	}
	m.uploadDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(m.uploadAttrs()...))
}

func (m *s3Metrics) recordPayloadSize(ctx context.Context, size int) {
	if m == nil {
		return
	}
	m.payloadSize.Record(ctx, int64(size), metric.WithAttributes(m.uploadAttrs()...))
}

func (m *s3Metrics) recordStartupSuccess(ctx context.Context) {
	if m == nil {
		return
	}
	m.startupSuccess.Add(ctx, 1, metric.WithAttributes(m.uploadAttrs()...))
}

func (m *s3Metrics) recordStartupFailure(ctx context.Context, stage string) {
	if m == nil {
		return
	}
	m.startupFailures.Add(ctx, 1, metric.WithAttributes(m.uploadAttrs(attribute.String(attrStage, stage))...))
}
