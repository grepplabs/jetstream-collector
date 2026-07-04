package s3exporter

import (
	"context"
	"io"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/grepplabs/jetstream-collector/exporter/s3exporter/internal/metadata"
)

const testS3ExporterName = "s3/metrics-input"

func TestS3MetricsRecordUploadSuccess(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	set := exportertest.NewNopSettings(metadata.Type)
	set.ID = component.MustNewIDWithName(metadata.Type.String(), "metrics-input")
	set.MeterProvider = provider

	exp, err := newExporter(set, &Config{Bucket: "bucket", MarshalerType: marshalerTypeProto, FilenameAppendUUID: true})
	require.NoError(t, err)
	exp.client = &fakeObjectPutter{}

	require.NoError(t, exp.ConsumeLogs(context.Background(), plog.NewLogs()))

	rm := collectS3Metrics(t, reader)
	assertCounterValue(t, rm, "s3_exporter_upload_attempts", 1, attrExporter, testS3ExporterName, attrBucket, "bucket")
	assertCounterValue(t, rm, "s3_exporter_upload_successes", 1, attrExporter, testS3ExporterName, attrBucket, "bucket")
	assertAggregationRecorded(t, rm, "s3_exporter_upload_duration", attrExporter, testS3ExporterName, attrBucket, "bucket")
	assertAggregationRecorded(t, rm, "s3_exporter_payload_size", attrExporter, testS3ExporterName, attrBucket, "bucket")
	assertHistogramBounds(t, rm, "s3_exporter_upload_duration", []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}, attrExporter, testS3ExporterName, attrBucket, "bucket")
	assertHistogramBounds(t, rm, "s3_exporter_payload_size", []float64{128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}, attrExporter, testS3ExporterName, attrBucket, "bucket")
}

func TestS3MetricsRecordUploadFailureIncludesS3ErrorDetails(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	set := exportertest.NewNopSettings(metadata.Type)
	set.ID = component.MustNewIDWithName(metadata.Type.String(), "metrics-input")
	set.MeterProvider = provider

	exp, err := newExporter(set, &Config{Bucket: "bucket", MarshalerType: marshalerTypeProto, FilenameAppendUUID: true})
	require.NoError(t, err)
	exp.client = &fakeObjectPutter{putErr: minio.ErrorResponse{Code: "AccessDenied", StatusCode: 403}}

	err = exp.ConsumeLogs(context.Background(), plog.NewLogs())
	require.Error(t, err)
	require.Contains(t, err.Error(), "put object")
	require.True(t, consumererror.IsPermanent(err))

	rm := collectS3Metrics(t, reader)
	assertCounterValue(t, rm, "s3_exporter_upload_attempts", 1, attrExporter, testS3ExporterName, attrBucket, "bucket")
	assertCounterValue(t, rm, "s3_exporter_upload_failures", 1, attrExporter, testS3ExporterName, attrBucket, "bucket", attrStage, stageUpload, attrCode, "AccessDenied", attrStatusCode, "403")
	assertNoMetric(t, rm, "s3_exporter_upload_successes")
}

func TestS3MetricsRecordStartupCounters(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	set := exportertest.NewNopSettings(metadata.Type)
	set.ID = component.MustNewIDWithName(metadata.Type.String(), "metrics-input")
	set.MeterProvider = provider

	exp, err := newExporter(set, &Config{Bucket: "bucket", MarshalerType: marshalerTypeProto})
	require.NoError(t, err)
	exp.metrics.recordStartupSuccess(context.Background())
	exp.metrics.recordStartupFailure(context.Background(), stageConnect)

	rm := collectS3Metrics(t, reader)
	assertCounterValue(t, rm, "s3_exporter_startup_successes", 1, attrExporter, testS3ExporterName, attrBucket, "bucket")
	assertCounterValue(t, rm, "s3_exporter_startup_failures", 1, attrExporter, testS3ExporterName, attrBucket, "bucket", attrStage, stageConnect)
}

func TestS3ExporterStartRecordsStartupFailure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	set := exportertest.NewNopSettings(metadata.Type)
	set.ID = component.MustNewIDWithName(metadata.Type.String(), "metrics-input")
	set.MeterProvider = provider

	exp, err := newExporter(set, &Config{Bucket: "bucket", MarshalerType: marshalerTypeProto})
	require.NoError(t, err)

	err = exp.Start(context.Background(), nil)
	require.Error(t, err)

	rm := collectS3Metrics(t, reader)
	assertCounterValue(t, rm, "s3_exporter_startup_failures", 1, attrExporter, testS3ExporterName, attrBucket, "bucket", attrStage, stageConnect)
}

func TestS3ExporterStartRecordsStartupSuccess(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	set := exportertest.NewNopSettings(metadata.Type)
	set.ID = component.MustNewIDWithName(metadata.Type.String(), "metrics-input")
	set.MeterProvider = provider

	exp, err := newExporter(set, &Config{
		Bucket:             "bucket",
		Credentials:        CredentialsConfig{AccessKeyID: "access", SecretAccessKey: "secret"},
		Endpoint:           "http://localhost:9000",
		ForcePathStyle:     true,
		FilenameAppendUUID: true,
	})
	require.NoError(t, err)

	require.NoError(t, exp.Start(context.Background(), nil))

	rm := collectS3Metrics(t, reader)
	assertCounterValue(t, rm, "s3_exporter_startup_successes", 1, attrExporter, testS3ExporterName, attrBucket, "bucket")
}

func collectS3Metrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
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

type fakeObjectPutter struct {
	putErr error
}

func (f *fakeObjectPutter) PutObject(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (minio.UploadInfo, error) {
	if f.putErr != nil {
		return minio.UploadInfo{}, f.putErr
	}
	return minio.UploadInfo{}, nil
}

var _ objectPutter = (*fakeObjectPutter)(nil)
