//go:build integration

package s3exporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grepplabs/jetstream-collector/exporter/s3exporter/internal/metadata"
	s3client "github.com/grepplabs/jetstream-collector/pkg/s3"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func TestSeaweedFSIntegration(t *testing.T) {
	endpoint := os.Getenv("S3_INTEGRATION_ENDPOINT")
	if endpoint == "" {
		t.Skip("set S3_INTEGRATION_ENDPOINT to run the S3 integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	region := integrationEnvOrDefault("S3_REGION", "us-east-1")
	bucket := integrationEnvOrDefault("S3_INTEGRATION_BUCKET", "s3exporter-it-"+suffix[:12])
	accessKey := integrationEnvOrDefault("S3_ACCESS_KEY_ID", "admin")
	secretKey := integrationEnvOrDefault("S3_SECRET_ACCESS_KEY", "admin123")

	s3, closeS3, err := s3client.NewS3ClientWithClose(ctx, nil, s3client.S3ClientConfig{
		Endpoint: endpoint, Region: region, Secure: strings.HasPrefix(endpoint, "https://"), ForcePathStyle: true,
		Credentials: s3client.S3CredentialsConfig{AccessKeyID: accessKey, SecretAccessKey: secretKey},
	})
	require.NoError(t, err)
	createdBucket := false
	exists, err := s3.BucketExists(ctx, bucket)
	require.NoError(t, err)
	if !exists {
		require.NoError(t, s3.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region}))
		createdBucket = true
	}
	var objectKeys []string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		for _, key := range objectKeys {
			_ = s3.RemoveObject(cleanupCtx, bucket, key, minio.RemoveObjectOptions{})
		}
		if createdBucket {
			_ = s3.RemoveBucket(cleanupCtx, bucket)
		}
		closeS3()
	})

	for _, signal := range []string{signalLogs, signalMetrics, signalTraces} {
		for _, compression := range []string{compressionNone, compressionGzip} {
			t.Run(signal+"/"+compression, func(t *testing.T) {
				objectPrefix := fmt.Sprintf("s3exporter-integration/%s/%s-%s", signal, compression, suffix)
				cfg := NewDefaultConfig()
				cfg.Endpoint = endpoint
				cfg.Region = region
				cfg.Bucket = bucket
				cfg.Secure = strings.HasPrefix(endpoint, "https://")
				cfg.ForcePathStyle = true
				cfg.Credentials.AccessKeyID = accessKey
				cfg.Credentials.SecretAccessKey = secretKey
				cfg.FilenameTemplate = objectPrefix
				cfg.FilenameAppendUUID = false
				cfg.FilenameExtension = "otlp"
				cfg.Compression = compression

				exp, err := newExporter(exportertest.NewNopSettings(metadata.Type), cfg)
				require.NoError(t, err)
				require.NoError(t, exp.Start(ctx, nil))
				t.Cleanup(func() { _ = exp.Shutdown(context.Background()) })

				switch signal {
				case signalLogs:
					logs := plog.NewLogs()
					record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
					record.Body().SetStr("s3 logs integration payload")
					require.NoError(t, exp.ConsumeLogs(ctx, logs))
				case signalMetrics:
					metrics := pmetric.NewMetrics()
					metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
					metric.SetEmptyGauge()
					metric.SetName("s3.integration.value")
					metric.Gauge().DataPoints().AppendEmpty().SetDoubleValue(42)
					require.NoError(t, exp.ConsumeMetrics(ctx, metrics))
				case signalTraces:
					traces := ptrace.NewTraces()
					span := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
					span.SetName("s3 traces integration payload")
					require.NoError(t, exp.ConsumeTraces(ctx, traces))
				}

				objectKey := objectPrefix + ".otlp"
				if compression == compressionGzip {
					objectKey += ".gz"
				}
				objectKeys = append(objectKeys, objectKey)
				object, err := s3.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{})
				require.NoError(t, err)
				defer object.Close()
				stored, err := io.ReadAll(object)
				require.NoError(t, err)

				payload := stored
				if compression == compressionGzip {
					compressed, err := gzip.NewReader(bytes.NewReader(stored))
					require.NoError(t, err)
					payload, err = io.ReadAll(compressed)
					require.NoError(t, err)
					require.NoError(t, compressed.Close())
				}

				switch signal {
				case signalLogs:
					req := plogotlp.NewExportRequest()
					require.NoError(t, req.UnmarshalProto(payload))
					require.Equal(t, "s3 logs integration payload", req.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str())
				case signalMetrics:
					req := pmetricotlp.NewExportRequest()
					require.NoError(t, req.UnmarshalProto(payload))
					got := req.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
					require.Equal(t, "s3.integration.value", got.Name())
					require.Equal(t, float64(42), got.Gauge().DataPoints().At(0).DoubleValue())
				case signalTraces:
					req := ptraceotlp.NewExportRequest()
					require.NoError(t, req.UnmarshalProto(payload))
					require.Equal(t, "s3 traces integration payload", req.Traces().ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())
				}
			})
		}
	}
}

func integrationEnvOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
