//go:build integration

package s3jetstreamexporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grepplabs/jetstream-collector/exporter/s3jetstreamexporter/internal/metadata"
	"github.com/grepplabs/jetstream-collector/pkg/jetstream"
	s3client "github.com/grepplabs/jetstream-collector/pkg/s3"
	protocol "github.com/grepplabs/jetstream-collector/pkg/s3jetstream"
	"github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

func TestSeaweedFSJetStreamIntegration(t *testing.T) {
	endpoint := os.Getenv("S3_INTEGRATION_ENDPOINT")
	natsURL := os.Getenv("NATS_URL")
	if endpoint == "" || natsURL == "" {
		t.Skip("set S3_INTEGRATION_ENDPOINT and NATS_URL to run the S3/JetStream integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	streamName := "S3JETSTREAM_IT_" + suffix
	subject := "s3jetstream.integration." + suffix

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := natsjs.New(nc)
	require.NoError(t, err)
	_, err = js.CreateStream(ctx, natsjs.StreamConfig{
		Name: streamName, Subjects: []string{subject}, Retention: natsjs.LimitsPolicy,
		Storage: natsjs.MemoryStorage, MaxAge: time.Minute,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), streamName) })

	sub, err := nc.SubscribeSync(subject)
	require.NoError(t, err)
	require.NoError(t, nc.FlushWithContext(ctx))

	bucket := envOrDefault("S3_INTEGRATION_BUCKET", "s3jetstream-it-"+suffix[:12])
	accessKey := envOrDefault("S3_ACCESS_KEY_ID", "admin")
	secretKey := envOrDefault("S3_SECRET_ACCESS_KEY", "admin123")
	prefix := "s3jetstream-integration"
	region := envOrDefault("S3_REGION", "us-east-1")
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
	objectKey := ""
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if objectKey != "" {
			_ = s3.RemoveObject(cleanupCtx, bucket, objectKey, minio.RemoveObjectOptions{})
		}
		if createdBucket {
			_ = s3.RemoveBucket(cleanupCtx, bucket)
		}
		closeS3()
	})
	cfg := NewDefaultConfig()
	cfg.S3.Endpoint = endpoint
	cfg.S3.Region = region
	cfg.S3.Bucket = bucket
	cfg.S3.Prefix = prefix
	cfg.S3.Secure = strings.HasPrefix(endpoint, "https://")
	cfg.S3.ForcePathStyle = true
	cfg.S3.Credentials.AccessKeyID = accessKey
	cfg.S3.Credentials.SecretAccessKey = secretKey
	cfg.Tenant.ID = "integration-" + suffix[:12]
	cfg.JetStream.URL = natsURL
	cfg.JetStream.Subject = subject
	cfg.Object.Compression = jetstream.CompressionGzip
	cfg.Signal = protocol.SignalLogs

	exp, err := newExporter(exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, nil))
	t.Cleanup(func() { _ = exp.Shutdown(context.Background()) })

	logs := plog.NewLogs()
	record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.Body().SetStr("seaweedfs integration payload")
	require.NoError(t, exp.ConsumeLogs(ctx, logs))

	msg, err := sub.NextMsgWithContext(ctx)
	require.NoError(t, err)
	require.Equal(t, subject, msg.Subject)
	ref, err := protocol.UnmarshalObjectReference(msg.Data)
	require.NoError(t, err)
	require.Equal(t, protocol.SignalLogs, ref.Signal)
	require.Equal(t, bucket, ref.Bucket)
	require.Equal(t, jetstream.CompressionGzip, ref.Compression)
	require.True(t, strings.HasSuffix(ref.Key, ".otlp.gz"), ref.Key)
	require.NoError(t, ref.Validate(32*1024*1024, bucket, prefix))
	objectKey = ref.Key

	object, err := s3.GetObject(ctx, ref.Bucket, ref.Key, minio.GetObjectOptions{})
	require.NoError(t, err)
	defer object.Close()
	stored, err := io.ReadAll(object)
	require.NoError(t, err)
	require.Equal(t, ref.Size, int64(len(stored)))
	sum := sha256.Sum256(stored)
	require.Equal(t, hex.EncodeToString(sum[:]), ref.SHA256)

	compressed, err := gzip.NewReader(bytes.NewReader(stored))
	require.NoError(t, err)
	decoded, err := io.ReadAll(compressed)
	require.NoError(t, err)
	require.NoError(t, compressed.Close())
	req := plogotlp.NewExportRequest()
	require.NoError(t, req.UnmarshalProto(decoded))
	require.Equal(t, "seaweedfs integration payload", req.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str())
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
