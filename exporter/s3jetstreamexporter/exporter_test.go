package s3jetstreamexporter

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	protocol "github.com/grepplabs/jetstream-collector/pkg/s3jetstream"
	"github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type fakeObjects struct {
	calls       *[]string
	data        []byte
	bucket, key string
	err         error
}

func (f *fakeObjects) PutObject(_ context.Context, bucket, key string, body io.Reader, size int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	*f.calls = append(*f.calls, "put")
	f.bucket = bucket
	f.key = key
	f.data, _ = io.ReadAll(body)
	if int64(len(f.data)) != size {
		return minio.UploadInfo{}, errors.New("wrong size")
	}
	return minio.UploadInfo{}, f.err
}

type fakePublisher struct {
	calls *[]string
	msg   *nats.Msg
	err   error
}

func (f *fakePublisher) Conn() *nats.Conn { return nil }
func (f *fakePublisher) PublishMsg(_ context.Context, msg *nats.Msg, _ ...natsjs.PublishOpt) (*natsjs.PubAck, error) {
	*f.calls = append(*f.calls, "publish")
	f.msg = msg
	return &natsjs.PubAck{}, f.err
}

func TestConsumeLogsUploadsBeforePublishingReference(t *testing.T) {
	calls := []string{}
	objects := &fakeObjects{calls: &calls}
	pub := &fakePublisher{calls: &calls}
	cfg := NewDefaultConfig()
	cfg.S3.Bucket = "otlp"
	cfg.Tenant.ID = "tenant-a"
	cfg.JetStream.Subject = "custom.ingest"
	cfg.Signal = protocol.SignalLogs
	e := &s3JetStreamExporter{logger: zap.NewNop(), cfg: cfg, s3: objects, js: pub}
	err := e.ConsumeLogs(context.Background(), plog.NewLogs())
	require.NoError(t, err)
	require.Equal(t, []string{"put", "publish"}, calls)
	require.Equal(t, "otlp", objects.bucket)
	require.Contains(t, objects.key, "/tenant-a/")
	require.Equal(t, "custom.ingest", pub.msg.Subject)
	ref, err := protocol.UnmarshalObjectReference(pub.msg.Data)
	require.NoError(t, err)
	require.Equal(t, protocol.SignalLogs, ref.Signal)
	require.Equal(t, objects.key, ref.Key)
	require.Equal(t, int64(len(objects.data)), ref.Size)
	require.NoError(t, ref.Validate(1024, "otlp", ""))
}

func TestConsumeReturnsPublishFailureAfterDurablePut(t *testing.T) {
	calls := []string{}
	objects := &fakeObjects{calls: &calls}
	pub := &fakePublisher{calls: &calls, err: errors.New("no puback")}
	cfg := NewDefaultConfig()
	cfg.S3.Bucket = "otlp"
	cfg.Tenant.ID = "tenant-a"
	cfg.JetStream.Subject = "custom.ingest"
	cfg.Signal = protocol.SignalLogs
	e := &s3JetStreamExporter{logger: zap.NewNop(), cfg: cfg, s3: objects, js: pub}
	require.Error(t, e.ConsumeLogs(context.Background(), plog.NewLogs()))
	require.Equal(t, []string{"put", "publish"}, calls)
}

func TestTenantMetadataMustBeSingularAndSafe(t *testing.T) {
	calls := []string{}
	cfg := NewDefaultConfig()
	cfg.S3.Bucket = "otlp"
	cfg.Tenant.MetadataKey = "x-tenant"
	cfg.JetStream.Subject = "custom.ingest"
	cfg.Signal = protocol.SignalLogs
	e := &s3JetStreamExporter{logger: zap.NewNop(), cfg: cfg, s3: &fakeObjects{calls: &calls}, js: &fakePublisher{calls: &calls}}
	for _, values := range [][]string{nil, {"one", "two"}, {"../bad"}} {
		ctx := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"x-tenant": values})})
		require.Error(t, e.ConsumeLogs(ctx, plog.NewLogs()))
	}
	require.Empty(t, calls)
	ctx := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"x-tenant": {"valid-tenant"}})})
	require.NoError(t, e.ConsumeLogs(ctx, plog.NewLogs()))
	require.Equal(t, []string{"put", "publish"}, calls)
}

func TestSubjectFromMetadataUsesConfiguredDefault(t *testing.T) {
	t.Run("metadata subject", func(t *testing.T) {
		calls := []string{}
		cfg := NewDefaultConfig()
		cfg.S3.Bucket = "otlp"
		cfg.Tenant.ID = "tenant-a"
		cfg.JetStream.Subject = "fallback.traces"
		cfg.JetStream.SubjectFromMetadataKey = "x-subject"
		cfg.Signal = protocol.SignalLogs
		pub := &fakePublisher{calls: &calls}
		e := &s3JetStreamExporter{logger: zap.NewNop(), cfg: cfg, s3: &fakeObjects{calls: &calls}, js: pub}
		ctx := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"x-subject": {"tenant-a.traces"}})})
		require.NoError(t, e.ConsumeLogs(ctx, plog.NewLogs()))
		require.Equal(t, "tenant-a.traces", pub.msg.Subject)
	})

	t.Run("default when metadata is absent", func(t *testing.T) {
		calls := []string{}
		cfg := NewDefaultConfig()
		cfg.S3.Bucket = "otlp"
		cfg.Tenant.ID = "tenant-a"
		cfg.JetStream.Subject = "fallback.traces"
		cfg.JetStream.SubjectFromMetadataKey = "x-subject"
		cfg.Signal = protocol.SignalLogs
		pub := &fakePublisher{calls: &calls}
		e := &s3JetStreamExporter{logger: zap.NewNop(), cfg: cfg, s3: &fakeObjects{calls: &calls}, js: pub}
		require.NoError(t, e.ConsumeLogs(context.Background(), plog.NewLogs()))
		require.Equal(t, "fallback.traces", pub.msg.Subject)
	})
}

func TestSubjectFromMetadataRejectsAmbiguousOrInvalidValuesBeforeUpload(t *testing.T) {
	for _, values := range [][]string{{"one.traces", "two.traces"}, {"bad subject"}, {"wild.>"}} {
		calls := []string{}
		cfg := NewDefaultConfig()
		cfg.S3.Bucket = "otlp"
		cfg.Tenant.ID = "tenant-a"
		cfg.JetStream.Subject = "fallback.traces"
		cfg.JetStream.SubjectFromMetadataKey = "x-subject"
		cfg.Signal = protocol.SignalLogs
		e := &s3JetStreamExporter{logger: zap.NewNop(), cfg: cfg, s3: &fakeObjects{calls: &calls}, js: &fakePublisher{calls: &calls}}
		ctx := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"x-subject": values})})
		require.Error(t, e.ConsumeLogs(ctx, plog.NewLogs()))
		require.Empty(t, calls)
	}
}

func TestGzipObjectUsesGzipExtension(t *testing.T) {
	calls := []string{}
	objects := &fakeObjects{calls: &calls}
	pub := &fakePublisher{calls: &calls}
	cfg := NewDefaultConfig()
	cfg.S3.Bucket = "otlp"
	cfg.Tenant.ID = "tenant-a"
	cfg.JetStream.Subject = "custom.ingest"
	cfg.Object.Compression = protocol.CompressionGzip
	cfg.Signal = protocol.SignalLogs
	e := &s3JetStreamExporter{logger: zap.NewNop(), cfg: cfg, s3: objects, js: pub}
	require.NoError(t, e.ConsumeLogs(context.Background(), plog.NewLogs()))
	require.True(t, strings.HasSuffix(objects.key, ".otlp.gz"), objects.key)
	ref, err := protocol.UnmarshalObjectReference(pub.msg.Data)
	require.NoError(t, err)
	require.Equal(t, protocol.CompressionGzip, ref.Compression)
	require.Equal(t, objects.key, ref.Key)
}
