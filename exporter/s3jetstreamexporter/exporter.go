package s3jetstreamexporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/grepplabs/jetstream-collector/pkg/jetstream"
	s3client "github.com/grepplabs/jetstream-collector/pkg/s3"
	protocol "github.com/grepplabs/jetstream-collector/pkg/s3jetstream"
	"github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/zap"
)

type objectClient interface {
	PutObject(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (minio.UploadInfo, error)
}
type publisher interface {
	Conn() *nats.Conn
	PublishMsg(context.Context, *nats.Msg, ...natsjs.PublishOpt) (*natsjs.PubAck, error)
}
type protoRequest interface{ MarshalProto() ([]byte, error) }

type s3JetStreamExporter struct {
	logger  *zap.Logger
	cfg     *Config
	mu      sync.Mutex
	s3      objectClient
	s3Close func()
	js      publisher
	started bool
}

func newExporter(set exporter.Settings, cfg *Config) (*s3JetStreamExporter, error) {
	logger := set.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &s3JetStreamExporter{logger: logger, cfg: cfg}, nil
}

func (e *s3JetStreamExporter) Start(ctx context.Context, host component.Host) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return nil
	}
	s3, closeS3, err := s3client.NewS3ClientWithClose(ctx, host, s3client.S3ClientConfig{
		Endpoint:       e.cfg.S3.Endpoint,
		Region:         e.cfg.S3.Region,
		Secure:         e.cfg.S3.Secure,
		ForcePathStyle: e.cfg.S3.ForcePathStyle,
		Credentials:    e.cfg.S3.Credentials,
	})
	if err != nil {
		return err
	}
	js, err := jetstream.Connect(e.cfg.JetStream.URL, e.cfg.JetStream.TLS, e.cfg.JetStream.Auth, e.cfg.JetStream.Connection, e.logger)
	if err != nil {
		closeS3()
		return fmt.Errorf("connect JetStream: %w", err)
	}
	e.s3 = s3
	e.s3Close = closeS3
	e.js = js
	e.started = true
	return nil
}
func (e *s3JetStreamExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.js != nil && e.js.Conn() != nil {
		e.js.Conn().Close()
	}
	if e.s3Close != nil {
		e.s3Close()
		e.s3Close = nil
	}
	e.js = nil
	e.s3 = nil
	e.started = false
	return nil
}
func (e *s3JetStreamExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
func (e *s3JetStreamExporter) ConsumeTraces(ctx context.Context, d ptrace.Traces) error {
	return e.consume(ctx, protocol.SignalTraces, ptraceotlp.NewExportRequestFromTraces(d))
}
func (e *s3JetStreamExporter) ConsumeMetrics(ctx context.Context, d pmetric.Metrics) error {
	return e.consume(ctx, protocol.SignalMetrics, pmetricotlp.NewExportRequestFromMetrics(d))
}
func (e *s3JetStreamExporter) ConsumeLogs(ctx context.Context, d plog.Logs) error {
	return e.consume(ctx, protocol.SignalLogs, plogotlp.NewExportRequestFromLogs(d))
}
func (e *s3JetStreamExporter) ConsumeProfiles(ctx context.Context, d pprofile.Profiles) error {
	return e.consume(ctx, protocol.SignalProfiles, pprofileotlp.NewExportRequestFromProfiles(d))
}

func (e *s3JetStreamExporter) consume(ctx context.Context, signal protocol.Signal, req protoRequest) error {
	if signal != e.cfg.Signal {
		return fmt.Errorf("configured signal %q cannot export %q data", e.cfg.Signal, signal)
	}
	ctx, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()
	subject, err := e.resolveSubject(ctx)
	if err != nil {
		return err
	}
	tenant := e.cfg.Tenant.ID
	if e.cfg.Tenant.MetadataKey != "" {
		values := client.FromContext(ctx).Metadata.Get(e.cfg.Tenant.MetadataKey)
		if len(values) != 1 || strings.TrimSpace(values[0]) == "" {
			return fmt.Errorf("tenant metadata key %q must have exactly one nonempty value", e.cfg.Tenant.MetadataKey)
		}
		tenant = values[0]
	}
	if err := protocol.ValidateTenant(tenant); err != nil {
		return fmt.Errorf("invalid tenant: %w", err)
	}
	payload, err := req.MarshalProto()
	if err != nil {
		return fmt.Errorf("marshal OTLP protobuf: %w", err)
	}
	if e.cfg.Object.Compression == jetstream.CompressionGzip {
		payload, err = s3client.GzipBytes(payload)
		if err != nil {
			return err
		}
	}
	at := time.Now().UTC()
	id, err := protocol.NewUUIDv7()
	if err != nil {
		return err
	}
	key, err := protocol.BuildObjectKey(e.cfg.S3.Prefix, at, tenant, id)
	if err != nil {
		return err
	}
	if e.cfg.Object.Compression == jetstream.CompressionGzip {
		key += ".gz"
	}
	ref := protocol.ObjectReference{Version: protocol.ObjectReferenceVersion, ID: id, Signal: signal, Tenant: tenant, Bucket: e.cfg.S3.Bucket, Key: key, Encoding: jetstream.ContentTypeProto, Compression: e.cfg.Object.Compression, Size: int64(len(payload))}
	sum := sha256.Sum256(payload)
	ref.SHA256 = hex.EncodeToString(sum[:])
	e.mu.Lock()
	s3, js := e.s3, e.js
	e.mu.Unlock()
	if s3 == nil || js == nil {
		return fmt.Errorf("s3jetstream exporter is not started")
	}
	opts := minio.PutObjectOptions{ContentType: jetstream.MIMEContentTypeProto}
	if ref.Compression == jetstream.CompressionGzip {
		opts.ContentEncoding = jetstream.CompressionGzip
	}
	if _, err = s3.PutObject(ctx, ref.Bucket, ref.Key, bytes.NewReader(payload), ref.Size, opts); err != nil {
		return fmt.Errorf("put S3 object %q/%q: %w", ref.Bucket, ref.Key, err)
	}
	data, err := ref.Marshal()
	if err != nil {
		return err
	}
	msg := nats.NewMsg(subject)
	msg.Data = data
	msg.Header.Set(jetstream.HeaderContentType, jetstream.MIMEContentTypeJSON)
	if _, err = js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("publish object reference to %q after S3 upload: %w", subject, err)
	}
	e.logger.Debug("published S3 object reference", zap.String("id", id), zap.String("signal", string(signal)), zap.String("subject", subject), zap.Int64("stored_size", ref.Size))
	return nil
}

func (e *s3JetStreamExporter) resolveSubject(ctx context.Context) (string, error) {
	key := e.cfg.JetStream.SubjectFromMetadataKey
	if key == "" {
		return e.cfg.JetStream.Subject, nil
	}
	values := client.FromContext(ctx).Metadata.Get(key)
	if len(values) == 0 {
		return e.cfg.JetStream.Subject, nil
	}
	if len(values) != 1 || values[0] == "" {
		return "", fmt.Errorf("subject metadata key %q must have exactly one nonempty value", key)
	}
	if err := validatePublishSubject(values[0]); err != nil {
		return "", fmt.Errorf("subject metadata key %q: %w", key, err)
	}
	return values[0], nil
}

func validatePublishSubject(subject string) error {
	if subject == "" || strings.TrimSpace(subject) != subject {
		return fmt.Errorf("subject must be nonempty and have no surrounding whitespace")
	}
	for _, r := range subject {
		if unicode.IsSpace(r) || unicode.IsControl(r) || r == '*' || r == '>' {
			return fmt.Errorf("subject contains an invalid character")
		}
	}
	for token := range strings.SplitSeq(subject, ".") {
		if token == "" {
			return fmt.Errorf("subject must not contain empty tokens")
		}
	}
	return nil
}
