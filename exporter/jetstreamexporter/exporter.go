package jetstreamexporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer/consumererror"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

var gzPool = sync.Pool{
	New: func() any {
		return gzip.NewWriter(io.Discard)
	},
}

type jetstreamPublisher interface {
	Conn() *nats.Conn
	PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

type otlpRequest interface {
	MarshalProto() ([]byte, error)
	MarshalJSON() ([]byte, error)
}

type jetstreamExporter struct {
	logger         *zap.Logger
	cfg            *Config
	subjectPattern *subjectTemplate
	metrics        *jetstreamMetrics

	mu      sync.Mutex
	js      jetstreamPublisher
	started bool
}

func newExporter(set exporter.Settings, cfg *Config) (*jetstreamExporter, error) {
	logger := set.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	buckets := cfg.metricsBuckets()
	metrics, err := newJetstreamMetrics(set.MeterProvider, set.ID.String(), cfg.IncludeSubject, buckets)
	if err != nil {
		return nil, err
	}

	subjectPattern, err := newSubjectTemplate(cfg.SubjectPattern)
	if err != nil {
		return nil, err
	}

	return &jetstreamExporter{
		logger:         logger,
		cfg:            cfg,
		subjectPattern: subjectPattern,
		metrics:        metrics,
	}, nil
}

func (e *jetstreamExporter) Start(ctx context.Context, _ component.Host) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return nil
	}

	js, err := e.connect()
	if err != nil {
		e.metrics.recordStartupFailure(ctx, stageConnect)
		return err
	}

	if err := sharedjetstream.EnsureBootstrapStreams(ctx, js, e.cfg.Bootstrap); err != nil {
		e.metrics.recordStartupFailure(ctx, stageBootstrap)
		if conn := js.Conn(); conn != nil {
			conn.Close()
		}
		return err
	}

	e.js = js
	e.started = true
	e.metrics.recordStartupSuccess(ctx)

	fields := make([]zap.Field, 0, 3)
	if e.cfg.Subject != "" {
		fields = append(fields, zap.String("subject", e.cfg.Subject))
	}
	if e.cfg.SubjectPattern != "" {
		fields = append(fields, zap.String("subject_pattern", e.cfg.SubjectPattern))
	}
	fields = append(fields, zap.String("url", e.cfg.URL))
	e.logger.Info("jetstream exporter started", fields...)
	return nil
}

func (e *jetstreamExporter) Shutdown(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.started {
		return nil
	}
	if e.js != nil && e.js.Conn() != nil {
		_ = e.js.Conn().Drain()
		e.js.Conn().Close()
	}
	e.js = nil
	e.started = false
	return nil
}

func (e *jetstreamExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *jetstreamExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return e.consume(ctx, signalLogs, NewLogsSubjectResolver(ld, e.cfg.Subject, e.subjectPattern), plogotlp.NewExportRequestFromLogs(ld))
}

func (e *jetstreamExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return e.consume(ctx, signalMetrics, NewMetricsSubjectResolver(md, e.cfg.Subject, e.subjectPattern), pmetricotlp.NewExportRequestFromMetrics(md))
}

func (e *jetstreamExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return e.consume(ctx, signalTraces, NewTracesSubjectResolver(td, e.cfg.Subject, e.subjectPattern), ptraceotlp.NewExportRequestFromTraces(td))
}

func (e *jetstreamExporter) ConsumeProfiles(ctx context.Context, pd pprofile.Profiles) error {
	return e.consume(ctx, signalProfiles, NewProfilesSubjectResolver(pd, e.cfg.Subject, e.subjectPattern), pprofileotlp.NewExportRequestFromProfiles(pd))
}

func (e *jetstreamExporter) consume(ctx context.Context, signal string, resolver SubjectResolver, req otlpRequest) error {
	subject, err := resolver.Resolve(ctx)
	if err != nil {
		e.metrics.recordPublishFailure(ctx, signal, stageResolveSubject, "")
		return consumererror.NewPermanent(fmt.Errorf("resolve subject: %w", err))
	}
	return e.publish(ctx, signal, subject, req)
}

func (e *jetstreamExporter) publish(ctx context.Context, signal, subject string, req otlpRequest) error {
	start := time.Now()
	e.metrics.recordPublishAttempt(ctx, signal, subject)
	defer func() {
		e.metrics.recordPublishDuration(ctx, signal, subject, time.Since(start))
	}()

	payload, err := marshalOTLP(req, e.cfg.ContentType)
	if err != nil {
		e.metrics.recordPublishFailure(ctx, signal, stageMarshal, subject)
		return err
	}

	msgID := ""
	if e.cfg.MsgID {
		msgID = msgIDHeaderValue(subject, payload)
	}

	if e.cfg.Compression == sharedjetstream.CompressionGzip {
		payload, err = gzipCompress(payload)
		if err != nil {
			e.metrics.recordPublishFailure(ctx, signal, stageCompress, subject)
			return fmt.Errorf("gzip compress payload: %w", err)
		}
	}

	e.metrics.recordPayloadSize(ctx, signal, subject, len(payload))

	err = e.publishPayload(ctx, subject, payload, msgID)
	if err != nil {
		e.metrics.recordPublishFailure(ctx, signal, stagePublish, subject)
		return err
	}

	e.metrics.recordPublishSuccess(ctx, signal, subject)
	return nil
}

func marshalOTLP(req otlpRequest, contentType string) ([]byte, error) {
	switch contentType {
	case sharedjetstream.ContentTypeJSON:
		payload, err := req.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("marshal otlp json payload: %w", err)
		}
		return payload, nil
	case sharedjetstream.ContentTypeProto:
		payload, err := req.MarshalProto()
		if err != nil {
			return nil, fmt.Errorf("marshal otlp proto payload: %w", err)
		}
		return payload, nil
	default:
		return nil, fmt.Errorf("unsupported content type %q", contentType)
	}
}

func (e *jetstreamExporter) publishPayload(ctx context.Context, subject string, payload []byte, msgID string) error {
	e.mu.Lock()
	js := e.js
	e.mu.Unlock()
	if js == nil {
		return fmt.Errorf("jetstream client is not started")
	}

	msg := nats.NewMsg(subject)
	msg.Data = payload
	msg.Header = nats.Header{}
	if len(e.cfg.Headers) > 0 {
		for k, v := range e.cfg.Headers {
			msg.Header.Set(k, v)
		}
	}
	if len(e.cfg.MetadataHeaders) > 0 {
		info := client.FromContext(ctx)
		for _, headerName := range e.cfg.MetadataHeaders {
			if values := info.Metadata.Get(headerName); len(values) > 0 {
				for _, value := range values {
					msg.Header.Add(headerName, value)
				}
			}
		}
	}
	msg.Header.Set(sharedjetstream.HeaderContentType, contentTypeHeaderValue(e.cfg.ContentType))
	if e.cfg.Compression == sharedjetstream.CompressionGzip {
		msg.Header.Set(sharedjetstream.HeaderContentEncoding, sharedjetstream.CompressionGzip)
	}
	if msgID != "" {
		msg.Header.Set(nats.MsgIdHdr, msgID)
	}

	e.logPublishedMessage(msg)

	_, err := js.PublishMsg(ctx, msg)
	return classifyPublishError(subject, err)
}

func classifyPublishError(subject string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, jetstream.ErrNoStreamResponse) ||
		errors.Is(err, nats.ErrBadSubject) ||
		errors.Is(err, nats.ErrAuthorization) ||
		errors.Is(err, nats.ErrPermissionViolation) ||
		errors.Is(err, nats.ErrInvalidMsg) {
		return consumererror.NewPermanent(fmt.Errorf("publish to subject %q: %w", subject, err))
	}
	return err
}

func msgIDHeaderValue(subject string, payload []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(subject))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(payload)
	sum := h.Sum(nil)
	return "v1:" + hex.EncodeToString(sum)
}

func (e *jetstreamExporter) connect() (jetstream.JetStream, error) {
	return sharedjetstream.Connect(e.cfg.URL, e.cfg.TLS, e.cfg.Auth, e.logger)
}

func contentTypeHeaderValue(value string) string {
	switch value {
	case sharedjetstream.ContentTypeJSON:
		return sharedjetstream.MIMEContentTypeJSON
	case sharedjetstream.ContentTypeProto:
		return sharedjetstream.MIMEContentTypeProto
	default:
		return sharedjetstream.MIMEContentTypeProto
	}
}

func (e *jetstreamExporter) logPublishedMessage(msg *nats.Msg) {
	if !e.logger.Core().Enabled(zap.DebugLevel) {
		return
	}

	fields := []zap.Field{
		zap.String("subject", msg.Subject),
		zap.Int("payload_size", len(msg.Data)),
	}

	if ct := headerValue(msg.Header, sharedjetstream.HeaderContentType); ct != "" {
		fields = append(fields, zap.String("content_type", ct))
	}
	if ce := headerValue(msg.Header, sharedjetstream.HeaderContentEncoding); ce != "" {
		fields = append(fields, zap.String("content_encoding", ce))
	}
	if msgID := headerValue(msg.Header, nats.MsgIdHdr); msgID != "" {
		fields = append(fields, zap.String("msg_id", msgID))
	}

	e.logger.Debug("Published message", fields...)
}

func headerValue(headers nats.Header, key string) string {
	if len(headers) == 0 {
		return ""
	}
	return headers.Get(key)
}

func gzipCompress(body []byte) ([]byte, error) {
	gz := gzPool.Get().(*gzip.Writer)
	defer func() {
		gz.Reset(io.Discard)
		gzPool.Put(gz)
	}()

	var b bytes.Buffer
	gz.Reset(&b)

	if _, err := gz.Write(body); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
