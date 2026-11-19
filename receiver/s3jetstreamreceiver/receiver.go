package s3jetstreamreceiver

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	shared "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	s3client "github.com/grepplabs/jetstream-collector/pkg/s3"
	protocol "github.com/grepplabs/jetstream-collector/pkg/s3jetstream"
	"github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/xreceiver"
	"go.uber.org/zap"
)

type s3JetStreamReceiver struct {
	logger      *zap.Logger
	cfg         *Config
	signal      string
	next        any
	mu          sync.Mutex
	s3          *minio.Client
	s3Close     func()
	js          jetstream.JetStream
	sub         jetstream.ConsumeContext
	started     bool
	ackProgress time.Duration
}

var _ receiver.Logs = (*s3JetStreamReceiver)(nil)
var _ receiver.Metrics = (*s3JetStreamReceiver)(nil)
var _ receiver.Traces = (*s3JetStreamReceiver)(nil)
var _ xreceiver.Profiles = (*s3JetStreamReceiver)(nil)

func newReceiver(set receiver.Settings, cfg *Config, signal string, next any) (*s3JetStreamReceiver, error) {
	logger := set.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &s3JetStreamReceiver{logger: logger, cfg: cfg, signal: signal, next: next}, nil
}

func (r *s3JetStreamReceiver) Start(ctx context.Context, host component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return nil
	}
	s3, closeS3, err := s3client.NewS3ClientWithClose(ctx, host, s3client.S3ClientConfig{
		Endpoint: r.cfg.S3.Endpoint, Region: r.cfg.S3.Region, Secure: r.cfg.S3.Secure,
		ForcePathStyle: r.cfg.S3.ForcePathStyle, Credentials: r.cfg.S3.Credentials,
	})
	if err != nil {
		return err
	}
	keepS3 := false
	defer func() {
		if !keepS3 {
			closeS3()
		}
	}()
	js, err := shared.Connect(r.cfg.JetStream.URL, r.cfg.JetStream.TLS, r.cfg.JetStream.Auth, r.cfg.JetStream.Connection, r.logger)
	if err != nil {
		return fmt.Errorf("connect JetStream: %w", err)
	}
	if err := shared.EnsureBootstrapStreams(ctx, js, r.cfg.Bootstrap); err != nil {
		js.Conn().Close()
		return fmt.Errorf("bootstrap stream: %w", err)
	}
	stream, err := js.Stream(ctx, r.cfg.JetStream.Stream)
	if err != nil {
		js.Conn().Close()
		return fmt.Errorf("lookup processing stream: %w", err)
	}
	if err := shared.EnsureBootstrapConsumers(ctx, stream, r.cfg.Bootstrap); err != nil {
		js.Conn().Close()
		return fmt.Errorf("bootstrap consumer: %w", err)
	}
	consumer, err := stream.Consumer(ctx, r.cfg.JetStream.Consumer)
	if err != nil {
		js.Conn().Close()
		return fmt.Errorf("lookup durable consumer: %w", err)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		js.Conn().Close()
		return fmt.Errorf("inspect durable consumer: %w", err)
	}
	if info.Config.FilterSubject != r.cfg.JetStream.Subject {
		js.Conn().Close()
		return fmt.Errorf("consumer %q filter subject %q does not match configured subject %q", r.cfg.JetStream.Consumer, info.Config.FilterSubject, r.cfg.JetStream.Subject)
	}
	if info.Config.AckPolicy != jetstream.AckExplicitPolicy || info.Config.DeliverSubject != "" || info.Config.Durable != r.cfg.JetStream.Consumer {
		js.Conn().Close()
		return fmt.Errorf("consumer %q must be a durable pull consumer with explicit acknowledgements", r.cfg.JetStream.Consumer)
	}
	sub, err := consumer.Consume(func(msg jetstream.Msg) {
		if e := r.handle(ctx, msg); e != nil {
			r.logger.Error("failed processing S3 JetStream reference", zap.String("signal", r.signal), zap.Error(e))
		}
	}, jetstream.PullMaxMessages(r.cfg.Processing.Workers), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, e error) {
		if e != nil {
			r.logger.Error("JetStream consume error", zap.Error(e))
		}
	}))
	if err != nil {
		js.Conn().Close()
		return fmt.Errorf("start durable pull consumer: %w", err)
	}
	r.s3 = s3
	r.s3Close = closeS3
	r.js = js
	r.sub = sub
	r.ackProgress = 0
	if r.cfg.Processing.AckProgress {
		r.ackProgress = 10 * time.Second
		if info.Config.AckWait > 0 && info.Config.AckWait/3 < r.ackProgress {
			r.ackProgress = info.Config.AckWait / 3
		}
	}
	r.started = true
	return nil
}

func (r *s3JetStreamReceiver) Shutdown(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sub != nil {
		r.sub.Stop()
		r.sub = nil
	}
	if r.js != nil && r.js.Conn() != nil {
		r.js.Conn().Close()
	}
	if r.s3Close != nil {
		r.s3Close()
		r.s3Close = nil
	}
	r.js = nil
	r.s3 = nil
	r.started = false
	return nil
}
func (r *s3JetStreamReceiver) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
func (r *s3JetStreamReceiver) ConsumeLogs(context.Context, plog.Logs) error {
	return fmt.Errorf("receiver is configured for %s", r.signal)
}
func (r *s3JetStreamReceiver) ConsumeMetrics(context.Context, pmetric.Metrics) error {
	return fmt.Errorf("receiver is configured for %s", r.signal)
}
func (r *s3JetStreamReceiver) ConsumeTraces(context.Context, ptrace.Traces) error {
	return fmt.Errorf("receiver is configured for %s", r.signal)
}
func (r *s3JetStreamReceiver) ConsumeProfiles(context.Context, pprofile.Profiles) error {
	return fmt.Errorf("receiver is configured for %s", r.signal)
}

func (r *s3JetStreamReceiver) handle(ctx context.Context, msg jetstream.Msg) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.Processing.Timeout)
	defer cancel()
	stopProgress := r.keepAckProgress(ctx, msg)
	defer stopProgress()
	ref, err := protocol.UnmarshalObjectReference(msg.Data())
	if err != nil {
		return r.terminate(msg, "invalid_reference", err)
	}
	if err = ref.Validate(r.cfg.Processing.MaxObjectSize, r.cfg.S3.Bucket, r.cfg.S3.Prefix); err != nil {
		return r.terminate(msg, "invalid_reference", err)
	}
	if string(ref.Signal) != r.signal {
		return r.terminate(msg, "signal_mismatch", fmt.Errorf("reference signal %q does not match receiver signal %q", ref.Signal, r.signal))
	}
	r.mu.Lock()
	s3, js := r.s3, r.js
	r.mu.Unlock()
	if s3 == nil || js == nil {
		return r.nak(msg, fmt.Errorf("receiver is not started"))
	}
	obj, err := s3.GetObject(ctx, ref.Bucket, ref.Key, minio.GetObjectOptions{})
	if err != nil {
		return r.nak(msg, fmt.Errorf("get S3 object: %w", err))
	}
	defer obj.Close()
	stored, err := io.ReadAll(io.LimitReader(obj, r.cfg.Processing.MaxObjectSize+1))
	if err != nil {
		return r.nak(msg, fmt.Errorf("read S3 object: %w", err))
	}
	if int64(len(stored)) > r.cfg.Processing.MaxObjectSize {
		return r.terminate(msg, "object_too_large", fmt.Errorf("object exceeds max_object_size"))
	}
	if ref.Size > 0 && int64(len(stored)) != ref.Size {
		return r.terminate(msg, "size_mismatch", fmt.Errorf("stored object size mismatch"))
	}
	if ref.SHA256 != "" {
		sum := sha256.Sum256(stored)
		if hex.EncodeToString(sum[:]) != strings.ToLower(ref.SHA256) {
			return r.terminate(msg, "checksum_mismatch", fmt.Errorf("stored object checksum mismatch"))
		}
	}
	payload, err := decodeStoredObject(stored, ref.Compression, r.cfg.Processing.MaxObjectSize)
	if err != nil {
		if errors.Is(err, errDecompressedObjectTooLarge) {
			return r.terminate(msg, "object_too_large", err)
		}
		return r.terminate(msg, "decompress", err)
	}
	if err := r.consumePayload(ctx, payload); err != nil {
		if consumererror.IsPermanent(err) {
			return r.terminate(msg, "downstream_permanent", err)
		}
		return r.nak(msg, err)
	}
	// Re-publish the original bytes so processing and cleanup carry the same envelope.
	if _, err := js.PublishMsg(ctx, &nats.Msg{Subject: r.cfg.Cleanup.Subject, Data: msg.Data()}); err != nil {
		return r.nak(msg, fmt.Errorf("publish cleanup reference: %w", err))
	}
	if err := msg.DoubleAck(ctx); err != nil {
		return fmt.Errorf("ack processing reference: %w", err)
	}
	return nil
}

var errDecompressedObjectTooLarge = errors.New("decompressed object exceeds max_object_size")

func decodeStoredObject(stored []byte, compression string, maxSize int64) ([]byte, error) {
	if compression != protocol.CompressionGzip {
		return stored, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(stored))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	payload, err := io.ReadAll(io.LimitReader(zr, maxSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > maxSize {
		return nil, errDecompressedObjectTooLarge
	}
	return payload, nil
}

func (r *s3JetStreamReceiver) keepAckProgress(ctx context.Context, msg jetstream.Msg) func() {
	r.mu.Lock()
	interval := r.ackProgress
	r.mu.Unlock()
	if interval <= 0 {
		return func() {}
	}
	progressCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					r.logger.Warn("failed to extend JetStream ack wait", zap.Error(err))
				}
			case <-progressCtx.Done():
				return
			}
		}
	}()
	return cancel
}

func (r *s3JetStreamReceiver) consumePayload(ctx context.Context, payload []byte) error {
	switch r.signal {
	case "logs":
		req := plogotlp.NewExportRequest()
		if e := req.UnmarshalProto(payload); e != nil {
			return consumererror.NewPermanent(fmt.Errorf("decode OTLP logs: %w", e))
		}
		return r.next.(consumer.Logs).ConsumeLogs(ctx, req.Logs())
	case "metrics":
		req := pmetricotlp.NewExportRequest()
		if e := req.UnmarshalProto(payload); e != nil {
			return consumererror.NewPermanent(fmt.Errorf("decode OTLP metrics: %w", e))
		}
		return r.next.(consumer.Metrics).ConsumeMetrics(ctx, req.Metrics())
	case "traces":
		req := ptraceotlp.NewExportRequest()
		if e := req.UnmarshalProto(payload); e != nil {
			return consumererror.NewPermanent(fmt.Errorf("decode OTLP traces: %w", e))
		}
		return r.next.(consumer.Traces).ConsumeTraces(ctx, req.Traces())
	case "profiles":
		req := pprofileotlp.NewExportRequest()
		if e := req.UnmarshalProto(payload); e != nil {
			return consumererror.NewPermanent(fmt.Errorf("decode OTLP profiles: %w", e))
		}
		return r.next.(xconsumer.Profiles).ConsumeProfiles(ctx, req.Profiles())
	default:
		return consumererror.NewPermanent(fmt.Errorf("unsupported signal %q", r.signal))
	}
}
func (r *s3JetStreamReceiver) terminate(msg jetstream.Msg, reason string, err error) error {
	if e := msg.TermWithReason(reason + ": " + err.Error()); e != nil {
		return fmt.Errorf("%s: %w (terminate: %v)", reason, err, e)
	}
	return err
}
func (r *s3JetStreamReceiver) nak(msg jetstream.Msg, err error) error {
	var nakErr error
	if delay := r.cfg.Processing.RetryBackoff; delay > 0 {
		nakErr = msg.NakWithDelay(delay)
	} else {
		nakErr = msg.Nak()
	}
	if nakErr != nil {
		return fmt.Errorf("%w (NAK: %v)", err, nakErr)
	}
	return err
}
