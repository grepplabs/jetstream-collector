package s3exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	s3client "github.com/grepplabs/jetstream-collector/pkg/s3"
	"github.com/grepplabs/jetstream-collector/pkg/s3event"
	"github.com/minio/minio-go/v7"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/zap"
)

// otlpRequest matches the OTLP export request methods used by the S3 exporter.
type otlpRequest interface {
	MarshalProto() ([]byte, error)
	MarshalJSON() ([]byte, error)
}

type objectPutter interface {
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
}

func marshalOTLP(req otlpRequest, contentType string) ([]byte, error) {
	switch contentType {
	case marshalerTypeJSON:
		payload, err := req.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("marshal otlp json payload: %w", err)
		}
		return payload, nil
	case marshalerTypeProto:
		payload, err := req.MarshalProto()
		if err != nil {
			return nil, fmt.Errorf("marshal otlp proto payload: %w", err)
		}
		return payload, nil
	default:
		return nil, fmt.Errorf("unsupported marshaler type %q", contentType)
	}
}

type s3Exporter struct {
	logger           *zap.Logger
	id               component.ID
	cfg              *Config
	filenameTemplate *keyPrefixTemplate
	metrics          *s3Metrics

	mu       sync.Mutex
	client   objectPutter
	notifier s3event.Notifier
	s3Close  func()
	started  bool
}

func newExporter(set exporter.Settings, cfg *Config) (*s3Exporter, error) {
	logger := set.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	filenameTemplate, err := newKeyPrefixTemplate(cfg.FilenameTemplate)
	if err != nil {
		return nil, err
	}

	buckets := cfg.metricsBuckets()
	metrics, err := newS3Metrics(set.MeterProvider, set.ID.String(), cfg.Bucket, buckets)
	if err != nil {
		return nil, err
	}

	return &s3Exporter{
		logger:           logger,
		id:               set.ID,
		cfg:              cfg,
		filenameTemplate: filenameTemplate,
		metrics:          metrics,
	}, nil
}

func (e *s3Exporter) Start(ctx context.Context, host component.Host) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return nil
	}
	var notifier s3event.Notifier
	if target := e.cfg.Notification.Target; target != (component.ID{}) {
		if host == nil {
			return fmt.Errorf("notification target %q unavailable: component host is nil", target)
		}
		ext, ok := host.GetExtensions()[target]
		if !ok {
			return fmt.Errorf("unknown notification target extension %q", target)
		}
		notifier, ok = ext.(s3event.Notifier)
		if !ok {
			return fmt.Errorf("notification target extension %q does not implement s3event.Notifier", target)
		}
	}

	client, closeS3, err := e.connect(ctx, host)
	if err != nil {
		e.metrics.recordStartupFailure(ctx, stageConnect)
		return err
	}

	e.client = client
	e.notifier = notifier
	e.s3Close = closeS3
	e.started = true
	e.metrics.recordStartupSuccess(ctx)
	e.logger.Info("s3 exporter started",
		zap.String("bucket", e.cfg.Bucket),
		zap.String("endpoint", e.resolvedEndpoint()),
		zap.String("region", e.resolvedRegion()),
		zap.Bool("force_path_style", e.cfg.ForcePathStyle),
	)
	return nil
}

func (e *s3Exporter) Shutdown(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.client = nil
	e.notifier = nil
	if e.s3Close != nil {
		e.s3Close()
		e.s3Close = nil
	}
	e.started = false
	return nil
}

func (e *s3Exporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *s3Exporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return e.consume(ctx, signalLogs, newLogsKeyPrefixResolver(ld, e.filenameTemplate, time.Now().UTC()), plogotlp.NewExportRequestFromLogs(ld))
}

func (e *s3Exporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return e.consume(ctx, signalMetrics, newMetricsKeyPrefixResolver(md, e.filenameTemplate, time.Now().UTC()), pmetricotlp.NewExportRequestFromMetrics(md))
}

func (e *s3Exporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return e.consume(ctx, signalTraces, newTracesKeyPrefixResolver(td, e.filenameTemplate, time.Now().UTC()), ptraceotlp.NewExportRequestFromTraces(td))
}

func (e *s3Exporter) consume(ctx context.Context, signal string, resolver KeyPrefixResolver, req otlpRequest) error {
	start := time.Now()
	e.metrics.recordUploadAttempt(ctx, signal)
	defer func() {
		e.metrics.recordUploadDuration(ctx, signal, time.Since(start))
	}()

	filenameTemplateValue, err := resolver.Resolve(ctx)
	if err != nil {
		e.metrics.recordUploadFailure(ctx, signal, stageResolveFilename, "", 0)
		return consumererror.NewPermanent(fmt.Errorf("resolve filename_template: %w", err))
	}

	payload, err := marshalOTLP(req, e.cfg.MarshalerType)
	if err != nil {
		e.metrics.recordUploadFailure(ctx, signal, stageMarshal, "", 0)
		return err
	}

	if e.cfg.Compression == compressionGzip {
		payload, err = s3client.GzipBytes(payload)
		if err != nil {
			e.metrics.recordUploadFailure(ctx, signal, stageCompress, "", 0)
			return fmt.Errorf("gzip compress payload: %w", err)
		}
	}

	e.metrics.recordPayloadSize(ctx, signal, len(payload))

	objectName, err := e.buildObjectName(filenameTemplateValue)
	if err != nil {
		e.metrics.recordUploadFailure(ctx, signal, stageBuildObjectName, "", 0)
		return consumererror.NewPermanent(err)
	}

	return e.upload(ctx, signal, objectName, payload)
}

func (e *s3Exporter) upload(ctx context.Context, signal, objectName string, payload []byte) error {
	e.mu.Lock()
	client := e.client
	notifier := e.notifier
	e.mu.Unlock()
	if client == nil {
		e.metrics.recordUploadFailure(ctx, signal, stageUpload, "unknown", 0)
		return fmt.Errorf("s3 client is not started")
	}

	contentType := mimeContentTypeProto
	if e.cfg.MarshalerType == marshalerTypeJSON {
		contentType = mimeContentTypeJSON
	}
	contentEncoding := ""
	if e.cfg.Compression == compressionGzip {
		contentEncoding = contentEncodingGzip
	}

	opts := minio.PutObjectOptions{
		ContentType: contentType,
	}
	if contentEncoding != "" {
		opts.ContentEncoding = contentEncoding
	}
	_, err := client.PutObject(ctx, e.cfg.Bucket, objectName, bytes.NewReader(payload), int64(len(payload)), opts)
	if err != nil {
		code, statusCode := s3ErrorDetails(err)
		e.metrics.recordUploadFailure(ctx, signal, stageUpload, code, statusCode)
		return classifyS3Error(e.logger,
			fmt.Errorf(
				"put object bucket=%q key=%q: %w",
				e.cfg.Bucket,
				objectName,
				err,
			),
		)
	}

	e.metrics.recordUploadSuccess(ctx, signal)
	e.logger.Debug("uploaded s3 object",
		zap.String("bucket", e.cfg.Bucket),
		zap.String("key", objectName),
		zap.Int("payload_size", len(payload)),
	)
	if notifier != nil {
		uploadedAt := time.Now().UTC()
		payloadHash := sha256.Sum256(payload)
		event, err := s3event.NewObjectCreated("urn:opentelemetry:exporter:"+e.id.String(), e.cfg.Bucket, objectName, s3event.ObjectReference{
			Bucket:          e.cfg.Bucket,
			Key:             objectName,
			Signal:          signal,
			API:             "PutObject",
			ContentType:     contentType,
			ContentEncoding: contentEncoding,
			Size:            int64(len(payload)),
			SHA256:          hex.EncodeToString(payloadHash[:]),
			UploadedAt:      uploadedAt,
		})
		if err != nil {
			return fmt.Errorf("create S3 object notification: %w", err)
		}
		if err := notifier.Notify(ctx, event); err != nil {
			return fmt.Errorf("notify target after S3 upload bucket=%q key=%q: %w", e.cfg.Bucket, objectName, err)
		}
	}
	return nil
}

func s3ErrorDetails(err error) (string, int) {
	if resp, ok := errors.AsType[minio.ErrorResponse](err); ok {
		return resp.Code, resp.StatusCode
	}
	return "", 0
}

func classifyS3Error(logger *zap.Logger, err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) {
		return err
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return consumererror.NewRetryableError(err)
	}

	if _, ok := errors.AsType[net.Error](err); ok {
		return consumererror.NewRetryableError(err)
	}

	if resp, ok := errors.AsType[minio.ErrorResponse](err); ok {
		switch resp.Code {
		case "SlowDown",
			"RequestTimeout",
			"OperationAborted",
			"InternalError",
			"ServiceUnavailable":
			return consumererror.NewRetryableError(err)

		case "AccessDenied",
			"InvalidAccessKeyId",
			"SignatureDoesNotMatch",
			"NoSuchBucket",
			"InvalidBucketName",
			"InvalidArgument",
			"InvalidRequest",
			"PermanentRedirect":
			return consumererror.NewPermanent(err)
		}

		logger.Debug("Unhandled S3 error code, falling back to HTTP status",
			zap.String("code", resp.Code),
			zap.Int("status_code", resp.StatusCode),
			zap.String("request_id", resp.RequestID),
			zap.String("server", resp.Server),
		)

		switch {
		case resp.StatusCode == http.StatusRequestTimeout:
			return consumererror.NewRetryableError(err)

		case resp.StatusCode == http.StatusTooManyRequests:
			return consumererror.NewRetryableError(err)

		case resp.StatusCode >= 500:
			return consumererror.NewRetryableError(err)

		case resp.StatusCode >= 400:
			return consumererror.NewPermanent(err)
		}
	}
	// assume retryable
	return consumererror.NewRetryableError(err)
}

func (e *s3Exporter) buildObjectName(prefix string) (string, error) {
	nameParts := make([]string, 0, 2)
	if prefix != "" {
		nameParts = append(nameParts, prefix)
	}

	fileName := strings.Join(nameParts, "")
	if e.cfg.FilenameAppendUUID {
		fileName += uuid.NewString()
	}

	if e.cfg.FilenameExtension != "" {
		if fileName != "" {
			fileName += "."
		}
		fileName += e.cfg.FilenameExtension
	}
	if e.cfg.Compression == compressionGzip {
		fileName += ".gz"
	}
	return fileName, nil
}

func (e *s3Exporter) connect(ctx context.Context, host component.Host) (objectPutter, func(), error) {
	return s3client.NewS3ClientWithClose(ctx, host, s3client.S3ClientConfig{
		Endpoint:       e.cfg.Endpoint,
		Region:         e.cfg.Region,
		Secure:         e.cfg.Secure,
		ForcePathStyle: e.cfg.ForcePathStyle,
		Credentials:    e.cfg.Credentials,
	})
}

func (e *s3Exporter) resolvedEndpoint() string {
	endpoint, _, _ := s3client.ResolveEndpoint(e.cfg.Endpoint, e.cfg.Region, e.cfg.Secure)
	return endpoint
}

func (e *s3Exporter) resolvedRegion() string {
	_, _, region := s3client.ResolveEndpoint(e.cfg.Endpoint, e.cfg.Region, e.cfg.Secure)
	return region
}
