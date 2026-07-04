package s3exporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.uber.org/zap"
)

var gzipWriterPool = sync.Pool{
	New: func() any {
		return gzip.NewWriter(io.Discard)
	},
}

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
	cfg              *Config
	filenameTemplate *keyPrefixTemplate
	metrics          *s3Metrics

	mu      sync.Mutex
	client  objectPutter
	started bool
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

	client, err := e.connect(ctx, host)
	if err != nil {
		e.metrics.recordStartupFailure(ctx, stageConnect)
		return err
	}

	e.client = client
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
	e.started = false
	return nil
}

func (e *s3Exporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *s3Exporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	start := time.Now()
	e.metrics.recordUploadAttempt(ctx)
	defer func() {
		e.metrics.recordUploadDuration(ctx, time.Since(start))
	}()

	resolvedAt := time.Now().UTC()
	filenameTemplateValue, err := newLogsKeyPrefixResolver(ld, e.filenameTemplate, resolvedAt).Resolve(ctx)
	if err != nil {
		e.metrics.recordUploadFailure(ctx, stageResolveFilename, "", 0)
		return consumererror.NewPermanent(fmt.Errorf("resolve filename_template: %w", err))
	}

	req := plogotlp.NewExportRequestFromLogs(ld)
	payload, err := marshalOTLP(req, e.cfg.MarshalerType)
	if err != nil {
		e.metrics.recordUploadFailure(ctx, stageMarshal, "", 0)
		return err
	}

	if e.cfg.Compression == compressionGzip {
		payload, err = gzipCompress(payload)
		if err != nil {
			e.metrics.recordUploadFailure(ctx, stageCompress, "", 0)
			return fmt.Errorf("gzip compress payload: %w", err)
		}
	}

	e.metrics.recordPayloadSize(ctx, len(payload))

	objectName, err := e.buildObjectName(filenameTemplateValue)
	if err != nil {
		e.metrics.recordUploadFailure(ctx, stageBuildObjectName, "", 0)
		return consumererror.NewPermanent(err)
	}

	return e.upload(ctx, objectName, payload)
}

func (e *s3Exporter) upload(ctx context.Context, objectName string, payload []byte) error {
	e.mu.Lock()
	client := e.client
	e.mu.Unlock()
	if client == nil {
		e.metrics.recordUploadFailure(ctx, stageUpload, "unknown", 0)
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
		e.metrics.recordUploadFailure(ctx, stageUpload, code, statusCode)
		return classifyS3Error(e.logger,
			fmt.Errorf(
				"put object bucket=%q key=%q: %w",
				e.cfg.Bucket,
				objectName,
				err,
			),
		)
	}

	e.metrics.recordUploadSuccess(ctx)
	e.logger.Debug("uploaded s3 object",
		zap.String("bucket", e.cfg.Bucket),
		zap.String("key", objectName),
		zap.Int("payload_size", len(payload)),
	)
	return nil
}

func s3ErrorDetails(err error) (string, int) {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
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

	var netErr net.Error
	if errors.As(err, &netErr) {
		return consumererror.NewRetryableError(err)
	}

	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
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

func (e *s3Exporter) connect(ctx context.Context, host component.Host) (objectPutter, error) {
	endpoint, secure := e.resolveEndpointAndSecure()
	creds, err := e.resolveCredentials(ctx, host)
	if err != nil {
		return nil, err
	}

	opts := &minio.Options{
		Creds:        creds,
		Secure:       secure,
		Region:       e.resolvedRegion(),
		BucketLookup: minio.BucketLookupAuto,
	}
	if e.cfg.ForcePathStyle {
		opts.BucketLookup = minio.BucketLookupPath
	}

	return minio.New(endpoint, opts)
}

func (e *s3Exporter) resolveCredentials(ctx context.Context, host component.Host) (*credentials.Credentials, error) {
	if e.cfg.Credentials.AccessKeyID != "" || e.cfg.Credentials.SecretAccessKey != "" {
		return credentials.NewStaticV4(e.cfg.Credentials.AccessKeyID, e.cfg.Credentials.SecretAccessKey, e.cfg.Credentials.SessionToken), nil
	}

	if e.cfg.Credentials.Provider == (component.ID{}) {
		return nil, fmt.Errorf("no credentials configured")
	}
	if host == nil {
		return nil, fmt.Errorf("unknown credentials provider extension %q", e.cfg.Credentials.Provider)
	}

	ext, ok := host.GetExtensions()[e.cfg.Credentials.Provider]
	if !ok {
		return nil, fmt.Errorf("unknown credentials provider extension %q", e.cfg.Credentials.Provider)
	}

	provider, ok := ext.(interface {
		GetCredentialsProvider() aws.CredentialsProvider
	})
	if !ok {
		return nil, fmt.Errorf("extension %q does not provide retrievable credentials", e.cfg.Credentials.Provider)
	}

	creds, err := provider.GetCredentialsProvider().Retrieve(ctx)
	if err != nil {
		return nil, err
	}
	return credentials.NewStaticV4(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken), nil
}

func (e *s3Exporter) resolveEndpointAndSecure() (string, bool) {
	endpoint := strings.TrimSpace(e.cfg.Endpoint)
	secure := e.cfg.Secure
	if endpoint == "" {
		endpoint = defaultAWSEndpoint(e.resolvedRegion())
		return endpoint, secure
	}

	if u, err := url.Parse(endpoint); err == nil && u.Scheme != "" && u.Host != "" {
		if u.Scheme == "http" {
			secure = false
		}
		if u.Scheme == "https" {
			secure = true
		}
		endpoint = u.Host
	}

	return endpoint, secure
}

func (e *s3Exporter) resolvedEndpoint() string {
	endpoint, _ := e.resolveEndpointAndSecure()
	return endpoint
}

func (e *s3Exporter) resolvedRegion() string {
	if e.cfg.Region != "" {
		return e.cfg.Region
	}
	return defaultRegion
}

func defaultAWSEndpoint(region string) string {
	if region == "" || region == defaultRegion {
		return "s3.amazonaws.com"
	}
	return "s3." + region + ".amazonaws.com"
}

func gzipCompress(body []byte) ([]byte, error) {
	gz := gzipWriterPool.Get().(*gzip.Writer)
	defer func() {
		gz.Reset(io.Discard)
		gzipWriterPool.Put(gz)
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
