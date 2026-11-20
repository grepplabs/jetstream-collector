// Package s3eventresolverextension resolves S3 object-created CloudEvents into OTLP payloads.
package s3eventresolverextension

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/grepplabs/jetstream-collector/pkg/payloadresolver"
	s3client "github.com/grepplabs/jetstream-collector/pkg/s3"
	"github.com/grepplabs/jetstream-collector/pkg/s3event"
	"github.com/minio/minio-go/v7"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"
)

const (
	componentType       = "s3eventresolver"
	mediaTypeJSON       = "application/json"
	mediaTypeProto      = "application/x-protobuf"
	contentEncodingID   = "identity"
	contentEncodingGzip = "gzip"
	signalLogs          = "logs"
	signalMetrics       = "metrics"
	signalTraces        = "traces"
	signalProfiles      = "profiles"
)

type eventResolverExtension struct {
	logger *zap.Logger
	cfg    *Config

	mu      sync.RWMutex
	client  s3ObjectStore
	closeS3 func()
	started bool
}

type s3ObjectStore interface {
	GetObject(context.Context, string, string) (io.ReadCloser, error)
}

type minioObjectStore struct {
	client *minio.Client
}

func (s minioObjectStore) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	return s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
}

var _ extension.Extension = (*eventResolverExtension)(nil)
var _ payloadresolver.ResolverExtension = (*eventResolverExtension)(nil)

func NewFactory() extension.Factory {
	typ := component.MustNewType(componentType)
	return extension.NewFactory(typ,
		func() component.Config { return NewDefaultConfig() },
		func(_ context.Context, set extension.Settings, baseCfg component.Config) (extension.Extension, error) {
			cfg, ok := baseCfg.(*Config)
			if !ok {
				return nil, fmt.Errorf("config structure is not of type *s3eventresolverextension.Config")
			}
			if err := cfg.Validate(); err != nil {
				return nil, err
			}
			logger := set.Logger
			if logger == nil {
				logger = zap.NewNop()
			}
			return &eventResolverExtension{logger: logger, cfg: cfg}, nil
		},
		component.StabilityLevelDevelopment,
	)
}

func (e *eventResolverExtension) Resolvers() []payloadresolver.Registration {
	return []payloadresolver.Registration{{
		Name:       "s3-object-reference",
		MediaTypes: []string{e.cfg.CloudEventsMediaType},
		Resolver:   e,
	}}
}

func (e *eventResolverExtension) Start(ctx context.Context, host component.Host) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return nil
	}
	client, closeS3, err := s3client.NewS3ClientWithClose(ctx, host, s3client.S3ClientConfig{
		Endpoint: e.cfg.Endpoint, Region: e.cfg.Region, Secure: e.cfg.Secure,
		ForcePathStyle: e.cfg.ForcePathStyle, Credentials: e.cfg.Credentials,
	})
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}
	e.client = minioObjectStore{client: client}
	e.closeS3 = closeS3
	e.started = true
	e.logger.Info("S3 event resolver started",
		zap.String("bucket", e.cfg.Bucket),
		zap.String("endpoint", e.resolvedEndpoint()),
		zap.String("region", e.resolvedRegion()),
		zap.Bool("force_path_style", e.cfg.ForcePathStyle),
	)
	return nil
}

func (e *eventResolverExtension) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.client = nil
	if e.closeS3 != nil {
		e.closeS3()
		e.closeS3 = nil
	}
	e.started = false
	return nil
}

func (e *eventResolverExtension) Resolve(ctx context.Context, input payloadresolver.Input) (payloadresolver.ResolvedPayload, error) {
	mediaType, err := payloadresolver.NormalizeMediaType(input.MediaType)
	if err != nil || mediaType != e.cfg.CloudEventsMediaType {
		return payloadresolver.ResolvedPayload{}, consumererror.NewPermanent(fmt.Errorf("unsupported content-type %q", input.MediaType))
	}
	var event s3event.CloudEvent
	if err := json.Unmarshal(input.Data, &event); err != nil {
		return payloadresolver.ResolvedPayload{}, consumererror.NewPermanent(fmt.Errorf("decode S3 CloudEvent: %w", err))
	}
	if err := validateCloudEvent(event, e.cfg); err != nil {
		return payloadresolver.ResolvedPayload{}, consumererror.NewPermanent(err)
	}

	e.mu.RLock()
	client := e.client
	e.mu.RUnlock()
	if client == nil {
		return payloadresolver.ResolvedPayload{}, fmt.Errorf("S3 event resolver is not started")
	}
	object, err := client.GetObject(ctx, event.Data.Bucket, event.Data.Key)
	if err != nil {
		return payloadresolver.ResolvedPayload{}, fmt.Errorf("get S3 object %q: %w", event.Data.Key, err)
	}
	defer object.Close()
	stored, err := io.ReadAll(io.LimitReader(object, e.cfg.MaxObjectSize+1))
	if err != nil {
		return payloadresolver.ResolvedPayload{}, fmt.Errorf("read S3 object %q: %w", event.Data.Key, err)
	}
	if err := validateStoredObject(stored, event.Data, e.cfg.MaxObjectSize); err != nil {
		return payloadresolver.ResolvedPayload{}, consumererror.NewPermanent(err)
	}
	payload, err := decodeObject(stored, event.Data.ContentEncoding, e.cfg.MaxObjectSize)
	if err != nil {
		return payloadresolver.ResolvedPayload{}, consumererror.NewPermanent(err)
	}
	e.logger.Debug("resolved S3 object event",
		zap.String("event_id", event.ID),
		zap.String("bucket", event.Data.Bucket),
		zap.String("key", event.Data.Key),
	)
	return payloadresolver.ResolvedPayload{Data: payload, MediaType: event.Data.ContentType}, nil
}

func validateStoredObject(stored []byte, ref s3event.ObjectReference, maxSize int64) error {
	if int64(len(stored)) > maxSize {
		return fmt.Errorf("S3 object exceeds max_object_size")
	}
	if int64(len(stored)) != ref.Size {
		return fmt.Errorf("S3 object size does not match event reference")
	}
	if ref.SHA256 == "" {
		return nil
	}
	sum := sha256.Sum256(stored)
	if !strings.EqualFold(hex.EncodeToString(sum[:]), ref.SHA256) {
		return fmt.Errorf("S3 object SHA-256 does not match event reference")
	}
	return nil
}

func validateCloudEvent(event s3event.CloudEvent, cfg *Config) error {
	if event.SpecVersion != s3event.CloudEventsVersion || event.ID == "" || event.Source == "" || event.Type != s3event.ObjectCreatedType {
		return fmt.Errorf("unsupported or incomplete CloudEvent envelope")
	}
	if event.DataContentType != mediaTypeJSON || event.DataSchema != s3event.ObjectSchema {
		return fmt.Errorf("unsupported CloudEvent data content type or schema")
	}
	ref := event.Data
	if ref.Bucket == "" || ref.Bucket != cfg.Bucket {
		return fmt.Errorf("event bucket is empty or does not match configured bucket")
	}
	if !isSafeRelativeKey(ref.Key) {
		return fmt.Errorf("event key must be a normalized relative S3 key")
	}
	if ref.Size < 0 || ref.Size > cfg.MaxObjectSize {
		return fmt.Errorf("event object size is outside max_object_size")
	}
	if ref.SHA256 != "" {
		if len(ref.SHA256) != 64 {
			return fmt.Errorf("event SHA-256 must contain 64 hexadecimal characters")
		}
		if _, err := hex.DecodeString(ref.SHA256); err != nil {
			return fmt.Errorf("invalid event SHA-256: %w", err)
		}
	}
	switch ref.Signal {
	case signalLogs, signalMetrics, signalTraces, signalProfiles:
	default:
		return fmt.Errorf("unsupported event signal %q", ref.Signal)
	}
	mediaType, err := payloadresolver.NormalizeMediaType(ref.ContentType)
	if err != nil {
		return fmt.Errorf("invalid event content type: %w", err)
	}
	if mediaType != mediaTypeProto && mediaType != mediaTypeJSON {
		return fmt.Errorf("unsupported event content type %q", ref.ContentType)
	}
	if ref.ContentEncoding != "" && ref.ContentEncoding != contentEncodingID && ref.ContentEncoding != contentEncodingGzip {
		return fmt.Errorf("unsupported event content encoding %q", ref.ContentEncoding)
	}
	return nil
}

func isSafeRelativeKey(key string) bool {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "\\") || path.Clean(key) != key {
		return false
	}
	for segment := range strings.SplitSeq(key, "/") {
		if segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

func decodeObject(stored []byte, encoding string, maxSize int64) ([]byte, error) {
	switch encoding {
	case "", contentEncodingID:
		return stored, nil
	case contentEncodingGzip:
		reader, err := gzip.NewReader(bytes.NewReader(stored))
		if err != nil {
			return nil, fmt.Errorf("open gzip S3 object: %w", err)
		}
		defer reader.Close()
		payload, err := io.ReadAll(io.LimitReader(reader, maxSize+1))
		if err != nil {
			return nil, fmt.Errorf("decompress S3 object: %w", err)
		}
		if int64(len(payload)) > maxSize {
			return nil, errors.New("decompressed S3 object exceeds max_object_size")
		}
		return payload, nil
	default:
		return nil, fmt.Errorf("unsupported S3 object content encoding %q", encoding)
	}
}

func (e *eventResolverExtension) resolvedEndpoint() string {
	endpoint, _, _ := s3client.ResolveEndpoint(e.cfg.Endpoint, e.cfg.Region, e.cfg.Secure)
	return endpoint
}

func (e *eventResolverExtension) resolvedRegion() string {
	_, _, region := s3client.ResolveEndpoint(e.cfg.Endpoint, e.cfg.Region, e.cfg.Secure)
	return region
}
