package s3eventresolverextension

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/grepplabs/jetstream-collector/pkg/payloadresolver"
	"github.com/grepplabs/jetstream-collector/pkg/s3event"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type fakeS3ObjectStore struct {
	object io.ReadCloser
	bucket string
	key    string
	err    error
}

func (s *fakeS3ObjectStore) GetObject(_ context.Context, bucket, key string) (io.ReadCloser, error) {
	s.bucket = bucket
	s.key = key
	return s.object, s.err
}

func TestConfigCloudEventsMediaType(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.Bucket = "otlp"
	cfg.Credentials.AccessKeyID = "access"
	cfg.Credentials.SecretAccessKey = "secret"
	require.NoError(t, cfg.Validate())
	require.Equal(t, s3event.CloudEventsMediaType, cfg.CloudEventsMediaType)

	cfg.CloudEventsMediaType = "Application/Vnd.S3-Object-Reference+Json; charset=utf-8"
	require.NoError(t, cfg.Validate())
	require.Equal(t, "application/vnd.s3-object-reference+json", cfg.CloudEventsMediaType)

	cfg.CloudEventsMediaType = "not a media type"
	require.ErrorContains(t, cfg.Validate(), "cloud_events_media_type")
}

func TestResolversUsesConfiguredMediaType(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.CloudEventsMediaType = "application/vnd.s3-object-reference+json"
	ext := &eventResolverExtension{cfg: cfg}
	registrations := ext.Resolvers()

	require.Len(t, registrations, 1)
	require.Equal(t, "s3-object-reference", registrations[0].Name)
	require.Equal(t, []string{cfg.CloudEventsMediaType}, registrations[0].MediaTypes)
	require.Same(t, ext, registrations[0].Resolver)
}

func TestDecodeObject(t *testing.T) {
	plain := []byte("otlp payload")
	compressed := gzipBytes(t, plain)

	got, err := decodeObject(plain, contentEncodingID, int64(len(plain)))
	require.NoError(t, err)
	require.Equal(t, plain, got)

	got, err = decodeObject(compressed, contentEncodingGzip, int64(len(plain)))
	require.NoError(t, err)
	require.Equal(t, plain, got)

	_, err = decodeObject(plain, "br", int64(len(plain)))
	require.ErrorContains(t, err, "unsupported S3 object content encoding")
	_, err = decodeObject(compressed, contentEncodingGzip, 3)
	require.ErrorContains(t, err, "exceeds max_object_size")
}

func TestValidateStoredObject(t *testing.T) {
	data := []byte("stored object")
	sum := sha256.Sum256(data)
	ref := s3event.ObjectReference{
		Size:   int64(len(data)),
		SHA256: hex.EncodeToString(sum[:]),
	}
	require.NoError(t, validateStoredObject(data, ref, int64(len(data))))
	require.ErrorContains(t, validateStoredObject(data, ref, int64(len(data)-1)), "max_object_size")
	ref.Size++
	require.ErrorContains(t, validateStoredObject(data, ref, int64(len(data))), "size does not match")
	ref.Size--
	ref.SHA256 = "invalid"
	require.ErrorContains(t, validateStoredObject(data, ref, int64(len(data))), "SHA-256")
}

func TestValidateCloudEvent(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.Bucket = "otlp"
	event := s3event.CloudEvent{
		SpecVersion:     s3event.CloudEventsVersion,
		ID:              "event-1",
		Source:          "test",
		Type:            s3event.ObjectCreatedType,
		Time:            time.Now(),
		DataContentType: mediaTypeJSON,
		DataSchema:      s3event.ObjectSchema,
		Data: s3event.ObjectReference{
			Bucket:      "otlp",
			Key:         "logs/one",
			Signal:      signalLogs,
			ContentType: mediaTypeProto,
			Size:        10,
		},
	}
	require.NoError(t, validateCloudEvent(event, cfg))

	event.Data.ContentEncoding = "br"
	require.ErrorContains(t, validateCloudEvent(event, cfg), "content encoding")
}

func TestResolveRejectsDifferentConfiguredMediaType(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.Bucket = "otlp"
	cfg.Credentials.AccessKeyID = "access"
	cfg.Credentials.SecretAccessKey = "secret"
	require.NoError(t, cfg.Validate())
	ext := &eventResolverExtension{cfg: cfg}

	_, err := ext.Resolve(t.Context(), payloadresolver.Input{Data: []byte(`{}`), MediaType: "application/vnd.other+json"})
	require.ErrorContains(t, err, "unsupported content-type")
}

func TestResolveDownloadsVerifiesAndDecodesObject(t *testing.T) {
	payload := []byte("OTLP payload")
	stored := gzipBytes(t, payload)
	digest := sha256.Sum256(stored)
	cfg := NewDefaultConfig()
	cfg.Bucket = "otlp"
	cfg.Credentials.AccessKeyID = "access"
	cfg.Credentials.SecretAccessKey = "secret"
	cfg.CloudEventsMediaType = "application/vnd.s3-object-reference+json"
	require.NoError(t, cfg.Validate())

	store := &fakeS3ObjectStore{object: io.NopCloser(bytes.NewReader(stored))}
	ext := &eventResolverExtension{logger: zap.NewNop(), cfg: cfg, client: store}
	event := s3event.CloudEvent{
		SpecVersion:     s3event.CloudEventsVersion,
		ID:              "event-1",
		Source:          "test",
		Type:            s3event.ObjectCreatedType,
		Time:            time.Now().UTC(),
		DataContentType: mediaTypeJSON,
		DataSchema:      s3event.ObjectSchema,
		Data: s3event.ObjectReference{
			Bucket:          "otlp",
			Key:             "logs/one.gz",
			Signal:          signalLogs,
			ContentType:     mediaTypeProto,
			ContentEncoding: contentEncodingGzip,
			Size:            int64(len(stored)),
			SHA256:          hex.EncodeToString(digest[:]),
		},
	}
	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)

	resolved, err := ext.Resolve(context.Background(), payloadresolver.Input{
		Data: eventBytes, MediaType: cfg.CloudEventsMediaType,
	})
	require.NoError(t, err)
	require.Equal(t, payload, resolved.Data)
	require.Equal(t, mediaTypeProto, resolved.MediaType)
	require.Equal(t, event.Data.Bucket, store.bucket)
	require.Equal(t, event.Data.Key, store.key)
}

func TestResolveRejectsObjectHashMismatch(t *testing.T) {
	stored := []byte("tampered payload")
	cfg := NewDefaultConfig()
	cfg.Bucket = "otlp"
	cfg.Credentials.AccessKeyID = "access"
	cfg.Credentials.SecretAccessKey = "secret"
	require.NoError(t, cfg.Validate())
	ext := &eventResolverExtension{
		cfg:    cfg,
		client: &fakeS3ObjectStore{object: io.NopCloser(bytes.NewReader(stored))},
	}
	event := s3event.CloudEvent{
		SpecVersion: s3event.CloudEventsVersion, ID: "event-1", Source: "test", Type: s3event.ObjectCreatedType,
		DataContentType: mediaTypeJSON, DataSchema: s3event.ObjectSchema,
		Data: s3event.ObjectReference{
			Bucket: "otlp", Key: "logs/one", Signal: signalLogs, ContentType: mediaTypeProto,
			Size: int64(len(stored)), SHA256: strings.Repeat("0", 64),
		},
	}
	eventBytes, err := json.Marshal(event)
	require.NoError(t, err)
	_, err = ext.Resolve(context.Background(), payloadresolver.Input{Data: eventBytes, MediaType: cfg.CloudEventsMediaType})
	require.ErrorContains(t, err, "SHA-256 does not match")
}

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, err := writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}
