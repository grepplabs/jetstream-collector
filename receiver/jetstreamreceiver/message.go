package jetstreamreceiver

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"mime"
	"strings"
	"time"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type decodedMessage struct {
	msg    jetstream.Msg
	logger *zap.Logger

	Subject string
	Headers nats.Header
	Payload []byte
}

func decodeMessage(msg jetstream.Msg, defaultCompression string, logger *zap.Logger) (decodedMessage, error) {
	payload, err := decodePayload(msg, defaultCompression)
	if err != nil {
		return decodedMessage{}, err
	}

	return decodedMessage{
		msg:     msg,
		logger:  logger,
		Subject: msg.Subject(),
		Headers: msg.Headers(),
		Payload: payload,
	}, nil
}

func (m decodedMessage) Msg() jetstream.Msg {
	return m.msg
}

func (m decodedMessage) Ack() error {
	return m.msg.Ack()
}

func (m decodedMessage) Nak() error {
	return m.msg.Nak()
}

func (m decodedMessage) NakWithDelay(delay time.Duration) error {
	return m.msg.NakWithDelay(delay)
}

func (m decodedMessage) TermWithReason(reason string) error {
	return m.msg.TermWithReason(reason)
}

func logReceivedMessage(msg jetstream.Msg, logger *zap.Logger) {
	if !logger.Core().Enabled(zap.DebugLevel) {
		return
	}

	fields := []zap.Field{
		zap.String("subject", msg.Subject()),
		zap.Int("payload_size", len(msg.Data())),
	}

	if ct := headerValue(msg.Headers(), sharedjetstream.HeaderContentType); ct != "" {
		fields = append(fields, zap.String("content_type", ct))
	}
	if ce := headerValue(msg.Headers(), sharedjetstream.HeaderContentEncoding); ce != "" {
		fields = append(fields, zap.String("content_encoding", ce))
	}

	if meta, err := msg.Metadata(); err == nil {
		fields = append(fields,
			zap.String("stream", meta.Stream),
			zap.String("consumer", meta.Consumer),
			zap.Uint64("stream_seq", meta.Sequence.Stream),
			zap.Uint64("consumer_seq", meta.Sequence.Consumer),
			zap.Uint64("delivery_count", meta.NumDelivered),
			zap.Time("published", meta.Timestamp),
		)
	}

	logger.Debug("Received message", fields...)
}

func (m decodedMessage) logAckMessage() {
	if !m.logger.Core().Enabled(zap.DebugLevel) {
		return
	}

	fields := []zap.Field{
		zap.String("subject", m.Subject),
	}

	if meta, err := m.msg.Metadata(); err == nil {
		fields = append(fields,
			zap.Uint64("stream_seq", meta.Sequence.Stream),
			zap.Uint64("delivery_count", meta.NumDelivered),
		)
	}

	m.logger.Debug("ACK message", fields...)
}

func (m decodedMessage) logRetryMessage(operation string, err error) {
	if !m.logger.Core().Enabled(zap.DebugLevel) {
		return
	}

	fields := []zap.Field{
		zap.String("operation", operation),
		zap.String("subject", m.Subject),
		zap.Error(err),
	}

	if meta, metaErr := m.msg.Metadata(); metaErr == nil {
		fields = append(fields,
			zap.Uint64("stream_seq", meta.Sequence.Stream),
			zap.Uint64("delivery_count", meta.NumDelivered),
		)
	}

	m.logger.Debug("Retry message", fields...)
}

func decodePayload(msg jetstream.Msg, defaultCompression string) ([]byte, error) {
	compression, err := payloadCompressionFromHeaders(msg.Headers(), defaultCompression)
	if err != nil {
		return nil, err
	}
	if compression != sharedjetstream.CompressionGzip {
		return msg.Data(), nil
	}
	return gunzipPayload(msg.Data())
}

func payloadCompressionFromHeaders(headers nats.Header, defaultCompression string) (string, error) {
	if encoding := headerValue(headers, sharedjetstream.HeaderContentEncoding); encoding != "" {
		return sharedjetstream.NormalizeCompression(encoding)
	}
	return sharedjetstream.NormalizeCompression(defaultCompression)
}

func gunzipPayload(body []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create gzip reader: %w", err)
	}
	defer zr.Close()

	decoded, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("read gzip payload: %w", err)
	}
	return decoded, nil
}

func payloadFormatFromHeaders(headers nats.Header) (string, error) {
	ct := headerValue(headers, sharedjetstream.HeaderContentType)
	if ct == "" {
		return sharedjetstream.ContentTypeProto, nil
	}

	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return "", fmt.Errorf("parse content-type %q: %w", ct, err)
	}

	switch strings.ToLower(mediaType) {
	case "application/json", "application/x-json", "text/json", "application/*+json":
		return sharedjetstream.ContentTypeJSON, nil
	case "application/x-protobuf", "application/protobuf", "application/octet-stream", "application/grpc+proto", "application/*+proto":
		return sharedjetstream.ContentTypeProto, nil
	default:
		if strings.HasSuffix(strings.ToLower(mediaType), "+json") {
			return sharedjetstream.ContentTypeJSON, nil
		}
		if strings.HasSuffix(strings.ToLower(mediaType), "+proto") {
			return sharedjetstream.ContentTypeProto, nil
		}
		return "", fmt.Errorf("unsupported content-type %q", ct)
	}
}

func headerValue(headers nats.Header, key string) string {
	if len(headers) == 0 {
		return ""
	}
	return headers.Get(key)
}
