package jetstreamreceiver

import (
	"context"
	"errors"
	"testing"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
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
	"go.uber.org/zap"
)

func TestHandleMessageAcceptsProtoJSONAndCompressionDefaultAndOverride(t *testing.T) {
	tests := []struct {
		name              string
		kind              payloadKind
		cfgCompression    string
		contentType       string
		contentEncoding   string
		payloadCompressed bool
		body              []byte
	}{
		{name: "logs proto default", kind: payloadLogs, cfgCompression: sharedjetstream.CompressionNone, contentType: "application/x-protobuf", body: mustMarshalProto(t, plogotlp.NewExportRequest())},
		{name: "logs json gzip header", kind: payloadLogs, cfgCompression: sharedjetstream.CompressionNone, contentType: "application/json", contentEncoding: "gzip", payloadCompressed: true, body: mustMarshalJSON(t, plogotlp.NewExportRequest())},
		{name: "metrics proto default gzip", kind: payloadMetrics, cfgCompression: sharedjetstream.CompressionGzip, contentType: "application/x-protobuf", payloadCompressed: true, body: mustMarshalProto(t, pmetricotlp.NewExportRequest())},
		{name: "metrics json gzip header overrides default none", kind: payloadMetrics, cfgCompression: sharedjetstream.CompressionNone, contentType: "application/json", contentEncoding: "gzip", payloadCompressed: true, body: mustMarshalJSON(t, pmetricotlp.NewExportRequest())},
		{name: "traces proto gzip header overrides default none", kind: payloadTraces, cfgCompression: sharedjetstream.CompressionNone, contentType: "application/x-protobuf", contentEncoding: "gzip", payloadCompressed: true, body: mustMarshalProto(t, ptraceotlp.NewExportRequest())},
		{name: "traces json default none", kind: payloadTraces, cfgCompression: sharedjetstream.CompressionNone, contentType: "application/json", body: mustMarshalJSON(t, ptraceotlp.NewExportRequest())},
		{name: "profiles proto default", kind: payloadProfiles, cfgCompression: sharedjetstream.CompressionNone, contentType: "application/x-protobuf", body: mustMarshalProto(t, pprofileotlp.NewExportRequest())},
		{name: "profiles json gzip header", kind: payloadProfiles, cfgCompression: sharedjetstream.CompressionNone, contentType: "application/json", contentEncoding: "gzip", payloadCompressed: true, body: mustMarshalJSON(t, pprofileotlp.NewExportRequest())},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			r := &jetstreamReceiver{logger: zap.NewNop(), kind: tt.kind, cfg: &Config{Compression: tt.cfgCompression}}
			switch tt.kind {
			case payloadLogs:
				r.nextLogs = mustLogsConsumer(t, &called)
			case payloadMetrics:
				r.nextMetrics = mustMetricsConsumer(t, &called)
			case payloadTraces:
				r.nextTraces = mustTracesConsumer(t, &called)
			case payloadProfiles:
				r.nextProfiles = mustProfilesConsumer(t, &called)
			}

			msg := encodedOTLPMessage(t, tt.body, tt.contentType, tt.contentEncoding, tt.payloadCompressed)
			require.NoError(t, r.handleMessage(context.Background(), msg))
			require.True(t, called)
			require.True(t, msg.acked)
			require.Empty(t, msg.termReason)
		})
	}
}

func TestHandleMessageNaksRetryableConsumerErrors(t *testing.T) {
	msg := encodedOTLPMessage(t, mustMarshalProto(t, pmetricotlp.NewExportRequest()), "application/x-protobuf", "", false)
	r := &jetstreamReceiver{logger: zap.NewNop(), kind: payloadMetrics, cfg: &Config{Compression: sharedjetstream.CompressionNone}, nextMetrics: mustFailingMetricsConsumer(t)}

	err := r.handleMessage(context.Background(), msg)
	require.NoError(t, err)
	require.False(t, msg.acked)
	require.True(t, msg.nacked)
	require.Equal(t, r.cfg.ConsumeRetryDelay, msg.nakDelay)
	require.Empty(t, msg.termReason)
}

func TestHandleMessageTerminatesPermanentConsumerErrors(t *testing.T) {
	msg := encodedOTLPMessage(t, mustMarshalProto(t, pmetricotlp.NewExportRequest()), "application/x-protobuf", "", false)
	r := &jetstreamReceiver{logger: zap.NewNop(), kind: payloadMetrics, cfg: &Config{Compression: sharedjetstream.CompressionNone}, nextMetrics: mustPermanentFailingMetricsConsumer(t)}

	err := r.handleMessage(context.Background(), msg)
	require.Error(t, err)
	require.False(t, msg.acked)
	require.False(t, msg.nacked)
	require.Equal(t, reasonPermanentConsumerError, msg.termReason)
}

func mustPermanentFailingMetricsConsumer(t *testing.T) consumer.Metrics {
	t.Helper()
	c, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error {
		return consumererror.NewPermanent(errors.New("bad subject"))
	})
	require.NoError(t, err)
	return c
}

func mustFailingMetricsConsumer(t *testing.T) consumer.Metrics {
	t.Helper()
	c, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error {
		return errors.New("sending queue is full")
	})
	require.NoError(t, err)
	return c
}

func TestHandleMessageTerminatesUnsupportedPayloadKind(t *testing.T) {
	msg := encodedOTLPMessage(t, mustMarshalProto(t, pmetricotlp.NewExportRequest()), "application/x-protobuf", "", false)
	r := &jetstreamReceiver{logger: zap.NewNop(), kind: payloadKind(99), cfg: &Config{Compression: sharedjetstream.CompressionNone}, nextMetrics: mustMetricsConsumer(t, new(bool))}

	err := r.handleMessage(context.Background(), msg)
	require.Error(t, err)
	require.False(t, msg.acked)
	require.False(t, msg.nacked)
	require.Equal(t, reasonUnsupportedPayloadKind, msg.termReason)
}

func TestHandleMessageRejectsUnsupportedCompression(t *testing.T) {
	msg := encodedOTLPMessage(t, []byte("payload"), "application/json", "br", false)
	r := &jetstreamReceiver{logger: zap.NewNop(), kind: payloadLogs, cfg: &Config{Compression: sharedjetstream.CompressionNone}, nextLogs: mustLogsConsumer(t, new(bool))}

	err := r.handleMessage(context.Background(), msg)
	require.Error(t, err)
	require.False(t, msg.acked)
	require.Equal(t, reasonInvalidCompressedPayload, msg.termReason)
}

func encodedOTLPMessage(t *testing.T, body []byte, contentType string, contentEncoding string, payloadCompressed bool) *testMsg {
	t.Helper()
	headers := nats.Header{}
	if contentType != "" {
		headers.Set(sharedjetstream.HeaderContentType, contentType)
	}
	if contentEncoding != "" {
		headers.Set(sharedjetstream.HeaderContentEncoding, contentEncoding)
	}
	if payloadCompressed {
		body = gzipBody(t, body)
	}
	return &testMsg{data: body, headers: headers}
}

func mustMarshalProto(t *testing.T, req interface{ MarshalProto() ([]byte, error) }) []byte {
	t.Helper()
	data, err := req.MarshalProto()
	require.NoError(t, err)
	return data
}

func mustMarshalJSON(t *testing.T, req interface{ MarshalJSON() ([]byte, error) }) []byte {
	t.Helper()
	data, err := req.MarshalJSON()
	require.NoError(t, err)
	return data
}

func mustLogsConsumer(t *testing.T, called *bool) consumer.Logs {
	t.Helper()
	c, err := consumer.NewLogs(func(_ context.Context, _ plog.Logs) error {
		*called = true
		return nil
	})
	require.NoError(t, err)
	return c
}

func mustMetricsConsumer(t *testing.T, called *bool) consumer.Metrics {
	t.Helper()
	c, err := consumer.NewMetrics(func(_ context.Context, _ pmetric.Metrics) error {
		*called = true
		return nil
	})
	require.NoError(t, err)
	return c
}

func mustTracesConsumer(t *testing.T, called *bool) consumer.Traces {
	t.Helper()
	c, err := consumer.NewTraces(func(_ context.Context, _ ptrace.Traces) error {
		*called = true
		return nil
	})
	require.NoError(t, err)
	return c
}

func mustProfilesConsumer(t *testing.T, called *bool) xconsumer.Profiles {
	t.Helper()
	c, err := xconsumer.NewProfiles(func(_ context.Context, _ pprofile.Profiles) error {
		*called = true
		return nil
	})
	require.NoError(t, err)
	return c
}
