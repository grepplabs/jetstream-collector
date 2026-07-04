package jetstreamexporter

import (
	"context"
	"testing"
	"time"

	client "go.opentelemetry.io/collector/client"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type fakePublisher struct {
	msg  *nats.Msg
	msgs []*nats.Msg
}

func (f *fakePublisher) Conn() *nats.Conn {
	return nil
}

func (f *fakePublisher) PublishMsg(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.msg = msg
	f.msgs = append(f.msgs, msg)
	return nil, nil
}

func TestNormalizeCompression(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "", want: sharedjetstream.CompressionNone},
		{in: "none", want: sharedjetstream.CompressionNone},
		{in: "identity", want: sharedjetstream.CompressionNone},
		{in: "gzip", want: sharedjetstream.CompressionGzip},
	}

	for _, tt := range tests {
		got, err := sharedjetstream.NormalizeCompression(tt.in)
		require.NoError(t, err)
		require.Equal(t, tt.want, got)
	}
}

func TestNormalizeContentType(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "", want: sharedjetstream.ContentTypeProto},
		{in: "proto", want: sharedjetstream.ContentTypeProto},
		{in: "application/x-protobuf", want: sharedjetstream.ContentTypeProto},
		{in: "json", want: sharedjetstream.ContentTypeJSON},
		{in: "application/json", want: sharedjetstream.ContentTypeJSON},
	}

	for _, tt := range tests {
		got, err := sharedjetstream.NormalizeContentType(tt.in)
		require.NoError(t, err)
		require.Equal(t, tt.want, got)
	}
}

func TestDefaultConfigHasHelperDefaults(t *testing.T) {
	cfg := NewDefaultConfig()
	require.Equal(t, sharedjetstream.CompressionNone, cfg.Compression)
	require.Equal(t, sharedjetstream.ContentTypeProto, cfg.ContentType)
	require.Equal(t, 5*time.Second, cfg.TimeoutSettings.Timeout)
	require.False(t, cfg.RetryOnFailure.Enabled)
	require.Equal(t, 1*time.Second, cfg.RetryOnFailure.InitialInterval)
	require.Equal(t, 0.2, cfg.RetryOnFailure.RandomizationFactor)
	require.Equal(t, 1.5, cfg.RetryOnFailure.Multiplier)
	require.Equal(t, 5*time.Second, cfg.RetryOnFailure.MaxInterval)
	require.Equal(t, 10*time.Second, cfg.RetryOnFailure.MaxElapsedTime)
}

func TestValidateNormalizesConfig(t *testing.T) {
	cfg := &Config{URL: sharedjetstream.DefaultURL, Subject: "otel.logs", Compression: "identity", ContentType: "application/json", Headers: map[string]string{"Content-Type": "text/plain", "X-Test": "1"}}
	require.NoError(t, cfg.Validate())
	require.Equal(t, sharedjetstream.CompressionNone, cfg.Compression)
	require.Equal(t, sharedjetstream.ContentTypeJSON, cfg.ContentType)
	require.Equal(t, map[string]string{"Content-Type": "text/plain", "X-Test": "1"}, cfg.Headers)
}

func TestValidateAllowsSubjectPatternOnly(t *testing.T) {
	cfg := &Config{URL: sharedjetstream.DefaultURL, SubjectPattern: "otel.${header:X-Tenant}"}
	require.NoError(t, cfg.Validate())
}

func TestConsumeLogsPublishesProto(t *testing.T) {
	pub := &fakePublisher{}
	exp := &jetstreamExporter{
		logger: zap.NewNop(),
		cfg: &Config{
			Subject:     "otel.logs",
			Compression: sharedjetstream.CompressionNone,
			ContentType: sharedjetstream.ContentTypeProto,
			Headers: map[string]string{
				sharedjetstream.HeaderContentType: "text/plain",
				"X-Test":                          "1",
			},
		},
		js: pub,
	}

	require.NoError(t, exp.ConsumeLogs(context.Background(), plog.NewLogs()))
	require.NotNil(t, pub.msg)
	require.Equal(t, "otel.logs", pub.msg.Subject)
	require.Equal(t, sharedjetstream.MIMEContentTypeProto, pub.msg.Header.Get(sharedjetstream.HeaderContentType))
	require.Equal(t, "1", pub.msg.Header.Get("X-Test"))
	require.Empty(t, pub.msg.Header.Get(sharedjetstream.HeaderContentEncoding))
	require.Empty(t, pub.msg.Header.Get(nats.MsgIdHdr))
}

func TestConsumeLogsResolvesHeaderSubjectPattern(t *testing.T) {
	pub := &fakePublisher{}
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"X-Tenant": {"acme"},
		}),
	})
	exp := &jetstreamExporter{
		logger: zap.NewNop(),
		cfg: &Config{
			SubjectPattern: "otel.${header:X-Tenant}",
			Compression:    sharedjetstream.CompressionNone,
			ContentType:    sharedjetstream.ContentTypeProto,
		},
		subjectPattern: mustExporterSubjectTemplate(t, "otel.${header:X-Tenant}"),
		js:             pub,
	}

	require.NoError(t, exp.ConsumeLogs(ctx, plog.NewLogs()))
	require.NotNil(t, pub.msg)
	require.Equal(t, "otel.acme", pub.msg.Subject)
}

func TestConsumeLogsPublishesMsgIDWhenEnabled(t *testing.T) {
	pub := &fakePublisher{}
	exp := &jetstreamExporter{
		logger: zap.NewNop(),
		cfg: &Config{
			Subject:     "otel.logs",
			Compression: sharedjetstream.CompressionNone,
			ContentType: sharedjetstream.ContentTypeProto,
			MsgID:       true,
		},
		js: pub,
	}

	in := plog.NewLogs()
	_ = in.ResourceLogs().AppendEmpty()
	require.NoError(t, exp.ConsumeLogs(context.Background(), in))
	require.NotNil(t, pub.msg)
	payload, err := plogotlp.NewExportRequestFromLogs(in).MarshalProto()
	require.NoError(t, err)
	require.Equal(t, msgIDHeaderValue("otel.logs", payload), pub.msg.Header.Get(nats.MsgIdHdr))
}

func TestConsumeMetricsResolvesHeaderSubjectPattern(t *testing.T) {
	pub := &fakePublisher{}
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"X-Tenant": {"acme"},
		}),
	})
	exp := &jetstreamExporter{
		logger: zap.NewNop(),
		cfg: &Config{
			SubjectPattern: "otel.${header:X-Tenant}",
			Compression:    sharedjetstream.CompressionNone,
			ContentType:    sharedjetstream.ContentTypeProto,
		},
		subjectPattern: mustExporterSubjectTemplate(t, "otel.${header:X-Tenant}"),
		js:             pub,
	}

	require.NoError(t, exp.ConsumeMetrics(ctx, pmetric.NewMetrics()))
	require.NotNil(t, pub.msg)
	require.Equal(t, "otel.acme", pub.msg.Subject)
}

func TestConsumeMetricsPublishesJSONGzip(t *testing.T) {
	pub := &fakePublisher{}
	exp := &jetstreamExporter{
		logger: zap.NewNop(),
		cfg: &Config{
			Subject:     "otel.metrics",
			Compression: sharedjetstream.CompressionGzip,
			ContentType: sharedjetstream.ContentTypeJSON,
		},
		js: pub,
	}

	require.NoError(t, exp.ConsumeMetrics(context.Background(), pmetric.NewMetrics()))
	require.NotNil(t, pub.msg)
	require.Equal(t, sharedjetstream.MIMEContentTypeJSON, pub.msg.Header.Get(sharedjetstream.HeaderContentType))
	require.Equal(t, sharedjetstream.CompressionGzip, pub.msg.Header.Get(sharedjetstream.HeaderContentEncoding))
}

func TestConsumeTracesResolvesHeaderSubjectPattern(t *testing.T) {
	pub := &fakePublisher{}
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"X-Tenant": {"acme"},
		}),
	})
	exp := &jetstreamExporter{
		logger: zap.NewNop(),
		cfg: &Config{
			SubjectPattern: "otel.${header:X-Tenant}",
			Compression:    sharedjetstream.CompressionNone,
			ContentType:    sharedjetstream.ContentTypeProto,
		},
		subjectPattern: mustExporterSubjectTemplate(t, "otel.${header:X-Tenant}"),
		js:             pub,
	}

	require.NoError(t, exp.ConsumeTraces(ctx, ptrace.NewTraces()))
	require.NotNil(t, pub.msg)
	require.Equal(t, "otel.acme", pub.msg.Subject)
}

func TestConsumeTracesPublishes(t *testing.T) {
	pub := &fakePublisher{}
	exp := &jetstreamExporter{
		logger: zap.NewNop(),
		cfg: &Config{
			Subject:     "otel.traces",
			Compression: sharedjetstream.CompressionNone,
			ContentType: sharedjetstream.ContentTypeProto,
		},
		js: pub,
	}

	require.NoError(t, exp.ConsumeTraces(context.Background(), ptrace.NewTraces()))
	require.NotNil(t, pub.msg)
}

func TestConsumeProfilesResolvesHeaderSubjectPattern(t *testing.T) {
	pub := &fakePublisher{}
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"X-Tenant": {"acme"},
		}),
	})
	exp := &jetstreamExporter{
		logger: zap.NewNop(),
		cfg: &Config{
			SubjectPattern: "otel.${header:X-Tenant}",
			Compression:    sharedjetstream.CompressionNone,
			ContentType:    sharedjetstream.ContentTypeProto,
		},
		subjectPattern: mustExporterSubjectTemplate(t, "otel.${header:X-Tenant}"),
		js:             pub,
	}

	require.NoError(t, exp.ConsumeProfiles(ctx, pprofile.NewProfiles()))
	require.NotNil(t, pub.msg)
	require.Equal(t, "otel.acme", pub.msg.Subject)
}

func TestConsumeProfilesPublishes(t *testing.T) {
	pub := &fakePublisher{}
	exp := &jetstreamExporter{
		logger: zap.NewNop(),
		cfg: &Config{
			Subject:     "otel.profiles",
			Compression: sharedjetstream.CompressionNone,
			ContentType: sharedjetstream.ContentTypeProto,
		},
		js: pub,
	}

	require.NoError(t, exp.ConsumeProfiles(context.Background(), pprofile.NewProfiles()))
	require.NotNil(t, pub.msg)
}

func TestClassifyPublishError(t *testing.T) {
	t.Run("permanent NATS error", func(t *testing.T) {
		err := classifyPublishError("otel.logs", jetstream.ErrNoStreamResponse)
		require.Error(t, err)
		require.True(t, consumererror.IsPermanent(err))
		require.ErrorContains(t, err, "publish to subject \"otel.logs\"")
		require.ErrorIs(t, err, jetstream.ErrNoStreamResponse)
	})

	t.Run("retryable error stays retryable", func(t *testing.T) {
		err := classifyPublishError("otel.logs", nats.ErrTimeout)
		require.Error(t, err)
		require.False(t, consumererror.IsPermanent(err))
		require.ErrorIs(t, err, nats.ErrTimeout)
	})
}

func TestFactoryCreatesDefaultConfig(t *testing.T) {
	f := NewFactory()
	require.NotNil(t, f)
	require.IsType(t, &Config{}, f.CreateDefaultConfig())
	_, ok := f.(exporter.Factory)
	require.True(t, ok)
}

func mustExporterSubjectTemplate(t *testing.T, pattern string) *subjectTemplate {
	t.Helper()
	tpl, err := newSubjectTemplate(pattern)
	require.NoError(t, err)
	require.NotNil(t, tpl)
	return tpl
}
