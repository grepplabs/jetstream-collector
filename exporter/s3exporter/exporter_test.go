package s3exporter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/grepplabs/jetstream-collector/exporter/s3exporter/internal/metadata"
	"github.com/grepplabs/jetstream-collector/pkg/s3event"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type uploadProbe struct {
	calls     []string
	payload   []byte
	opts      minio.PutObjectOptions
	putErr    error
	events    []s3event.CloudEvent
	notifyErr error
}

func (p *uploadProbe) PutObject(_ context.Context, _, _ string, body io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	p.calls = append(p.calls, "put")
	data, err := io.ReadAll(body)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	if int64(len(data)) != size {
		return minio.UploadInfo{}, errors.New("unexpected upload size")
	}
	p.payload = data
	p.opts = opts
	return minio.UploadInfo{}, p.putErr
}

func (p *uploadProbe) Notify(_ context.Context, event s3event.CloudEvent) error {
	p.calls = append(p.calls, "notify")
	p.events = append(p.events, event)
	return p.notifyErr
}

func newTestExporter(t *testing.T, cfg *Config, probe *uploadProbe) *s3Exporter {
	t.Helper()
	mp := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	set := exportertest.NewNopSettings(metadata.Type)
	set.ID = component.MustNewIDWithName(metadata.Type.String(), "notification-test")
	set.MeterProvider = mp

	exp, err := newExporter(set, cfg)
	require.NoError(t, err)
	exp.client = probe
	exp.notifier = probe
	return exp
}

func TestUpload(t *testing.T) {
	const (
		bucket = "telemetry"
		key    = "logs/batch.log.gz"
	)
	payload := []byte("stored object bytes")
	digest := sha256.Sum256(payload)

	errPut := errors.New("put failed")
	errNotify := errors.New("notify failed")

	tests := []struct {
		name         string
		compression  string
		wantEncoding string
		putErr       error
		notifyErr    error
		wantNotify   bool
		wantErr      error
	}{
		{
			name:         "gzip",
			compression:  compressionGzip,
			wantEncoding: contentEncodingGzip,
			wantNotify:   true,
		},
		{
			name:         "no compression",
			compression:  compressionNone,
			wantEncoding: "",
			wantNotify:   true,
		},
		{
			name:         "put failure does not notify",
			compression:  compressionGzip,
			wantEncoding: contentEncodingGzip,
			putErr:       errPut,
			wantErr:      errPut,
		},
		{
			name:         "notify failure is returned",
			compression:  compressionGzip,
			wantEncoding: contentEncodingGzip,
			notifyErr:    errNotify,
			wantNotify:   true,
			wantErr:      errNotify,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			cfg := NewDefaultConfig()
			cfg.Bucket = bucket
			cfg.Compression = tt.compression

			probe := &uploadProbe{putErr: tt.putErr, notifyErr: tt.notifyErr}
			exp := newTestExporter(t, cfg, probe)

			before := time.Now()
			err := exp.upload(ctx, signalLogs, key, payload)
			after := time.Now()
			wantCalls := []string{"put"}
			if tt.wantNotify {
				wantCalls = append(wantCalls, "notify")
			}
			require.Equal(t, wantCalls, probe.calls)
			require.Equal(t, payload, probe.payload)
			require.Equal(t, mimeContentTypeProto, probe.opts.ContentType)
			require.Equal(t, tt.wantEncoding, probe.opts.ContentEncoding)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			if !tt.wantNotify {
				return
			}

			gotEvent := probe.events[0]
			uploadedAt := gotEvent.Data.UploadedAt
			require.False(t, uploadedAt.Before(before), "UploadedAt too early")
			require.False(t, uploadedAt.After(after), "UploadedAt too late")
			require.Regexp(t, `^[0-9a-f]{64}$`, gotEvent.ID)

			require.Equal(t, s3event.CloudEvent{
				SpecVersion:     s3event.CloudEventsVersion,
				Type:            s3event.ObjectCreatedType,
				Source:          "urn:opentelemetry:exporter:" + exp.id.String(),
				ID:              gotEvent.ID,
				Time:            uploadedAt,
				DataContentType: "application/json",
				DataSchema:      s3event.ObjectSchema,
				Data: s3event.ObjectReference{
					Bucket:          bucket,
					Key:             key,
					Signal:          signalLogs,
					API:             "PutObject",
					ContentType:     mimeContentTypeProto,
					ContentEncoding: tt.wantEncoding,
					Size:            int64(len(payload)),
					SHA256:          hex.EncodeToString(digest[:]),
					UploadedAt:      uploadedAt,
				},
			}, gotEvent)
		})
	}
}

func TestUploadCloudEventIDIsStableForSameObject(t *testing.T) {
	const (
		bucket = "telemetry"
		key    = "logs/batch.log"
	)
	ctx := context.Background()
	payload := []byte("stored object bytes")
	cfg := NewDefaultConfig()
	cfg.Bucket = bucket
	probe := &uploadProbe{}
	exp := newTestExporter(t, cfg, probe)

	require.NoError(t, exp.upload(ctx, signalLogs, key, payload))
	require.NoError(t, exp.upload(ctx, signalLogs, key, payload))
	require.Equal(t, []string{"put", "notify", "put", "notify"}, probe.calls)
	require.Len(t, probe.events, 2)
	require.NotEmpty(t, probe.events[0].ID)
	require.Equal(t, probe.events[0].ID, probe.events[1].ID)
}
