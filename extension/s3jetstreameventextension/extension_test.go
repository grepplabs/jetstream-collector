package s3jetstreameventextension

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/grepplabs/jetstream-collector/pkg/s3event"
	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type capturePublisher struct {
	msg *nats.Msg
	err error
}

func (p *capturePublisher) Conn() *nats.Conn { return nil }

func (p *capturePublisher) PublishMsg(_ context.Context, msg *nats.Msg, _ ...natsjetstream.PublishOpt) (*natsjetstream.PubAck, error) {
	p.msg = msg
	return nil, p.err
}

func TestNotifyPublishesConfiguredContentTypeAndEvent(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.Subject = "events.s3"
	cfg.CloudEventsMediaType = "application/vnd.s3-object-reference+json"
	cfg.Headers = map[string]string{"X-Test": "value", sharedjetstream.HeaderContentType: "text/plain"}
	publisher := &capturePublisher{}
	ext := &eventExtension{logger: zap.NewNop(), cfg: cfg, js: publisher}
	event := s3event.CloudEvent{SpecVersion: "1.0", ID: "event-1", Type: s3event.ObjectCreatedType}

	require.NoError(t, ext.Notify(context.Background(), event))
	require.Equal(t, cfg.Subject, publisher.msg.Subject)
	require.Equal(t, cfg.CloudEventsMediaType, publisher.msg.Header.Get(sharedjetstream.HeaderContentType))
	require.Equal(t, "value", publisher.msg.Header.Get("X-Test"))
	require.Equal(t, event.ID, publisher.msg.Header.Get(nats.MsgIdHdr))
	var got s3event.CloudEvent
	require.NoError(t, json.Unmarshal(publisher.msg.Data, &got))
	require.Equal(t, event, got)
}

func TestNotifyReturnsPublishError(t *testing.T) {
	wantErr := errors.New("publish failed")
	publisher := &capturePublisher{err: wantErr}
	ext := &eventExtension{logger: zap.NewNop(), cfg: NewDefaultConfig(), js: publisher}

	err := ext.Notify(context.Background(), s3event.CloudEvent{ID: "event-1"})
	require.ErrorIs(t, err, wantErr)
}

func TestNotifyRequiresStartedPublisher(t *testing.T) {
	ext := &eventExtension{cfg: NewDefaultConfig()}
	require.ErrorContains(t, ext.Notify(context.Background(), s3event.CloudEvent{}), "not started")
}

func TestConfigCloudEventsMediaType(t *testing.T) {
	cfg := NewDefaultConfig()
	require.NoError(t, cfg.Validate())
	require.Equal(t, s3event.CloudEventsMediaType, cfg.CloudEventsMediaType)

	cfg.CloudEventsMediaType = "Application/Vnd.S3-Object-Reference+Json; charset=utf-8"
	require.NoError(t, cfg.Validate())
	require.Equal(t, "application/vnd.s3-object-reference+json", cfg.CloudEventsMediaType)

	cfg.CloudEventsMediaType = "not a media type"
	require.ErrorContains(t, cfg.Validate(), "cloud_events_media_type")
}
