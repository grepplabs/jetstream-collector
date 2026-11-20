// Package s3jetstreameventextension publishes S3 object CloudEvents to JetStream.
package s3jetstreameventextension

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/grepplabs/jetstream-collector/pkg/s3event"
	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"
)

type eventExtension struct {
	logger *zap.Logger
	cfg    *Config

	mu      sync.RWMutex
	js      publisher
	started bool
}

type publisher interface {
	Conn() *nats.Conn
	PublishMsg(context.Context, *nats.Msg, ...natsjetstream.PublishOpt) (*natsjetstream.PubAck, error)
}

const cloudEventsContentType = "application/cloudevents+json"

var _ extension.Extension = (*eventExtension)(nil)
var _ s3event.Notifier = (*eventExtension)(nil)

func NewFactory() extension.Factory {
	typ := component.MustNewType("jetstreams3event")
	return extension.NewFactory(typ,
		func() component.Config { return NewDefaultConfig() },
		func(_ context.Context, set extension.Settings, baseCfg component.Config) (extension.Extension, error) {
			logger := set.Logger
			if logger == nil {
				logger = zap.NewNop()
			}
			cfg, ok := baseCfg.(*Config)
			if !ok {
				return nil, fmt.Errorf("config structure is not of type *s3jetstreameventextension.Config")
			}
			return &eventExtension{logger: logger, cfg: cfg}, nil
		},
		component.StabilityLevelDevelopment,
	)
}

func (e *eventExtension) Start(ctx context.Context, _ component.Host) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return nil
	}
	js, err := sharedjetstream.Connect(e.cfg.URL, e.cfg.TLS, e.cfg.Auth, e.cfg.Connection, e.logger)
	if err != nil {
		return fmt.Errorf("connect JetStream: %w", err)
	}
	if err := sharedjetstream.EnsureBootstrapStreams(ctx, js, e.cfg.Bootstrap); err != nil {
		if conn := js.Conn(); conn != nil {
			conn.Close()
		}
		return fmt.Errorf("bootstrap JetStream stream: %w", err)
	}
	e.js = js
	e.started = true
	e.logger.Info("S3 event JetStream target started",
		zap.String("url", e.cfg.URL),
		zap.String("subject", e.cfg.Subject),
	)
	return nil
}

func (e *eventExtension) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.js != nil && e.js.Conn() != nil {
		_ = e.js.Conn().Drain()
		e.js.Conn().Close()
	}
	e.js = nil
	e.started = false
	return nil
}

func (e *eventExtension) Notify(ctx context.Context, event s3event.CloudEvent) error {
	e.mu.RLock()
	js := e.js
	e.mu.RUnlock()
	if js == nil {
		return fmt.Errorf("S3 event JetStream target is not started.")
	}
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal CloudEvent: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()
	msg := nats.NewMsg(e.cfg.Subject)
	msg.Data = data
	for key, value := range e.cfg.Headers {
		msg.Header.Set(key, value)
	}
	msg.Header.Set(sharedjetstream.HeaderContentType, cloudEventsContentType)
	if e.cfg.MsgID {
		msg.Header.Set(nats.MsgIdHdr, event.ID)
	}
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("publish CloudEvent to JetStream subject %q: %w", e.cfg.Subject, err)
	}
	e.logger.Debug("published S3 object CloudEvent",
		zap.String("event_id", event.ID),
		zap.String("subject", e.cfg.Subject),
		zap.String("bucket", event.Data.Bucket),
		zap.String("key", event.Data.Key),
	)
	return nil
}
