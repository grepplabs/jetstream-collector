package jetstreamreceiver

import (
	"context"
	"errors"
	"fmt"
	"sync"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/xreceiver"
	"go.uber.org/zap"
)

type payloadKind int

const (
	payloadLogs payloadKind = iota
	payloadMetrics
	payloadTraces
	payloadProfiles
)

const (
	reasonInvalidCompressedPayload = "invalid compressed payload"
	reasonUnsupportedContentType   = "unsupported content-type"
	reasonInvalidLogsPayload       = "invalid otlp logs payload"
	reasonInvalidMetricsPayload    = "invalid otlp metrics payload"
	reasonInvalidTracesPayload     = "invalid otlp traces payload"
	reasonInvalidProfilesPayload   = "invalid otlp profiles payload"
	reasonUnsupportedPayloadKind   = "unsupported payload kind"
	reasonPermanentConsumerError   = "permanent consumer error"
)

var _ receiver.Logs = (*jetstreamReceiver)(nil)
var _ receiver.Metrics = (*jetstreamReceiver)(nil)
var _ receiver.Traces = (*jetstreamReceiver)(nil)
var _ xreceiver.Profiles = (*jetstreamReceiver)(nil)

type jetstreamReceiver struct {
	logger  *zap.Logger
	cfg     *Config
	kind    payloadKind
	metrics *jetstreamMetrics

	nextLogs     consumer.Logs
	nextMetrics  consumer.Metrics
	nextTraces   consumer.Traces
	nextProfiles xconsumer.Profiles

	mu           sync.Mutex
	js           jetstream.JetStream
	subscription jetstream.ConsumeContext
	cancel       context.CancelFunc
	jobs         chan jetstream.Msg
	workersWG    sync.WaitGroup
	started      bool
}

func newLogsReceiver(set receiver.Settings, cfg *Config, next consumer.Logs) (receiver.Logs, error) {
	return newReceiver(set, cfg, payloadLogs, next, nil, nil, nil)
}

func newMetricsReceiver(set receiver.Settings, cfg *Config, next consumer.Metrics) (receiver.Metrics, error) {
	return newReceiver(set, cfg, payloadMetrics, nil, next, nil, nil)
}

func newTracesReceiver(set receiver.Settings, cfg *Config, next consumer.Traces) (receiver.Traces, error) {
	return newReceiver(set, cfg, payloadTraces, nil, nil, next, nil)
}

func newProfilesReceiver(set receiver.Settings, cfg *Config, next xconsumer.Profiles) (xreceiver.Profiles, error) {
	return newReceiver(set, cfg, payloadProfiles, nil, nil, nil, next)
}

func newReceiver(set receiver.Settings, cfg *Config, kind payloadKind, logs consumer.Logs, metrics consumer.Metrics, traces consumer.Traces, profiles xconsumer.Profiles) (*jetstreamReceiver, error) {
	if logs == nil && metrics == nil && traces == nil && profiles == nil {
		return nil, fmt.Errorf("next consumer is required")
	}
	logger := set.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	consumerName, err := cfg.consumerName()
	if err != nil {
		return nil, err
	}
	buckets := cfg.metricsBuckets()
	jm, err := newJetstreamMetrics(set.MeterProvider, set.ID.String(), consumerName, cfg.IncludeSubject, buckets)
	if err != nil {
		return nil, err
	}
	return &jetstreamReceiver{
		logger:       logger,
		cfg:          cfg,
		kind:         kind,
		metrics:      jm,
		nextLogs:     logs,
		nextMetrics:  metrics,
		nextTraces:   traces,
		nextProfiles: profiles,
	}, nil
}

func (r *jetstreamReceiver) Start(ctx context.Context, _ component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return nil
	}

	js, err := r.connect()
	if err != nil {
		r.metrics.recordStartupFailure(ctx, stageConnect)
		return err
	}

	if err := sharedjetstream.EnsureBootstrapStreams(ctx, js, r.cfg.Bootstrap); err != nil {
		if conn := js.Conn(); conn != nil {
			conn.Close()
		}
		r.metrics.recordStartupFailure(ctx, stageBootstrap)
		return err
	}

	stream, err := js.Stream(ctx, r.cfg.Stream)
	if err != nil {
		if conn := js.Conn(); conn != nil {
			conn.Close()
		}
		r.metrics.recordStartupFailure(ctx, stageLookupStream)
		return fmt.Errorf("lookup stream %q: %w", r.cfg.Stream, err)
	}

	if err := sharedjetstream.EnsureBootstrapConsumers(ctx, stream, r.cfg.Bootstrap); err != nil {
		if conn := js.Conn(); conn != nil {
			conn.Close()
		}
		r.metrics.recordStartupFailure(ctx, stageBootstrapConsumers)
		return err
	}

	consumerName, err := r.cfg.consumerName()
	if err != nil {
		if conn := js.Conn(); conn != nil {
			conn.Close()
		}
		r.metrics.recordStartupFailure(ctx, stageConsumerName)
		return err
	}

	pullConsumer, err := stream.Consumer(ctx, consumerName)
	if err != nil {
		if conn := js.Conn(); conn != nil {
			conn.Close()
		}
		r.metrics.recordStartupFailure(ctx, stageLookupConsumer)
		return fmt.Errorf("lookup consumer %q: %w", consumerName, err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.js = js

	switch r.cfg.processingMode() {
	case processingModeBatch:
		r.startBatchConsumer(runCtx, pullConsumer)
	case processingModeSingle:
		sub, err := r.startSingleConsumer(runCtx, pullConsumer)
		if err != nil {
			cancel()
			if conn := js.Conn(); conn != nil {
				conn.Close()
			}
			r.metrics.recordStartupFailure(ctx, stageStartConsumer)
			return err
		}
		r.subscription = sub
	default:
		cancel()
		if conn := js.Conn(); conn != nil {
			conn.Close()
		}
		return fmt.Errorf("unsupported processing mode %q", r.cfg.ProcessingMode)
	}
	r.started = true
	r.metrics.recordStartupSuccess(ctx)
	r.logger.Info("jetstream receiver started",
		zap.String("stream", r.cfg.Stream),
		zap.String("subject", r.cfg.Subject),
		zap.String("url", r.cfg.URL),
	)
	return nil
}

func (r *jetstreamReceiver) startWorkers(ctx context.Context) {
	if r.cfg.Workers <= 0 {
		return
	}

	r.jobs = make(chan jetstream.Msg, r.parallelism())
	for i := 0; i < r.cfg.Workers; i++ {
		r.workersWG.Add(1)
		go r.workerLoop(ctx)
	}
	go r.watchWorkers(ctx)
}

func (r *jetstreamReceiver) startSingleConsumer(ctx context.Context, consumer jetstream.Consumer) (jetstream.ConsumeContext, error) {
	r.startWorkers(ctx)

	sub, err := consumer.Consume(func(msg jetstream.Msg) {
		if err := r.consumeMessage(ctx, msg); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error("failed to handle jetstream message", zap.Error(err))
		}
	}, jetstream.PullMaxMessages(r.parallelism()), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error("jetstream consumer error", zap.Error(err))
		}
	}))
	if err != nil {
		r.stopWorkers()
		return nil, fmt.Errorf("start jetstream consumer: %w", err)
	}
	return sub, nil
}

func (r *jetstreamReceiver) startBatchConsumer(ctx context.Context, consumer jetstream.Consumer) {
	workers := r.parallelism()
	r.workersWG.Add(workers)
	for range workers {
		go r.batchWorkerLoop(ctx, consumer)
	}
	go r.watchWorkers(ctx)
}

func (r *jetstreamReceiver) watchWorkers(ctx context.Context) {
	r.workersWG.Wait()
	if ctx.Err() != nil {
		return
	}
	if r.cancel != nil {
		r.logger.Info("all batch workers stopped; stopping receiver")
		r.cancel()
	}
}

func (r *jetstreamReceiver) stopWorkers() {
	if r.jobs != nil {
		close(r.jobs)
		r.jobs = nil
	}
	r.workersWG.Wait()
}

func (r *jetstreamReceiver) consumeMessage(ctx context.Context, msg jetstream.Msg) error {
	if r.jobs != nil {
		select {
		case r.jobs <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.handleMessage(ctx, msg)
}

func (r *jetstreamReceiver) Shutdown(_ context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		return nil
	}
	if r.cancel != nil {
		r.cancel()
	}
	if r.subscription != nil {
		r.subscription.Drain()
		<-r.subscription.Closed()
		r.subscription = nil
	}
	r.stopWorkers()
	if r.js != nil && r.js.Conn() != nil {
		r.js.Conn().Close()
	}
	r.started = false
	return nil
}

func (r *jetstreamReceiver) connect() (jetstream.JetStream, error) {
	return sharedjetstream.Connect(r.cfg.URL, r.cfg.TLS, r.cfg.Auth, r.logger)
}

func (r *jetstreamReceiver) parallelism() int {
	if r.cfg.Workers > 0 {
		return r.cfg.Workers
	}
	return 1
}
