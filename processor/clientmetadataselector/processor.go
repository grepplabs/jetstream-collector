package clientmetadataselector

import (
	"context"
	"math/rand/v2"
	"sync/atomic"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type selector struct {
	cfg   *Config
	index atomic.Uint64
}

func newSelector(cfg *Config) *selector {
	return &selector{cfg: cfg}
}

func (*selector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (*selector) Start(context.Context, component.Host) error { return nil }
func (*selector) Shutdown(context.Context) error              { return nil }

func (p *selector) addMetadata(ctx context.Context) context.Context {
	value := p.cfg.Values[0]
	if len(p.cfg.Values) > 1 {
		if p.cfg.Strategy == strategyRandom {
			value = p.cfg.Values[rand.IntN(len(p.cfg.Values))]
		} else {
			i := p.index.Add(1) - 1
			value = p.cfg.Values[i%uint64(len(p.cfg.Values))]
		}
	}

	info := client.FromContext(ctx)
	metadata := make(map[string][]string)
	for key := range info.Metadata.Keys() {
		metadata[key] = info.Metadata.Get(key)
	}
	metadata[p.cfg.MetadataKey] = []string{value}
	info.Metadata = client.NewMetadata(metadata)
	return client.NewContext(ctx, info)
}

type logsProcessor struct {
	*selector
	next consumer.Logs
}

func newLogsProcessor(cfg *Config, next consumer.Logs) *logsProcessor {
	return &logsProcessor{selector: newSelector(cfg), next: next}
}

func (p *logsProcessor) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	return p.next.ConsumeLogs(p.addMetadata(ctx), logs)
}

type metricsProcessor struct {
	*selector
	next consumer.Metrics
}

func (p *metricsProcessor) ConsumeMetrics(ctx context.Context, data pmetric.Metrics) error {
	return p.next.ConsumeMetrics(p.addMetadata(ctx), data)
}

type tracesProcessor struct {
	*selector
	next consumer.Traces
}

func (p *tracesProcessor) ConsumeTraces(ctx context.Context, data ptrace.Traces) error {
	return p.next.ConsumeTraces(p.addMetadata(ctx), data)
}

type profilesProcessor struct {
	*selector
	next xconsumer.Profiles
}

func (p *profilesProcessor) ConsumeProfiles(ctx context.Context, data pprofile.Profiles) error {
	return p.next.ConsumeProfiles(p.addMetadata(ctx), data)
}
