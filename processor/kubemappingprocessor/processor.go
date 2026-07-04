package kubemappingprocessor

import (
	"context"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type kubeMappingProcessor struct {
	cfg    *Config
	lookup ResourceLookup
	mapper mapper
	logger *zap.Logger
}

func (p *kubeMappingProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
func (p *kubeMappingProcessor) Start(ctx context.Context, _ component.Host) error {
	if p.lookup == nil {
		return nil
	}
	return p.lookup.Start(ctx)
}

func (p *kubeMappingProcessor) Shutdown(ctx context.Context) error {
	if p.lookup == nil {
		return nil
	}
	return p.lookup.Shutdown(ctx)
}

func (p *kubeMappingProcessor) mapContext(ctx context.Context) (context.Context, error) {
	info := client.FromContext(ctx)
	values := make(map[string][]string, len(p.cfg.Mappings))
	for key := range info.Metadata.Keys() {
		values[key] = info.Metadata.Get(key)
	}
	for i, mapping := range p.cfg.Mappings {
		md := client.NewMetadata(values)
		result, err := p.mapper.Map(ctx, mapping, md)
		if err != nil {
			p.logger.Warn("Kubernetes metadata mapping failed", zap.Int("mapping_index", i), zap.String("group", mapping.Resource.Group), zap.String("version", mapping.Resource.Version), zap.String("kind", mapping.Resource.Kind), zap.String("mapping_result", result.String()), zap.Error(err))
			if p.cfg.ErrorMode != ErrorModeIgnore {
				return ctx, err
			}
			continue
		}
		if !result.found {
			p.logger.Debug("Kubernetes metadata mapping missed", zap.Int("mapping_index", i), zap.String("group", mapping.Resource.Group), zap.String("version", mapping.Resource.Version), zap.String("kind", mapping.Resource.Kind), zap.String("mapping_result", result.String()))
			continue
		}
		values[mapping.Target] = []string{result.value}
	}
	info.Metadata = client.NewMetadata(values)
	return client.NewContext(ctx, info), nil
}

type logsProcessor struct {
	*kubeMappingProcessor
	next consumer.Logs
}

func (p *logsProcessor) ConsumeLogs(ctx context.Context, data plog.Logs) error {
	mapped, err := p.mapContext(ctx)
	if err != nil {
		return err
	}
	return p.next.ConsumeLogs(mapped, data)
}

type metricsProcessor struct {
	*kubeMappingProcessor
	next consumer.Metrics
}

func (p *metricsProcessor) ConsumeMetrics(ctx context.Context, data pmetric.Metrics) error {
	mapped, err := p.mapContext(ctx)
	if err != nil {
		return err
	}
	return p.next.ConsumeMetrics(mapped, data)
}

type tracesProcessor struct {
	*kubeMappingProcessor
	next consumer.Traces
}

func (p *tracesProcessor) ConsumeTraces(ctx context.Context, data ptrace.Traces) error {
	mapped, err := p.mapContext(ctx)
	if err != nil {
		return err
	}
	return p.next.ConsumeTraces(mapped, data)
}
