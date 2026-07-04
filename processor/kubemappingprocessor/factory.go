package kubemappingprocessor

import (
	"context"

	"github.com/grepplabs/jetstream-collector/processor/kubemappingprocessor/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(metadata.Type, func() component.Config { return NewDefaultConfig() },
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		processor.WithTraces(createTracesProcessor, metadata.TracesStability))
}

func createBase(settings processor.Settings, cfg component.Config) (*kubeMappingProcessor, error) {
	config := cfg.(*Config)
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if len(config.Mappings) == 0 {
		return &kubeMappingProcessor{cfg: config, logger: settings.Logger}, nil
	}
	lookup, err := newDynamicResourceLookup(config.KubeConfig, config.Logger, config.Mappings)
	if err != nil {
		return nil, err
	}
	return &kubeMappingProcessor{cfg: config, lookup: lookup, mapper: newMapper(lookup, config.Cache), logger: settings.Logger}, nil
}
func createLogsProcessor(_ context.Context, s processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
	p, err := createBase(s, cfg)
	if err != nil {
		return nil, err
	}
	return &logsProcessor{kubeMappingProcessor: p, next: next}, nil
}
func createMetricsProcessor(_ context.Context, s processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
	p, err := createBase(s, cfg)
	if err != nil {
		return nil, err
	}
	return &metricsProcessor{kubeMappingProcessor: p, next: next}, nil
}
func createTracesProcessor(_ context.Context, s processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	p, err := createBase(s, cfg)
	if err != nil {
		return nil, err
	}
	return &tracesProcessor{kubeMappingProcessor: p, next: next}, nil
}
