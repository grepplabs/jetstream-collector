package celpartitionprocessor

import (
	"context"

	"github.com/grepplabs/jetstream-collector/processor/celpartitionprocessor/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(metadata.Type, func() component.Config { return NewDefaultConfig() },
		processor.WithLogs(createLogs, metadata.LogsStability),
		processor.WithMetrics(createMetrics, metadata.MetricsStability),
		processor.WithTraces(createTraces, metadata.TracesStability))
}

func compileConfig(base component.Config) (*compiledConfig, error) {
	cfg := base.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return compileExpressions(cfg)
}

func createLogs(_ context.Context, _ processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
	compiled, err := compileConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &logsProcessor{compiled: compiled, next: next}, nil
}
func createMetrics(_ context.Context, _ processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
	compiled, err := compileConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &metricsProcessor{compiled: compiled, next: next}, nil
}
func createTraces(_ context.Context, _ processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	compiled, err := compileConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &tracesProcessor{compiled: compiled, next: next}, nil
}
