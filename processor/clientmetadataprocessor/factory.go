package clientmetadataprocessor

import (
	"context"

	"github.com/grepplabs/jetstream-collector/processor/clientmetadataprocessor/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/xprocessor"
)

func NewFactory() xprocessor.Factory {
	return xprocessor.NewFactory(
		metadata.Type,
		func() component.Config { return NewDefaultConfig() },
		xprocessor.WithLogs(createLogsProcessor, metadata.LogsStability),
		xprocessor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		xprocessor.WithTraces(createTracesProcessor, metadata.TracesStability),
	)
}

func createLogsProcessor(_ context.Context, _ processor.Settings, baseCfg component.Config, nextConsumer consumer.Logs) (processor.Logs, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newLogsClientMetadataProcessor(cfg, nextConsumer)
}

func createMetricsProcessor(_ context.Context, _ processor.Settings, baseCfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newMetricsClientMetadataProcessor(cfg, next)
}

func createTracesProcessor(_ context.Context, _ processor.Settings, baseCfg component.Config, next consumer.Traces) (processor.Traces, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newTracesClientMetadataProcessor(cfg, next)
}
