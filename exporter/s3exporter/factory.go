package s3exporter

import (
	"context"
	"fmt"

	"github.com/grepplabs/jetstream-collector/exporter/s3exporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

var _ consumer.Logs = (*s3Exporter)(nil)
var _ consumer.Metrics = (*s3Exporter)(nil)
var _ consumer.Traces = (*s3Exporter)(nil)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		func() component.Config { return NewDefaultConfig() },
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
	)
}

func createMetricsExporter(ctx context.Context, set exporter.Settings, baseCfg component.Config) (exporter.Metrics, error) {
	cfg, ok := baseCfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("config structure is not of type *s3exporter.Config")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	exp, err := newExporter(set, cfg)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetrics(ctx, set, cfg, exp.ConsumeMetrics, helperOptions(cfg, exp)...)
}

func createTracesExporter(ctx context.Context, set exporter.Settings, baseCfg component.Config) (exporter.Traces, error) {
	cfg, ok := baseCfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("config structure is not of type *s3exporter.Config")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	exp, err := newExporter(set, cfg)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(ctx, set, cfg, exp.ConsumeTraces, helperOptions(cfg, exp)...)
}

func helperOptions(cfg *Config, exp *s3Exporter) []exporterhelper.Option {
	return []exporterhelper.Option{
		exporterhelper.WithTimeout(cfg.TimeoutSettings),
		exporterhelper.WithRetry(cfg.RetryOnFailure),
		exporterhelper.WithQueue(cfg.SendingQueue),
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
	}
}

func createLogsExporter(ctx context.Context, set exporter.Settings, baseCfg component.Config) (exporter.Logs, error) {
	cfg, ok := baseCfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("config structure is not of type *s3exporter.Config")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	exp, err := newExporter(set, cfg)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewLogs(ctx, set, cfg, exp.ConsumeLogs, helperOptions(cfg, exp)...)
}
