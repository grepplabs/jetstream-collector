package jetstreamexporter

import (
	"context"

	"github.com/grepplabs/jetstream-collector/exporter/jetstreamexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/xexporter"
)

var _ consumer.Logs = (*jetstreamExporter)(nil)
var _ consumer.Metrics = (*jetstreamExporter)(nil)
var _ consumer.Traces = (*jetstreamExporter)(nil)
var _ xconsumer.Profiles = (*jetstreamExporter)(nil)

// NewFactory creates an exporter factory for JetStream.
func NewFactory() exporter.Factory {
	return xexporter.NewFactory(
		metadata.Type,
		func() component.Config { return NewDefaultConfig() },
		xexporter.WithLogs(createLogsExporter, metadata.LogsStability),
		xexporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		xexporter.WithTraces(createTracesExporter, metadata.TracesStability),
		xexporter.WithProfiles(createProfilesExporter, metadata.ProfilesStability),
	)
}

func helperOptions(cfg *Config, exp *jetstreamExporter) []exporterhelper.Option {
	return []exporterhelper.Option{
		exporterhelper.WithTimeout(cfg.TimeoutSettings),
		exporterhelper.WithRetry(cfg.RetryOnFailure),
		exporterhelper.WithQueue(cfg.SendingQueue),
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
	}
}

func createLogsExporter(ctx context.Context, set exporter.Settings, baseCfg component.Config) (exporter.Logs, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	exp, err := newExporter(set, cfg)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(ctx, set, cfg, exp.ConsumeLogs, helperOptions(cfg, exp)...)
}

func createMetricsExporter(ctx context.Context, set exporter.Settings, baseCfg component.Config) (exporter.Metrics, error) {
	cfg := baseCfg.(*Config)
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
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	exp, err := newExporter(set, cfg)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(ctx, set, cfg, exp.ConsumeTraces, helperOptions(cfg, exp)...)
}

func createProfilesExporter(_ context.Context, set exporter.Settings, baseCfg component.Config) (xexporter.Profiles, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newExporter(set, cfg)
}
