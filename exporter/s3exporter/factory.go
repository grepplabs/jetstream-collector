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

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		func() component.Config { return NewDefaultConfig() },
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
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
