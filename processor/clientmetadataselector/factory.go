package clientmetadataselector

import (
	"context"

	"github.com/grepplabs/jetstream-collector/processor/clientmetadataselector/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
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
		xprocessor.WithProfiles(createProfilesProcessor, metadata.ProfilesStability),
	)
}

func createLogsProcessor(_ context.Context, _ processor.Settings, baseCfg component.Config, next consumer.Logs) (processor.Logs, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newLogsProcessor(cfg, next), nil
}

func createMetricsProcessor(_ context.Context, _ processor.Settings, baseCfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &metricsProcessor{selector: newSelector(cfg), next: next}, nil
}

func createTracesProcessor(_ context.Context, _ processor.Settings, baseCfg component.Config, next consumer.Traces) (processor.Traces, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &tracesProcessor{selector: newSelector(cfg), next: next}, nil
}

func createProfilesProcessor(_ context.Context, _ processor.Settings, baseCfg component.Config, next xconsumer.Profiles) (xprocessor.Profiles, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &profilesProcessor{selector: newSelector(cfg), next: next}, nil
}
