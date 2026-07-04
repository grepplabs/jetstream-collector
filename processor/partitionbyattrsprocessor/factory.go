package partitionbyattrsprocessor

import (
	"context"

	"github.com/grepplabs/jetstream-collector/processor/partitionbyattrsprocessor/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		func() component.Config { return NewDefaultConfig() },
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
	)
}

func NewDefaultConfig() component.Config {
	return &Config{
		MissingAttributeAction: MissingAttributeActionError,
	}
}

func createLogsProcessor(_ context.Context, _ processor.Settings, baseCfg component.Config, nextConsumer consumer.Logs) (processor.Logs, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newPartitionByAttrsProcessor(cfg, nextConsumer), nil
}
