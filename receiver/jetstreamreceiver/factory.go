package jetstreamreceiver

import (
	"context"

	"github.com/grepplabs/jetstream-collector/receiver/jetstreamreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/xreceiver"
)

var _ receiver.Logs = (*jetstreamReceiver)(nil)
var _ receiver.Metrics = (*jetstreamReceiver)(nil)
var _ receiver.Traces = (*jetstreamReceiver)(nil)
var _ xreceiver.Profiles = (*jetstreamReceiver)(nil)

var typeStr = component.MustNewType("jetstream")

// NewFactory creates a receiver factory for the JetStream receiver.
func NewFactory() receiver.Factory {
	return xreceiver.NewFactory(
		metadata.Type,
		func() component.Config { return NewDefaultConfig() },
		xreceiver.WithLogs(createLogsReceiver, metadata.LogsStability),
		xreceiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
		xreceiver.WithTraces(createTracesReceiver, metadata.TracesStability),
		xreceiver.WithProfiles(createProfilesReceiver, metadata.ProfilesStability),
	)
}

func createLogsReceiver(_ context.Context, set receiver.Settings, baseCfg component.Config, next consumer.Logs) (receiver.Logs, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newLogsReceiver(set, cfg, next)
}

func createMetricsReceiver(_ context.Context, set receiver.Settings, baseCfg component.Config, next consumer.Metrics) (receiver.Metrics, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newMetricsReceiver(set, cfg, next)
}

func createTracesReceiver(_ context.Context, set receiver.Settings, baseCfg component.Config, next consumer.Traces) (receiver.Traces, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newTracesReceiver(set, cfg, next)
}
func createProfilesReceiver(_ context.Context, set receiver.Settings, baseCfg component.Config, next xconsumer.Profiles) (xreceiver.Profiles, error) {
	cfg := baseCfg.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newProfilesReceiver(set, cfg, next)
}
