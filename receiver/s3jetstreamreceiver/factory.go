package s3jetstreamreceiver

import (
	"context"
	"fmt"

	protocol "github.com/grepplabs/jetstream-collector/pkg/s3jetstream"
	"github.com/grepplabs/jetstream-collector/receiver/s3jetstreamreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/xreceiver"
)

func NewFactory() receiver.Factory {
	return xreceiver.NewFactory(metadata.Type, func() component.Config { return NewDefaultConfig() }, xreceiver.WithLogs(createLogs, metadata.LogsStability), xreceiver.WithMetrics(createMetrics, metadata.MetricsStability), xreceiver.WithTraces(createTraces, metadata.TracesStability), xreceiver.WithProfiles(createProfiles, metadata.ProfilesStability))
}
func makeReceiver(cfg component.Config, set receiver.Settings, signal protocol.Signal, next any) (*s3JetStreamReceiver, error) {
	c := cfg.(*Config)
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c.Signal != signal {
		return nil, fmt.Errorf("configured signal %q does not match %q pipeline", c.Signal, signal)
	}
	return newReceiver(set, c, string(signal), next)
}
func createLogs(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Logs) (receiver.Logs, error) {
	return makeReceiver(cfg, set, protocol.SignalLogs, next)
}
func createMetrics(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Metrics) (receiver.Metrics, error) {
	return makeReceiver(cfg, set, protocol.SignalMetrics, next)
}
func createTraces(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Traces) (receiver.Traces, error) {
	return makeReceiver(cfg, set, protocol.SignalTraces, next)
}
func createProfiles(_ context.Context, set receiver.Settings, cfg component.Config, next xconsumer.Profiles) (xreceiver.Profiles, error) {
	return makeReceiver(cfg, set, protocol.SignalProfiles, next)
}
