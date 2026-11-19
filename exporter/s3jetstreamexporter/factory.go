package s3jetstreamexporter

import (
	"context"
	"fmt"

	"github.com/grepplabs/jetstream-collector/exporter/s3jetstreamexporter/internal/metadata"
	protocol "github.com/grepplabs/jetstream-collector/pkg/s3jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/xexporter"
)

var _ consumer.Logs = (*s3JetStreamExporter)(nil)
var _ consumer.Metrics = (*s3JetStreamExporter)(nil)
var _ consumer.Traces = (*s3JetStreamExporter)(nil)
var _ xconsumer.Profiles = (*s3JetStreamExporter)(nil)

func NewFactory() exporter.Factory {
	return xexporter.NewFactory(metadata.Type, func() component.Config { return NewDefaultConfig() },
		xexporter.WithLogs(createLogs, metadata.LogsStability), xexporter.WithMetrics(createMetrics, metadata.MetricsStability),
		xexporter.WithTraces(createTraces, metadata.TracesStability), xexporter.WithProfiles(createProfiles, metadata.ProfilesStability))
}

func build(cfg component.Config, set exporter.Settings, signal protocol.Signal) (*s3JetStreamExporter, error) {
	c := cfg.(*Config)
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c.Signal != signal {
		return nil, fmt.Errorf("configured signal %q does not match %q pipeline", c.Signal, signal)
	}
	return newExporter(set, c)
}
func createLogs(_ context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	return build(cfg, set, protocol.SignalLogs)
}
func createMetrics(_ context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	return build(cfg, set, protocol.SignalMetrics)
}
func createTraces(_ context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	return build(cfg, set, protocol.SignalTraces)
}
func createProfiles(_ context.Context, set exporter.Settings, cfg component.Config) (xexporter.Profiles, error) {
	return build(cfg, set, protocol.SignalProfiles)
}
