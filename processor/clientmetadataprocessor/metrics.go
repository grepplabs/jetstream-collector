package clientmetadataprocessor

import (
	"context"

	tmpl "github.com/grepplabs/jetstream-collector/pkg/template"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type metricsClientMetadataProcessor struct {
	specs []clientMetadataSpec
	next  consumer.Metrics
}

func newMetricsClientMetadataProcessor(cfg *Config, next consumer.Metrics) (*metricsClientMetadataProcessor, error) {
	specs, err := parseClientMetadataSpecs(cfg.ClientMetadata)
	if err != nil {
		return nil, err
	}
	return &metricsClientMetadataProcessor{specs: specs, next: next}, nil
}

func (p *metricsClientMetadataProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (*metricsClientMetadataProcessor) Start(context.Context, component.Host) error { return nil }

func (*metricsClientMetadataProcessor) Shutdown(context.Context) error { return nil }

func (p *metricsClientMetadataProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	entries, err := resolveClientMetadata(ctx, p.specs, tmpl.MetricsValues{Metrics: md})
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	return p.next.ConsumeMetrics(addClientMetadata(ctx, entries), md)
}
