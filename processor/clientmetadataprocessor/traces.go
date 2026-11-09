package clientmetadataprocessor

import (
	"context"

	tmpl "github.com/grepplabs/jetstream-collector/pkg/template"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type tracesClientMetadataProcessor struct {
	specs []clientMetadataSpec
	next  consumer.Traces
}

func newTracesClientMetadataProcessor(cfg *Config, next consumer.Traces) (*tracesClientMetadataProcessor, error) {
	specs, err := parseClientMetadataSpecs(cfg.ClientMetadata)
	if err != nil {
		return nil, err
	}
	return &tracesClientMetadataProcessor{specs: specs, next: next}, nil
}

func (p *tracesClientMetadataProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (*tracesClientMetadataProcessor) Start(context.Context, component.Host) error { return nil }

func (*tracesClientMetadataProcessor) Shutdown(context.Context) error { return nil }

func (p *tracesClientMetadataProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	entries, err := resolveClientMetadata(ctx, p.specs, tmpl.TracesValues{Traces: td})
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	return p.next.ConsumeTraces(addClientMetadata(ctx, entries), td)
}
