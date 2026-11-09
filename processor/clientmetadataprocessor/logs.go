package clientmetadataprocessor

import (
	"context"

	tmpl "github.com/grepplabs/jetstream-collector/pkg/template"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
)

type logsClientMetadataProcessor struct {
	next  consumer.Logs
	specs []clientMetadataSpec
}

func newLogsClientMetadataProcessor(cfg *Config, next consumer.Logs) (*logsClientMetadataProcessor, error) {
	specs, err := parseClientMetadataSpecs(cfg.ClientMetadata)
	if err != nil {
		return nil, err
	}
	return &logsClientMetadataProcessor{next: next, specs: specs}, nil
}

func (p *logsClientMetadataProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (*logsClientMetadataProcessor) Start(context.Context, component.Host) error { return nil }

func (*logsClientMetadataProcessor) Shutdown(context.Context) error { return nil }

func (p *logsClientMetadataProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	entries, err := resolveClientMetadata(ctx, p.specs, tmpl.LogsValues{Logs: ld})
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	return p.next.ConsumeLogs(addClientMetadata(ctx, entries), ld)
}
