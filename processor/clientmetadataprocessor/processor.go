package clientmetadataprocessor

import (
	"context"
	"fmt"

	tmpl "github.com/grepplabs/jetstream-collector/pkg/template"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
)

type clientMetadataProcessor struct {
	cfg   *Config
	next  consumer.Logs
	specs []clientMetadataSpec
}

func newClientMetadataProcessor(cfg *Config, next consumer.Logs) (*clientMetadataProcessor, error) {
	specs, err := parseClientMetadataSpecs(cfg.ClientMetadata)
	if err != nil {
		return nil, err
	}

	return &clientMetadataProcessor{
		cfg:   cfg,
		next:  next,
		specs: specs,
	}, nil
}

func (p *clientMetadataProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *clientMetadataProcessor) Start(context.Context, component.Host) error {
	return nil
}

func (p *clientMetadataProcessor) Shutdown(context.Context) error {
	return nil
}

func (p *clientMetadataProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	entries, err := p.resolveClientMetadata(ctx, ld)
	if err != nil {
		return consumererror.NewPermanent(err)
	}

	ctx = addClientMetadata(ctx, entries)
	return p.next.ConsumeLogs(ctx, ld)
}

func (p *clientMetadataProcessor) resolveClientMetadata(ctx context.Context, ld plog.Logs) (map[string]string, error) {
	if len(p.specs) == 0 {
		return nil, nil
	}

	resolver := &logsClientMetadataResolver{LogValues: tmpl.LogValues{Logs: ld}}
	entries := make(map[string]string, len(p.specs))

	for _, spec := range p.specs {
		value, err := resolveClientMetadataValue(ctx, spec, resolver)
		if err != nil {
			return nil, err
		}
		entries[spec.key] = value
	}

	return entries, nil
}

func resolveClientMetadataValue(ctx context.Context, spec clientMetadataSpec, source tmpl.Source) (string, error) {
	switch spec.source {
	case clientMetadataSourceResource:
		values, err := source.ResourceValues(ctx, spec.key)
		if err != nil {
			return "", err
		}
		return tmpl.ResolveValue("client_metadata", spec.source, spec.key, values, true)
	case clientMetadataSourceTelemetry:
		values, err := source.TelemetryValues(ctx, spec.key)
		if err != nil {
			return "", err
		}
		return tmpl.ResolveValue("client_metadata", spec.source, spec.key, values, true)
	default:
		return "", fmt.Errorf("unsupported client_metadata source %q", spec.source)
	}
}

type logsClientMetadataResolver struct {
	tmpl.LogValues
}

func addClientMetadata(ctx context.Context, entries map[string]string) context.Context {
	if len(entries) == 0 {
		return ctx
	}

	info := client.FromContext(ctx)
	metadata := make(map[string][]string)

	for existingKey := range info.Metadata.Keys() {
		metadata[existingKey] = info.Metadata.Get(existingKey)
	}
	for key, value := range entries {
		metadata[key] = []string{value}
	}

	info.Metadata = client.NewMetadata(metadata)
	return client.NewContext(ctx, info)
}
