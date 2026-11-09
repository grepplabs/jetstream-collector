package clientmetadataprocessor

import (
	"context"
	"fmt"

	tmpl "github.com/grepplabs/jetstream-collector/pkg/template"
	"go.opentelemetry.io/collector/client"
)

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

func resolveClientMetadata(ctx context.Context, specs []clientMetadataSpec, source tmpl.Source) (map[string]string, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	entries := make(map[string]string, len(specs))
	for _, spec := range specs {
		value, err := resolveClientMetadataValue(ctx, spec, source)
		if err != nil {
			return nil, err
		}
		entries[spec.key] = value
	}
	return entries, nil
}
