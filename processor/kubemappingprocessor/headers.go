package kubemappingprocessor

import (
	"context"
	"errors"
	"fmt"

	tmpl "github.com/grepplabs/jetstream-collector/pkg/template"
	"go.opentelemetry.io/collector/client"
)

const headerTemplateSource = "header"

var errMissingHeader = errors.New("referenced header is missing")

func parseHeaderTemplate(pattern string) ([]tmpl.Part, error) {
	return tmpl.Parse(pattern, func(part tmpl.Part) error {
		if part.Source != headerTemplateSource {
			return fmt.Errorf("unsupported selector template source %q", part.Source)
		}
		return nil
	})
}

func validateHeaderTemplate(pattern string) error {
	_, err := parseHeaderTemplate(pattern)
	return err
}

type metadataHeaderResolver struct {
	metadata client.Metadata
}

func (r metadataHeaderResolver) ResolvePart(_ context.Context, part tmpl.Part) (string, error) {
	values := r.metadata.Get(part.Key)
	if len(values) == 0 {
		return "", errMissingHeader
	}
	return tmpl.ResolveValue("selector", part.Source, part.Key, values, false)
}

func expandHeaderTemplate(ctx context.Context, pattern string, metadata client.Metadata) (string, bool, error) {
	parts, err := parseHeaderTemplate(pattern)
	if err != nil {
		return "", false, err
	}
	value, err := tmpl.Resolve(ctx, parts, metadataHeaderResolver{metadata: metadata})
	if errors.Is(err, errMissingHeader) {
		return "", false, nil
	}
	return value, err == nil, err
}
