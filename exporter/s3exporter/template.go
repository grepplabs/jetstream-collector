package s3exporter

import (
	"context"
	"fmt"
	"time"

	tmpl "github.com/grepplabs/jetstream-collector/pkg/template"
	"github.com/itchyny/timefmt-go"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	keySourceHeader    = "header"
	keySourceResource  = "resource"
	keySourceRecord    = "record"
	keySourceTelemetry = "telemetry"
	keySourceTimeFmt   = "timefmt"
)

type KeyPrefixResolver interface {
	Resolve(context.Context) (string, error)
}

type keyPrefixTemplate struct {
	parts []tmpl.Part
}

func newKeyPrefixTemplate(pattern string) (*keyPrefixTemplate, error) {
	parts, err := tmpl.Parse(pattern, func(part tmpl.Part) error {
		switch part.Source {
		case keySourceHeader, keySourceResource, keySourceRecord, keySourceTelemetry, keySourceTimeFmt:
			return nil
		default:
			return fmt.Errorf("unsupported filename_template source %q", part.Source)
		}
	})
	if err != nil {
		return nil, err
	}
	return &keyPrefixTemplate{parts: parts}, nil
}

func (t *keyPrefixTemplate) resolve(ctx context.Context, source tmpl.Resolver) (string, error) {
	if t == nil {
		return "", fmt.Errorf("filename_template is nil")
	}
	return tmpl.Resolve(ctx, t.parts, source)
}

func resolveKeyPrefixPart(ctx context.Context, part tmpl.Part, source KeySource) (string, error) {
	switch part.Source {
	case keySourceHeader:
		values, err := source.HeaderValues(ctx, part.Key)
		if err != nil {
			return "", err
		}
		return tmpl.ResolveValue("filename_template", part.Source, part.Key, values, false)
	case keySourceResource:
		values, err := source.ResourceValues(ctx, part.Key)
		if err != nil {
			return "", err
		}
		return tmpl.ResolveValue("filename_template", part.Source, part.Key, values, true)
	case keySourceRecord:
		values, err := source.TelemetryValues(ctx, part.Key)
		if err != nil {
			return "", err
		}
		return tmpl.ResolveValue("filename_template", part.Source, part.Key, values, true)
	case keySourceTelemetry:
		values, err := source.TelemetryValues(ctx, part.Key)
		if err != nil {
			return "", err
		}
		return tmpl.ResolveValue("filename_template", part.Source, part.Key, values, true)
	case keySourceTimeFmt:
		return source.TimefmtValue(ctx, part.Key)
	default:
		return "", fmt.Errorf("unsupported filename_template source %q", part.Source)
	}
}

type KeySource interface {
	tmpl.Source
	TimefmtValue(context.Context, string) (string, error)
}

type logsKeyPrefixResolver struct {
	tmpl.LogValues
	template   *keyPrefixTemplate
	resolvedAt time.Time
}

func newLogsKeyPrefixResolver(logs plog.Logs, template *keyPrefixTemplate, resolvedAt time.Time) KeyPrefixResolver {
	return &logsKeyPrefixResolver{LogValues: tmpl.LogValues{Logs: logs}, template: template, resolvedAt: resolvedAt}
}

func (r *logsKeyPrefixResolver) Resolve(ctx context.Context) (string, error) {
	if r.template == nil {
		return "", nil
	}
	return r.template.resolve(ctx, r)
}

func (r *logsKeyPrefixResolver) ResolvePart(ctx context.Context, part tmpl.Part) (string, error) {
	return resolveKeyPrefixPart(ctx, part, r)
}

func (r *logsKeyPrefixResolver) TimefmtValue(_ context.Context, layout string) (string, error) {
	return timefmt.Format(r.resolvedAt, layout), nil
}
