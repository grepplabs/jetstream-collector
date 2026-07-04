package jetstreamexporter

import (
	"context"
	"fmt"

	tmpl "github.com/grepplabs/jetstream-collector/pkg/template"
	client "go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	subjectSourceHeader    = "header"
	subjectSourceResource  = "resource"
	subjectSourceTelemetry = "telemetry"
)

type SubjectResolver interface {
	Resolve(context.Context) (string, error)
}

type subjectTemplate struct {
	parts []tmpl.Part
}

func newSubjectTemplate(pattern string) (*subjectTemplate, error) {
	if pattern == "" {
		return nil, nil
	}
	parts, err := tmpl.Parse(pattern, func(part tmpl.Part) error {
		switch part.Source {
		case subjectSourceHeader, subjectSourceResource, subjectSourceTelemetry:
			return nil
		default:
			return fmt.Errorf("unsupported subject source %q", part.Source)
		}
	})
	if err != nil {
		return nil, err
	}
	return &subjectTemplate{parts: parts}, nil
}

func (t *subjectTemplate) resolve(ctx context.Context, source subjectResolver) (string, error) {
	if t == nil {
		return "", fmt.Errorf("subject template is nil")
	}
	return tmpl.Resolve(ctx, t.parts, source)
}

func resolveSubject(ctx context.Context, subject string, template *subjectTemplate, source subjectResolver) (string, error) {
	if template == nil {
		if subject == "" {
			return "", fmt.Errorf("subject is required")
		}
		return subject, nil
	}

	return template.resolve(ctx, source)
}

func resolveSubjectPart(ctx context.Context, part tmpl.Part, source SubjectSource) (string, error) {
	switch part.Source {
	case subjectSourceHeader:
		values, err := source.HeaderValues(ctx, part.Key)
		if err != nil {
			return "", err
		}
		return tmpl.ResolveValue("subject", part.Source, part.Key, values, true)
	case subjectSourceResource:
		values, err := source.ResourceValues(ctx, part.Key)
		if err != nil {
			return "", err
		}
		return tmpl.ResolveValue("subject", part.Source, part.Key, values, true)
	case subjectSourceTelemetry:
		values, err := source.TelemetryValues(ctx, part.Key)
		if err != nil {
			return "", err
		}
		return tmpl.ResolveValue("subject", part.Source, part.Key, values, true)
	default:
		return "", fmt.Errorf("unsupported subject source %q", part.Source)
	}
}

type SubjectSource interface {
	tmpl.Source
}

type subjectResolver interface {
	tmpl.Resolver
}

type logsSubjectResolver struct {
	tmpl.LogValues
	subject  string
	template *subjectTemplate
}

func NewLogsSubjectResolver(logs plog.Logs, subject string, template *subjectTemplate) SubjectResolver {
	return &logsSubjectResolver{
		LogValues: tmpl.LogValues{Logs: logs},
		subject:   subject,
		template:  template,
	}
}

func (r *logsSubjectResolver) Resolve(ctx context.Context) (string, error) {
	return resolveSubject(ctx, r.subject, r.template, r)
}

func (r *logsSubjectResolver) ResolvePart(ctx context.Context, part tmpl.Part) (string, error) {
	return resolveSubjectPart(ctx, part, r)
}

type metricsSubjectResolver struct {
	subject  string
	template *subjectTemplate
}

func NewMetricsSubjectResolver(_ pmetric.Metrics, subject string, template *subjectTemplate) SubjectResolver {
	return &metricsSubjectResolver{
		subject:  subject,
		template: template,
	}
}

func (r *metricsSubjectResolver) Resolve(ctx context.Context) (string, error) {
	return resolveSubject(ctx, r.subject, r.template, r)
}

func (r *metricsSubjectResolver) ResolvePart(ctx context.Context, part tmpl.Part) (string, error) {
	return resolveSubjectPart(ctx, part, r)
}

func (r *metricsSubjectResolver) HeaderValues(ctx context.Context, key string) ([]string, error) {
	info := client.FromContext(ctx)
	return info.Metadata.Get(key), nil
}

func (r *metricsSubjectResolver) ResourceValues(ctx context.Context, key string) ([]string, error) {
	return nil, fmt.Errorf("subject source %q is not supported for metrics", subjectSourceResource)
}

func (r *metricsSubjectResolver) TelemetryValues(ctx context.Context, key string) ([]string, error) {
	return nil, fmt.Errorf("subject source %q is not supported for metrics", subjectSourceTelemetry)
}

type tracesSubjectResolver struct {
	subject  string
	template *subjectTemplate
}

func NewTracesSubjectResolver(_ ptrace.Traces, subject string, template *subjectTemplate) SubjectResolver {
	return &tracesSubjectResolver{
		subject:  subject,
		template: template,
	}
}

func (r *tracesSubjectResolver) Resolve(ctx context.Context) (string, error) {
	return resolveSubject(ctx, r.subject, r.template, r)
}

func (r *tracesSubjectResolver) ResolvePart(ctx context.Context, part tmpl.Part) (string, error) {
	return resolveSubjectPart(ctx, part, r)
}

func (r *tracesSubjectResolver) HeaderValues(ctx context.Context, key string) ([]string, error) {
	info := client.FromContext(ctx)
	return info.Metadata.Get(key), nil
}

func (r *tracesSubjectResolver) ResourceValues(ctx context.Context, key string) ([]string, error) {
	return nil, fmt.Errorf("subject source %q is not supported for traces", subjectSourceResource)
}

func (r *tracesSubjectResolver) TelemetryValues(ctx context.Context, key string) ([]string, error) {
	return nil, fmt.Errorf("subject source %q is not supported for traces", subjectSourceTelemetry)
}

type profilesSubjectResolver struct {
	subject  string
	template *subjectTemplate
}

func NewProfilesSubjectResolver(_ pprofile.Profiles, subject string, template *subjectTemplate) SubjectResolver {
	return &profilesSubjectResolver{
		subject:  subject,
		template: template,
	}
}

func (r *profilesSubjectResolver) Resolve(ctx context.Context) (string, error) {
	return resolveSubject(ctx, r.subject, r.template, r)
}

func (r *profilesSubjectResolver) ResolvePart(ctx context.Context, part tmpl.Part) (string, error) {
	return resolveSubjectPart(ctx, part, r)
}

func (r *profilesSubjectResolver) HeaderValues(ctx context.Context, key string) ([]string, error) {
	info := client.FromContext(ctx)
	return info.Metadata.Get(key), nil
}

func (r *profilesSubjectResolver) ResourceValues(ctx context.Context, key string) ([]string, error) {
	return nil, fmt.Errorf("subject source %q is not supported for profiles", subjectSourceResource)
}

func (r *profilesSubjectResolver) TelemetryValues(ctx context.Context, key string) ([]string, error) {
	return nil, fmt.Errorf("subject source %q is not supported for profiles", subjectSourceTelemetry)
}
