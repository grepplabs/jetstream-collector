package template

import (
	"context"

	client "go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type TracesValues struct {
	Traces ptrace.Traces
}

func (v TracesValues) ResourceValues(_ context.Context, key string) ([]string, error) {
	values := make([]string, 0)
	for i := 0; i < v.Traces.ResourceSpans().Len(); i++ {
		attrs := v.Traces.ResourceSpans().At(i).Resource().Attributes()
		if value, ok := attrs.Get(key); ok {
			values = append(values, value.AsString())
		}
	}
	return values, nil
}

func (v TracesValues) HeaderValues(ctx context.Context, key string) ([]string, error) {
	info := client.FromContext(ctx)
	return info.Metadata.Get(key), nil
}

func (v TracesValues) TelemetryValues(_ context.Context, key string) ([]string, error) {
	values := make([]string, 0)
	for ri := 0; ri < v.Traces.ResourceSpans().Len(); ri++ {
		rs := v.Traces.ResourceSpans().At(ri)
		for si := 0; si < rs.ScopeSpans().Len(); si++ {
			spans := rs.ScopeSpans().At(si).Spans()
			for i := 0; i < spans.Len(); i++ {
				if value, ok := spans.At(i).Attributes().Get(key); ok {
					values = append(values, value.AsString())
				}
			}
		}
	}
	return values, nil
}
