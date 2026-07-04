package jetstreamexporter

import (
	"context"
	"testing"

	tmpl "github.com/grepplabs/jetstream-collector/pkg/template"
	client "go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/stretchr/testify/require"
)

func TestNewSubjectTemplate(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    *subjectTemplate
		wantErr string
	}{
		{
			name:    "empty",
			pattern: "",
			want:    nil,
		},
		{
			name:    "literal only",
			pattern: "otel.logs",
			want:    &subjectTemplate{parts: []tmpl.Part{{Literal: "otel.logs"}}},
		},
		{
			name:    "header placeholder",
			pattern: "otel.${header:X-Tenant}.logs",
			want: &subjectTemplate{parts: []tmpl.Part{
				{Literal: "otel."},
				{Source: subjectSourceHeader, Key: "X-Tenant"},
				{Literal: ".logs"},
			}},
		},
		{
			name:    "resource and telemetry placeholders",
			pattern: "otel.${resource:service.name}.${telemetry:tenant}",
			want: &subjectTemplate{parts: []tmpl.Part{
				{Literal: "otel."},
				{Source: subjectSourceResource, Key: "service.name"},
				{Literal: "."},
				{Source: subjectSourceTelemetry, Key: "tenant"},
			}},
		},
		{
			name:    "all placeholder types",
			pattern: "otel.${header:X-Tenant}.${resource:service.name}.${telemetry:tenant}.logs",
			want: &subjectTemplate{parts: []tmpl.Part{
				{Literal: "otel."},
				{Source: subjectSourceHeader, Key: "X-Tenant"},
				{Literal: "."},
				{Source: subjectSourceResource, Key: "service.name"},
				{Literal: "."},
				{Source: subjectSourceTelemetry, Key: "tenant"},
				{Literal: ".logs"},
			}},
		},
		{
			name:    "missing closing brace",
			pattern: "otel.${header:X-Tenant",
			wantErr: "missing closing }",
		},
		{
			name:    "missing source or key",
			pattern: "otel.${header:}.logs",
			wantErr: "expected ${source:key}",
		},
		{
			name:    "unsupported source",
			pattern: "otel.${metadata:X-Tenant}.logs",
			wantErr: "unsupported subject source",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newSubjectTemplate(tt.pattern)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

type testSubjectSource struct {
	headers   map[string][]string
	resources map[string][]string
	telemetry map[string][]string
}

func (s testSubjectSource) HeaderValues(ctx context.Context, key string) ([]string, error) {
	return s.headers[key], nil
}

func (s testSubjectSource) ResourceValues(ctx context.Context, key string) ([]string, error) {
	return s.resources[key], nil
}

func (s testSubjectSource) TelemetryValues(ctx context.Context, key string) ([]string, error) {
	return s.telemetry[key], nil
}

func (s testSubjectSource) ResolvePart(ctx context.Context, part tmpl.Part) (string, error) {
	return resolveSubjectPart(ctx, tmpl.Part(part), s)
}

func TestSubjectTemplateResolve(t *testing.T) {
	tests := []struct {
		name    string
		tpl     *subjectTemplate
		source  testSubjectSource
		want    string
		wantErr string
	}{
		{
			name: "literal only",
			tpl:  &subjectTemplate{parts: []tmpl.Part{{Literal: "otel.logs"}}},
			want: "otel.logs",
		},
		{
			name: "header placeholder",
			tpl: &subjectTemplate{parts: []tmpl.Part{
				{Literal: "otel."},
				{Source: subjectSourceHeader, Key: "X-Tenant"},
				{Literal: ".logs"},
			}},
			source: testSubjectSource{headers: map[string][]string{"X-Tenant": {"tenant-a"}}},
			want:   "otel.tenant-a.logs",
		},
		{
			name: "resource placeholder",
			tpl: &subjectTemplate{parts: []tmpl.Part{
				{Literal: "otel."},
				{Source: subjectSourceResource, Key: "service.name"},
				{Literal: ".logs"},
			}},
			source: testSubjectSource{resources: map[string][]string{"service.name": {"orders"}}},
			want:   "otel.orders.logs",
		},
		{
			name: "telemetry placeholder",
			tpl: &subjectTemplate{parts: []tmpl.Part{
				{Literal: "otel."},
				{Source: subjectSourceTelemetry, Key: "tenant"},
				{Literal: ".logs"},
			}},
			source: testSubjectSource{telemetry: map[string][]string{"tenant": {"tenant-a"}}},
			want:   "otel.tenant-a.logs",
		},
		{
			name: "all placeholder types",
			tpl: &subjectTemplate{parts: []tmpl.Part{
				{Literal: "otel."},
				{Source: subjectSourceHeader, Key: "X-Tenant"},
				{Literal: "."},
				{Source: subjectSourceResource, Key: "service.name"},
				{Literal: "."},
				{Source: subjectSourceTelemetry, Key: "tenant"},
				{Literal: ".logs"},
			}},
			source: testSubjectSource{
				headers:   map[string][]string{"X-Tenant": {"tenant-a"}},
				resources: map[string][]string{"service.name": {"orders"}},
				telemetry: map[string][]string{"tenant": {"tenant-a"}},
			},
			want: "otel.tenant-a.orders.tenant-a.logs",
		},
		{
			name:    "missing header value",
			tpl:     &subjectTemplate{parts: []tmpl.Part{{Source: subjectSourceHeader, Key: "X-Tenant"}}},
			source:  testSubjectSource{headers: map[string][]string{}},
			wantErr: "missing subject value for header:X-Tenant",
		},
		{
			name: "conflicting resource values",
			tpl:  &subjectTemplate{parts: []tmpl.Part{{Source: subjectSourceResource, Key: "service.name"}}},
			source: testSubjectSource{resources: map[string][]string{
				"service.name": {"orders", "payments"},
			}},
			wantErr: "conflicting subject values for resource:service.name",
		},
		{
			name: "conflicting telemetry values",
			tpl:  &subjectTemplate{parts: []tmpl.Part{{Source: subjectSourceTelemetry, Key: "tenant"}}},
			source: testSubjectSource{telemetry: map[string][]string{
				"tenant": {"tenant-a", "tenant-b"},
			}},
			wantErr: "conflicting subject values for telemetry:tenant",
		},
		{
			name:    "unsupported source",
			tpl:     &subjectTemplate{parts: []tmpl.Part{{Source: "metadata", Key: "X-Tenant"}}},
			source:  testSubjectSource{},
			wantErr: "unsupported subject source",
		},
		{
			name:    "nil template",
			tpl:     nil,
			source:  testSubjectSource{},
			wantErr: "subject template is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.tpl.resolve(context.Background(), tt.source)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func newTestSubjectContext() context.Context {
	return client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"X-Tenant": {"tenant-a", "tenant-b"},
		}),
	})
}

func newTestSubjectLogs() plog.Logs {
	logs := plog.NewLogs()

	rl1 := logs.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("service.name", "orders")
	sl1 := rl1.ScopeLogs().AppendEmpty()
	sl1.LogRecords().AppendEmpty().Attributes().PutStr("tenant", "tenant-a")
	sl1.LogRecords().AppendEmpty().Attributes().PutStr("tenant", "tenant-b")

	rl2 := logs.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("service.name", "payments")
	sl2 := rl2.ScopeLogs().AppendEmpty()
	sl2.LogRecords().AppendEmpty().Attributes().PutStr("tenant", "tenant-c")

	return logs
}

func TestLogsSubjectSource(t *testing.T) {
	ctx := newTestSubjectContext()
	logs := newTestSubjectLogs()

	tests := []struct {
		name    string
		call    func(SubjectSource) ([]string, error)
		want    []string
		wantErr string
	}{
		{
			name: "header values",
			call: func(s SubjectSource) ([]string, error) { return s.HeaderValues(ctx, "X-Tenant") },
			want: []string{"tenant-a", "tenant-b"},
		},
		{
			name: "resource values",
			call: func(s SubjectSource) ([]string, error) { return s.ResourceValues(ctx, "service.name") },
			want: []string{"orders", "payments"},
		},
		{
			name: "telemetry values",
			call: func(s SubjectSource) ([]string, error) { return s.TelemetryValues(ctx, "tenant") },
			want: []string{"tenant-a", "tenant-b", "tenant-c"},
		},
		{
			name: "missing header value",
			call: func(s SubjectSource) ([]string, error) { return s.HeaderValues(ctx, "X-Missing") },
			want: nil,
		},
	}

	src := &logsSubjectResolver{LogValues: tmpl.LogValues{Logs: logs}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.call(src)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMetricsSubjectSource(t *testing.T) {
	ctx := newTestSubjectContext()
	src := &metricsSubjectResolver{}

	tests := []struct {
		name    string
		call    func(SubjectSource) ([]string, error)
		want    []string
		wantErr string
	}{
		{
			name: "header values",
			call: func(s SubjectSource) ([]string, error) { return s.HeaderValues(ctx, "X-Tenant") },
			want: []string{"tenant-a", "tenant-b"},
		},
		{
			name:    "resource unsupported",
			call:    func(s SubjectSource) ([]string, error) { return s.ResourceValues(ctx, "service.name") },
			wantErr: "not supported for metrics",
		},
		{
			name:    "telemetry unsupported",
			call:    func(s SubjectSource) ([]string, error) { return s.TelemetryValues(ctx, "tenant") },
			wantErr: "not supported for metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.call(src)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestTracesSubjectSource(t *testing.T) {
	ctx := newTestSubjectContext()
	src := &tracesSubjectResolver{}

	tests := []struct {
		name    string
		call    func(SubjectSource) ([]string, error)
		want    []string
		wantErr string
	}{
		{
			name: "header values",
			call: func(s SubjectSource) ([]string, error) { return s.HeaderValues(ctx, "X-Tenant") },
			want: []string{"tenant-a", "tenant-b"},
		},
		{
			name:    "resource unsupported",
			call:    func(s SubjectSource) ([]string, error) { return s.ResourceValues(ctx, "service.name") },
			wantErr: "not supported for traces",
		},
		{
			name:    "telemetry unsupported",
			call:    func(s SubjectSource) ([]string, error) { return s.TelemetryValues(ctx, "tenant") },
			wantErr: "not supported for traces",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.call(src)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestProfilesSubjectSource(t *testing.T) {
	ctx := newTestSubjectContext()
	src := &profilesSubjectResolver{}

	tests := []struct {
		name    string
		call    func(SubjectSource) ([]string, error)
		want    []string
		wantErr string
	}{
		{
			name: "header values",
			call: func(s SubjectSource) ([]string, error) { return s.HeaderValues(ctx, "X-Tenant") },
			want: []string{"tenant-a", "tenant-b"},
		},
		{
			name:    "resource unsupported",
			call:    func(s SubjectSource) ([]string, error) { return s.ResourceValues(ctx, "service.name") },
			wantErr: "not supported for profiles",
		},
		{
			name:    "telemetry unsupported",
			call:    func(s SubjectSource) ([]string, error) { return s.TelemetryValues(ctx, "tenant") },
			wantErr: "not supported for profiles",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.call(src)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
