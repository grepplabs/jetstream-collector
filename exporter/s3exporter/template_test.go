package s3exporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestFilenameTemplateTimeFmt(t *testing.T) {
	tpl, err := newKeyPrefixTemplate("year=${timefmt:%Y}/month=${timefmt:%m}/day=${timefmt:%d}/stamp=${timefmt:%Y-%m-%d-%H-%M-%S-%f}")
	require.NoError(t, err)

	resolvedAt := time.Date(2026, 7, 22, 13, 14, 15, 987654321, time.UTC)
	logs := plog.NewLogs()
	record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.SetTimestamp(pcommon.NewTimestampFromTime(time.Date(1999, 1, 2, 3, 4, 5, 6, time.UTC)))
	got, err := newLogsKeyPrefixResolver(logs, tpl, resolvedAt).Resolve(context.Background())
	require.NoError(t, err)

	want := "year=2026/month=07/day=22/stamp=2026-07-22-13-14-15-987654"
	assert.Equal(t, want, got)
}

func TestMetricsFilenameTemplate(t *testing.T) {
	tpl, err := newKeyPrefixTemplate("${resource:service.name}/${record:tenant}/${timefmt:%Y/%m/%d}")
	require.NoError(t, err)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "checkout")
	metric := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetEmptyGauge()
	metric.Gauge().DataPoints().AppendEmpty().Attributes().PutStr("tenant", "tenant-a")

	resolvedAt := time.Date(2026, 7, 22, 13, 14, 15, 0, time.UTC)
	got, err := newMetricsKeyPrefixResolver(metrics, tpl, resolvedAt).Resolve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "checkout/tenant-a/2026/07/22", got)
}

func TestTracesFilenameTemplate(t *testing.T) {
	tpl, err := newKeyPrefixTemplate("${resource:service.name}/${record:tenant}/${timefmt:%Y/%m/%d}")
	require.NoError(t, err)

	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty().Attributes().PutStr("tenant", "tenant-a")

	resolvedAt := time.Date(2026, 7, 22, 13, 14, 15, 0, time.UTC)
	got, err := newTracesKeyPrefixResolver(traces, tpl, resolvedAt).Resolve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "checkout/tenant-a/2026/07/22", got)
}
