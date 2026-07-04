package s3exporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestFilenameTemplateTimeFmt(t *testing.T) {
	tpl, err := newKeyPrefixTemplate("year=${timefmt:%Y}/month=${timefmt:%m}/day=${timefmt:%d}/stamp=${timefmt:%Y-%m-%d-%H-%M-%S-%f}")
	require.NoError(t, err)

	resolvedAt := time.Date(2026, 7, 22, 13, 14, 15, 987654321, time.UTC)
	logs := newTestLogs(time.Date(1999, 1, 2, 3, 4, 5, 6, time.UTC))
	got, err := newLogsKeyPrefixResolver(logs, tpl, resolvedAt).Resolve(context.Background())
	require.NoError(t, err)

	want := "year=2026/month=07/day=22/stamp=2026-07-22-13-14-15-987654"
	assert.Equal(t, want, got)
}

func newTestLogs(ts time.Time) plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	rec := sl.LogRecords().AppendEmpty()
	rec.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	return logs
}
