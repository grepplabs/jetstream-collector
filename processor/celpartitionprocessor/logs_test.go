package celpartitionprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
)

type captureLogsConsumer struct{ batches []plog.Logs }

func (c *captureLogsConsumer) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (c *captureLogsConsumer) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	c.batches = append(c.batches, ld)
	return nil
}

func TestLogsPartitionByItemAndResourceCEL(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.PartitionBy.Logs = []string{"record.attributes['tenant']", "record.resource.attributes['service.name']"}
	compiled, err := compileConfig(cfg)
	require.NoError(t, err)
	next := &captureLogsConsumer{}
	proc := &logsProcessor{compiled: compiled, next: next}
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	sl := rl.ScopeLogs().AppendEmpty()
	for _, tenant := range []string{"a", "b", "a"} {
		record := sl.LogRecords().AppendEmpty()
		record.Attributes().PutStr("tenant", tenant)
	}
	require.NoError(t, proc.ConsumeLogs(context.Background(), ld))
	require.Len(t, next.batches, 2)
	require.ElementsMatch(t, []int{2, 1}, []int{countRecords(next.batches[0]), countRecords(next.batches[1])})
}

func TestLogsMissingAttributeRejectsOrDrops(t *testing.T) {
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	cfg := NewDefaultConfig()
	cfg.PartitionBy.Logs = []string{"record.attributes['tenant']"}
	cfg.OnError = OnErrorReject
	compiled, err := compileConfig(cfg)
	require.NoError(t, err)
	next := &captureLogsConsumer{}
	proc := &logsProcessor{compiled: compiled, next: next}
	require.Error(t, proc.ConsumeLogs(context.Background(), ld))
	require.Empty(t, next.batches)

	cfg.OnError = OnErrorDrop
	compiled, err = compileConfig(cfg)
	require.NoError(t, err)
	proc = &logsProcessor{compiled: compiled, next: next}
	require.NoError(t, proc.ConsumeLogs(context.Background(), ld))
	require.Empty(t, next.batches)
}

func countRecords(ld plog.Logs) int {
	total := 0
	for ri := 0; ri < ld.ResourceLogs().Len(); ri++ {
		rl := ld.ResourceLogs().At(ri)
		for si := 0; si < rl.ScopeLogs().Len(); si++ {
			total += rl.ScopeLogs().At(si).LogRecords().Len()
		}
	}
	return total
}
