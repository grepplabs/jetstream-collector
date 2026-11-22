package flowmanagerextension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestFanoutCallsConsumersForEverySignal(t *testing.T) {
	t.Run("logs", func(t *testing.T) {
		calls := 0
		consumers := []consumer.Logs{newLogsTestConsumer(t, &calls), newLogsTestConsumer(t, &calls)}
		require.NoError(t, newLogsFanout(consumers).ConsumeLogs(context.Background(), plog.NewLogs()))
		assertFanoutCalls(t, calls)
	})
	t.Run("metrics", func(t *testing.T) {
		calls := 0
		consumers := []consumer.Metrics{newMetricsTestConsumer(t, &calls), newMetricsTestConsumer(t, &calls)}
		require.NoError(t, newMetricsFanout(consumers).ConsumeMetrics(context.Background(), pmetric.NewMetrics()))
		assertFanoutCalls(t, calls)
	})
	t.Run("traces", func(t *testing.T) {
		calls := 0
		consumers := []consumer.Traces{newTracesTestConsumer(t, &calls), newTracesTestConsumer(t, &calls)}
		require.NoError(t, newTracesFanout(consumers).ConsumeTraces(context.Background(), ptrace.NewTraces()))
		assertFanoutCalls(t, calls)
	})
	t.Run("profiles", func(t *testing.T) {
		calls := 0
		consumers := []xconsumer.Profiles{newProfilesTestConsumer(t, &calls), newProfilesTestConsumer(t, &calls)}
		require.NoError(t, newProfilesFanout(consumers).ConsumeProfiles(context.Background(), pprofile.NewProfiles()))
		assertFanoutCalls(t, calls)
	})
}

func TestLogsFanoutIsolatesMutatingConsumer(t *testing.T) {
	data := plog.NewLogs()
	data.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("original")
	var mutatingValue, readonlyValue string
	mutating, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
		logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetStr("mutated")
		mutatingValue = logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
		return nil
	}, consumer.WithCapabilities(consumer.Capabilities{MutatesData: true}))
	require.NoError(t, err)
	readonly, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
		readonlyValue = logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
		return nil
	}, consumer.WithCapabilities(consumer.Capabilities{}))
	require.NoError(t, err)
	require.NoError(t, newLogsFanout([]consumer.Logs{mutating, readonly}).ConsumeLogs(context.Background(), data))
	assert.Equal(t, "mutated", mutatingValue)
	assert.Equal(t, "original", readonlyValue)
	assert.Equal(t, "original", data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str())
}

func assertFanoutCalls(t *testing.T, calls int) {
	t.Helper()
	assert.Equal(t, 2, calls)
}

func newLogsTestConsumer(t *testing.T, calls *int) consumer.Logs {
	t.Helper()
	result, err := consumer.NewLogs(func(context.Context, plog.Logs) error { *calls++; return nil })
	require.NoError(t, err)
	return result
}

func newMetricsTestConsumer(t *testing.T, calls *int) consumer.Metrics {
	t.Helper()
	result, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error { *calls++; return nil })
	require.NoError(t, err)
	return result
}

func newTracesTestConsumer(t *testing.T, calls *int) consumer.Traces {
	t.Helper()
	result, err := consumer.NewTraces(func(context.Context, ptrace.Traces) error { *calls++; return nil })
	require.NoError(t, err)
	return result
}

func newProfilesTestConsumer(t *testing.T, calls *int) xconsumer.Profiles {
	t.Helper()
	result, err := xconsumer.NewProfiles(func(context.Context, pprofile.Profiles) error { *calls++; return nil })
	require.NoError(t, err)
	return result
}
