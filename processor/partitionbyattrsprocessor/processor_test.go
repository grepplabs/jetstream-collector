package partitionbyattrsprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestConfigValidate(t *testing.T) {
	t.Run("normalizes and defaults action", func(t *testing.T) {
		cfg := &Config{
			PartitionBy: PartitionByConfig{
				Resource:  []string{" service.name ", "service.instance.id"},
				Telemetry: []string{" trace.id "},
			},
		}

		require.NoError(t, cfg.Validate())
		require.Equal(t, []string{"service.name", "service.instance.id"}, cfg.PartitionBy.Resource)
		require.Equal(t, []string{"trace.id"}, cfg.PartitionBy.Telemetry)
		require.Equal(t, MissingAttributeActionError, cfg.MissingAttributeAction)
	})

	t.Run("rejects duplicates and invalid actions", func(t *testing.T) {
		cfg := &Config{
			PartitionBy: PartitionByConfig{
				Resource:  []string{"service.name", "service.name"},
				Telemetry: []string{""},
			},
			MissingAttributeAction: "nope",
		}

		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate")
		require.Contains(t, err.Error(), "cannot be empty")
		require.Contains(t, err.Error(), "invalid missing_attribute_action")
	})
}

func TestPartitionKeySpecResolve(t *testing.T) {
	tests := []struct {
		name           string
		parts          []partitionKeyPart
		resourceAttrs  map[string]any
		telemetryAttrs map[string]any
		wantKey        string
		wantErr        string
	}{
		{
			name: "resource and telemetry values are joined",
			parts: []partitionKeyPart{
				{source: partitionSourceResource, key: "service.name"},
				{source: partitionSourceTelemetry, key: "my.resource.id"},
			},
			resourceAttrs:  map[string]any{"service.name": "svc|a"},
			telemetryAttrs: map[string]any{"my.resource.id": "id-1"},
			wantKey:        `"svc|a"|"id-1"`,
		},
		{
			name: "single resource value",
			parts: []partitionKeyPart{
				{source: partitionSourceResource, key: "service.name"},
			},
			resourceAttrs: map[string]any{"service.name": "svc-a"},
			wantKey:       `"svc-a"`,
		},
		{
			name: "two resource values",
			parts: []partitionKeyPart{
				{source: partitionSourceResource, key: "service.name"},
				{source: partitionSourceResource, key: "service.instance.id"},
			},
			resourceAttrs: map[string]any{
				"service.name":        "svc-a",
				"service.instance.id": "inst-1",
			},
			wantKey: `"svc-a"|"inst-1"`,
		},
		{
			name: "two telemetry values",
			parts: []partitionKeyPart{
				{source: partitionSourceTelemetry, key: "trace.id"},
				{source: partitionSourceTelemetry, key: "my.resource.id"},
			},
			telemetryAttrs: map[string]any{
				"trace.id":       "trace-1",
				"my.resource.id": "id-1",
			},
			wantKey: `"trace-1"|"id-1"`,
		},
		{
			name: "resource and two telemetry values",
			parts: []partitionKeyPart{
				{source: partitionSourceResource, key: "service.name"},
				{source: partitionSourceTelemetry, key: "trace.id"},
				{source: partitionSourceTelemetry, key: "my.resource.id"},
			},
			resourceAttrs: map[string]any{"service.name": "svc-a"},
			telemetryAttrs: map[string]any{
				"trace.id":       "trace-1",
				"my.resource.id": "id-1",
			},
			wantKey: `"svc-a"|"trace-1"|"id-1"`,
		},
		{
			name: "missing attribute returns error",
			parts: []partitionKeyPart{
				{source: partitionSourceTelemetry, key: "my.resource.id"},
			},
			wantErr: "missing telemetry value for my.resource.id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := &logsPartitionResolver{
				resourceAttrs:  pcommon.NewMap(),
				telemetryAttrs: pcommon.NewMap(),
			}
			putRawMap(resolver.resourceAttrs, tt.resourceAttrs)
			putRawMap(resolver.telemetryAttrs, tt.telemetryAttrs)

			spec := &partitionKeySpec{parts: tt.parts}
			key, err := spec.resolve(resolver)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				require.Empty(t, key)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantKey, key)
		})
	}
}

func TestPartitionKeyMissingAttributeHandling(t *testing.T) {
	tests := []struct {
		name           string
		missingAction  MissingAttributeAction
		parts          []partitionKeyPart
		resourceAttrs  map[string]any
		telemetryAttrs map[string]any
		wantKey        string
		wantInclude    bool
		wantErr        string
	}{
		{
			name:          "drop missing telemetry value",
			missingAction: MissingAttributeActionDrop,
			parts:         []partitionKeyPart{{source: partitionSourceTelemetry, key: "my.resource.id"}},
			wantInclude:   false,
		},
		{
			name:          "error missing telemetry value",
			missingAction: MissingAttributeActionError,
			parts:         []partitionKeyPart{{source: partitionSourceTelemetry, key: "my.resource.id"}},
			wantInclude:   false,
			wantErr:       "missing telemetry value for my.resource.id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := newPartitionByAttrsProcessor(&Config{MissingAttributeAction: tt.missingAction}, nil)
			proc.keySpec = &partitionKeySpec{parts: tt.parts}

			resourceAttrs := pcommon.NewMap()
			telemetryAttrs := pcommon.NewMap()
			putRawMap(resourceAttrs, tt.resourceAttrs)
			putRawMap(telemetryAttrs, tt.telemetryAttrs)

			key, include, err := proc.partitionKey(resourceAttrs, telemetryAttrs)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.True(t, consumererror.IsPermanent(err))
				require.Contains(t, err.Error(), tt.wantErr)
				require.False(t, include)
				require.Empty(t, key)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantInclude, include)
			require.Equal(t, tt.wantKey, key)
		})
	}
}

func TestPartitionByAttrsProcessorPartitionsByTelemetryAttr(t *testing.T) {
	sink := &consumertest.LogsSink{}
	proc := mustCreateProcessor(t, &Config{
		PartitionBy: PartitionByConfig{
			Telemetry: []string{"my.resource.id"},
		},
	}, sink)

	input := newLogsWithRecords(
		map[string]any{"service.name": "svc-a"},
		[]recordSpec{
			{id: "A", attrs: map[string]any{"my.resource.id": "A"}},
			{id: "A", attrs: map[string]any{"my.resource.id": "A"}},
			{id: "B", attrs: map[string]any{"my.resource.id": "B"}},
		},
	)

	require.NoError(t, proc.ConsumeLogs(context.Background(), input))
	require.Len(t, sink.AllLogs(), 2)
	require.Equal(t, 2, sink.AllLogs()[0].LogRecordCount())
	require.Equal(t, 1, sink.AllLogs()[1].LogRecordCount())
	require.Equal(t, "A", firstRecordID(t, sink.AllLogs()[0]))
	require.Equal(t, "B", firstRecordID(t, sink.AllLogs()[1]))
}

func TestPartitionByAttrsProcessorPartitionsByResourceAttr(t *testing.T) {
	sink := &consumertest.LogsSink{}
	proc := mustCreateProcessor(t, &Config{
		PartitionBy: PartitionByConfig{
			Resource: []string{"service.name"},
		},
	}, sink)

	input := plog.NewLogs()
	rl1 := input.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("service.name", "svc-a")
	sl1 := rl1.ScopeLogs().AppendEmpty()
	lr1 := sl1.LogRecords().AppendEmpty()
	lr1.Attributes().PutStr("id", "1")
	lr1.Body().SetStr("1")

	rl2 := input.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("service.name", "svc-b")
	sl2 := rl2.ScopeLogs().AppendEmpty()
	lr2 := sl2.LogRecords().AppendEmpty()
	lr2.Attributes().PutStr("id", "2")
	lr2.Body().SetStr("2")

	require.NoError(t, proc.ConsumeLogs(context.Background(), input))
	require.Len(t, sink.AllLogs(), 2)
	require.Equal(t, "1", firstRecordID(t, sink.AllLogs()[0]))
	require.Equal(t, "2", firstRecordID(t, sink.AllLogs()[1]))
}

func TestPartitionByAttrsProcessorDropsMissingTelemetryAttr(t *testing.T) {
	sink := &consumertest.LogsSink{}
	proc := mustCreateProcessor(t, &Config{
		PartitionBy: PartitionByConfig{
			Telemetry: []string{"my.resource.id"},
		},
		MissingAttributeAction: MissingAttributeActionDrop,
	}, sink)

	input := newLogsWithRecords(
		map[string]any{"service.name": "svc-a"},
		[]recordSpec{
			{id: "A", attrs: map[string]any{"my.resource.id": "A"}},
			{id: "drop", attrs: map[string]any{}},
		},
	)

	require.NoError(t, proc.ConsumeLogs(context.Background(), input))
	require.Len(t, sink.AllLogs(), 1)
	require.Equal(t, 1, sink.AllLogs()[0].LogRecordCount())
	require.Equal(t, "A", firstRecordID(t, sink.AllLogs()[0]))
}

func TestPartitionByAttrsProcessorErrorsOnMissingTelemetryAttr(t *testing.T) {
	sink := &consumertest.LogsSink{}
	proc := mustCreateProcessor(t, &Config{
		PartitionBy: PartitionByConfig{
			Telemetry: []string{"my.resource.id"},
		},
	}, sink)

	input := newLogsWithRecords(
		map[string]any{"service.name": "svc-a"},
		[]recordSpec{
			{id: "A", attrs: map[string]any{"my.resource.id": "A"}},
			{id: "missing", attrs: map[string]any{}},
		},
	)

	err := proc.ConsumeLogs(context.Background(), input)
	require.Error(t, err)
	require.True(t, consumererror.IsPermanent(err))
	require.Empty(t, sink.AllLogs())
}

func TestPartitionByAttrsProcessorEmptyPartitionBySplitsPerRecord(t *testing.T) {
	sink := &consumertest.LogsSink{}
	proc := mustCreateProcessor(t, &Config{}, sink)

	input := newLogsWithRecords(
		map[string]any{"service.name": "svc-a"},
		[]recordSpec{
			{id: "1", attrs: map[string]any{"id": "1"}},
			{id: "2", attrs: map[string]any{"id": "2"}},
			{id: "3", attrs: map[string]any{"id": "3"}},
		},
	)

	require.NoError(t, proc.ConsumeLogs(context.Background(), input))
	require.Len(t, sink.AllLogs(), 3)
	for i, batch := range sink.AllLogs() {
		require.Equal(t, 1, batch.LogRecordCount(), "batch %d", i)
	}
}

type recordSpec struct {
	id    string
	attrs map[string]any
}

func mustCreateProcessor(t *testing.T, cfg *Config, next *consumertest.LogsSink) *partitionByAttrsProcessor {
	t.Helper()

	factory := NewFactory()
	settings := processortest.NewNopSettings(factory.Type())
	proc, err := factory.CreateLogs(context.Background(), settings, cfg, next)
	require.NoError(t, err)
	return proc.(*partitionByAttrsProcessor)
}

func newLogsWithRecords(resourceAttrs map[string]any, records []recordSpec) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	putRawMap(rl.Resource().Attributes(), resourceAttrs)
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("test")
	for _, spec := range records {
		lr := sl.LogRecords().AppendEmpty()
		putRawMap(lr.Attributes(), spec.attrs)
		lr.Body().SetStr(spec.id)
	}
	return ld
}

func putRawMap(m pcommon.Map, attrs map[string]any) {
	for k, v := range attrs {
		switch value := v.(type) {
		case string:
			m.PutStr(k, value)
		case int:
			m.PutInt(k, int64(value))
		case int64:
			m.PutInt(k, value)
		case bool:
			m.PutBool(k, value)
		default:
			_ = m.FromRaw(map[string]any{k: v})
		}
	}
}

func firstRecordID(t *testing.T, logs plog.Logs) string {
	t.Helper()
	require.Greater(t, logs.ResourceLogs().Len(), 0)
	require.Greater(t, logs.ResourceLogs().At(0).ScopeLogs().Len(), 0)
	require.Greater(t, logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().Len(), 0)
	return logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().AsString()
}
