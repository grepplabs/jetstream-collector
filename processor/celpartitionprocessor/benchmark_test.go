package celpartitionprocessor

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const partitionBenchmarkItems = 256

func BenchmarkPartitionLogs(b *testing.B) {
	for name, expressions := range partitionBenchmarkExpressions("record") {
		b.Run(name, func(b *testing.B) {
			compiled := benchmarkCompiledConfig(b, expressions, nil, nil)
			processor := &logsProcessor{compiled: compiled, next: benchmarkLogsConsumer{}}
			logs := benchmarkLogs()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := processor.ConsumeLogs(context.Background(), logs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPartitionMetrics(b *testing.B) {
	for name, expressions := range partitionBenchmarkExpressions("datapoint") {
		b.Run(name, func(b *testing.B) {
			compiled := benchmarkCompiledConfig(b, nil, expressions, nil)
			processor := &metricsProcessor{compiled: compiled, next: benchmarkMetricsConsumer{}}
			metrics := benchmarkMetrics()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := processor.ConsumeMetrics(context.Background(), metrics); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPartitionTraces(b *testing.B) {
	for name, expressions := range partitionBenchmarkExpressions("span") {
		b.Run(name, func(b *testing.B) {
			compiled := benchmarkCompiledConfig(b, nil, nil, expressions)
			processor := &tracesProcessor{compiled: compiled, next: benchmarkTracesConsumer{}}
			traces := benchmarkTraces()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := processor.ConsumeTraces(context.Background(), traces); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// The variants cover a single tenant key, the requested get_attr syntax,
// multiple item attributes, and combining an item key with a resource key.
func partitionBenchmarkExpressions(item string) map[string][]string {
	return map[string][]string{
		"one_attribute": {item + ".attributes['example.tenant.id']"},
		"get_attr":      {item + ".attributes.get_attr('example.tenant.id')"},
		"two_attributes": {
			item + ".attributes.get_attr('example.tenant.id')",
			item + ".attributes.get_attr('example.visibility')",
		},
		"item_and_resource": {
			item + ".attributes.get_attr('example.tenant.id')",
			item + ".resource.attributes.get_attr('service.name')",
		},
	}
}

func benchmarkCompiledConfig(b *testing.B, logs, metrics, traces []string) *compiledConfig {
	b.Helper()
	cfg := NewDefaultConfig()
	cfg.PartitionBy = PartitionByConfig{Logs: logs, Metrics: metrics, Traces: traces}
	compiled, err := compileConfig(cfg)
	if err != nil {
		b.Fatal(err)
	}
	return compiled
}

func benchmarkLogs() plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "checkout")
	sl := rl.ScopeLogs().AppendEmpty()
	for i := range partitionBenchmarkItems {
		record := sl.LogRecords().AppendEmpty()
		record.Attributes().PutStr("example.tenant.id", fmt.Sprintf("tenant-%d", i%16))
		record.Attributes().PutStr("example.visibility", fmt.Sprintf("visibility-%d", i%4))
	}
	return logs
}

func benchmarkMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "checkout")
	metric := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("requests")
	points := metric.SetEmptyGauge().DataPoints()
	for i := range partitionBenchmarkItems {
		point := points.AppendEmpty()
		point.Attributes().PutStr("example.tenant.id", fmt.Sprintf("tenant-%d", i%16))
		point.Attributes().PutStr("example.visibility", fmt.Sprintf("visibility-%d", i%4))
	}
	return metrics
}

func benchmarkTraces() ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := range partitionBenchmarkItems {
		span := ss.Spans().AppendEmpty()
		span.Attributes().PutStr("example.tenant.id", fmt.Sprintf("tenant-%d", i%16))
		span.Attributes().PutStr("example.visibility", fmt.Sprintf("visibility-%d", i%4))
	}
	return traces
}

type benchmarkLogsConsumer struct{}

func (benchmarkLogsConsumer) Capabilities() consumer.Capabilities          { return consumer.Capabilities{} }
func (benchmarkLogsConsumer) ConsumeLogs(context.Context, plog.Logs) error { return nil }

type benchmarkMetricsConsumer struct{}

func (benchmarkMetricsConsumer) Capabilities() consumer.Capabilities                   { return consumer.Capabilities{} }
func (benchmarkMetricsConsumer) ConsumeMetrics(context.Context, pmetric.Metrics) error { return nil }

type benchmarkTracesConsumer struct{}

func (benchmarkTracesConsumer) Capabilities() consumer.Capabilities                { return consumer.Capabilities{} }
func (benchmarkTracesConsumer) ConsumeTraces(context.Context, ptrace.Traces) error { return nil }
