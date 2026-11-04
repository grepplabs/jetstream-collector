package celpartitionprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type captureTracesConsumer struct{ batches []ptrace.Traces }

func (c *captureTracesConsumer) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (c *captureTracesConsumer) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	c.batches = append(c.batches, td)
	return nil
}

func TestTracesPartitionAndDropEvaluationErrors(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.PartitionBy.Traces = []string{"span.attributes['tenant']"}
	cfg.OnError = OnErrorDrop
	compiled, err := compileConfig(cfg)
	require.NoError(t, err)
	next := &captureTracesConsumer{}
	proc := &tracesProcessor{compiled: compiled, next: next}
	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
	for _, tenant := range []string{"a", "b", "a", ""} {
		span := ss.Spans().AppendEmpty()
		if tenant != "" {
			span.Attributes().PutStr("tenant", tenant)
		}
	}
	require.NoError(t, proc.ConsumeTraces(context.Background(), td))
	require.Len(t, next.batches, 2)
	require.ElementsMatch(t, []int{2, 1}, []int{countSpans(next.batches[0]), countSpans(next.batches[1])})
}

func countSpans(td ptrace.Traces) int {
	total := 0
	for ri := 0; ri < td.ResourceSpans().Len(); ri++ {
		rs := td.ResourceSpans().At(ri)
		for si := 0; si < rs.ScopeSpans().Len(); si++ {
			total += rs.ScopeSpans().At(si).Spans().Len()
		}
	}
	return total
}
