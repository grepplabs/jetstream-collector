package celpartitionprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type tracesProcessor struct {
	compiled *compiledConfig
	next     consumer.Traces
}

func (p *tracesProcessor) Start(context.Context, component.Host) error { return nil }
func (p *tracesProcessor) Shutdown(context.Context) error              { return nil }
func (p *tracesProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
func (p *tracesProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	batches := make([]ptrace.Traces, 0)
	byKey := make(map[string]int)
	resources := make(map[partitionResourceKey]ptrace.ResourceSpans)
	scopes := make(map[partitionScopeKey]ptrace.ScopeSpans)
	for ri := 0; ri < td.ResourceSpans().Len(); ri++ {
		rs := td.ResourceSpans().At(ri)
		resourceAttrs := rs.Resource().Attributes().AsRaw()
		for si := 0; si < rs.ScopeSpans().Len(); si++ {
			ss := rs.ScopeSpans().At(si)
			for i := 0; i < ss.Spans().Len(); i++ {
				span := ss.Spans().At(i)
				key, err := p.compiled.key(p.compiled.traces, span.Attributes().AsRaw(), resourceAttrs)
				if err != nil {
					if p.compiled.cfg.OnError == OnErrorDrop {
						continue
					}
					return consumererror.NewPermanent(err)
				}
				idx, ok := byKey[key]
				if !ok {
					batches = append(batches, ptrace.NewTraces())
					idx = len(batches) - 1
					byKey[key] = idx
				}
				resourceKey := partitionResourceKey{partition: idx, resource: ri}
				outRS, ok := resources[resourceKey]
				if !ok {
					outRS = batches[idx].ResourceSpans().AppendEmpty()
					rs.Resource().CopyTo(outRS.Resource())
					outRS.SetSchemaUrl(rs.SchemaUrl())
					resources[resourceKey] = outRS
				}
				scopeKey := partitionScopeKey{partition: idx, resource: ri, scope: si}
				outSS, ok := scopes[scopeKey]
				if !ok {
					outSS = outRS.ScopeSpans().AppendEmpty()
					ss.Scope().CopyTo(outSS.Scope())
					outSS.SetSchemaUrl(ss.SchemaUrl())
					scopes[scopeKey] = outSS
				}
				span.CopyTo(outSS.Spans().AppendEmpty())
			}
		}
	}
	for _, batch := range batches {
		if err := p.next.ConsumeTraces(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}
