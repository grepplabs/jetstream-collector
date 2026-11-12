package celpartitionprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
)

type logsProcessor struct {
	compiled *compiledConfig
	next     consumer.Logs
}

func (p *logsProcessor) Start(context.Context, component.Host) error { return nil }
func (p *logsProcessor) Shutdown(context.Context) error              { return nil }
func (p *logsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
func (p *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	batches := make([]plog.Logs, 0)
	byKey := make(map[string]int)
	resources := make(map[partitionResourceKey]plog.ResourceLogs)
	scopes := make(map[partitionScopeKey]plog.ScopeLogs)
	for ri := 0; ri < ld.ResourceLogs().Len(); ri++ {
		rl := ld.ResourceLogs().At(ri)
		resourceAttrs := rl.Resource().Attributes().AsRaw()
		for si := 0; si < rl.ScopeLogs().Len(); si++ {
			sl := rl.ScopeLogs().At(si)
			for li := 0; li < sl.LogRecords().Len(); li++ {
				record := sl.LogRecords().At(li)
				key, err := p.compiled.key(p.compiled.logs, record.Attributes().AsRaw(), resourceAttrs)
				if err != nil {
					if p.compiled.cfg.OnError == OnErrorDrop {
						continue
					}
					return consumererror.NewPermanent(err)
				}
				idx, ok := byKey[key]
				if !ok {
					batches = append(batches, plog.NewLogs())
					idx = len(batches) - 1
					byKey[key] = idx
				}
				resourceKey := partitionResourceKey{partition: idx, resource: ri}
				outRL, ok := resources[resourceKey]
				if !ok {
					outRL = batches[idx].ResourceLogs().AppendEmpty()
					rl.Resource().CopyTo(outRL.Resource())
					outRL.SetSchemaUrl(rl.SchemaUrl())
					resources[resourceKey] = outRL
				}
				scopeKey := partitionScopeKey{partition: idx, resource: ri, scope: si}
				outSL, ok := scopes[scopeKey]
				if !ok {
					outSL = outRL.ScopeLogs().AppendEmpty()
					sl.Scope().CopyTo(outSL.Scope())
					outSL.SetSchemaUrl(sl.SchemaUrl())
					scopes[scopeKey] = outSL
				}
				record.CopyTo(outSL.LogRecords().AppendEmpty())
			}
		}
	}
	for _, batch := range batches {
		if err := p.next.ConsumeLogs(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

type partitionResourceKey struct {
	partition int
	resource  int
}

type partitionScopeKey struct {
	partition int
	resource  int
	scope     int
}
