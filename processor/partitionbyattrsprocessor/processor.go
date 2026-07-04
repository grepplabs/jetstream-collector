package partitionbyattrsprocessor

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

type partitionByAttrsProcessor struct {
	cfg     *Config
	next    consumer.Logs
	keySpec *partitionKeySpec
}

type partitionSource string

const (
	partitionSourceResource  partitionSource = "resource"
	partitionSourceTelemetry partitionSource = "telemetry"
)

func newPartitionByAttrsProcessor(cfg *Config, next consumer.Logs) *partitionByAttrsProcessor {
	return &partitionByAttrsProcessor{
		cfg:     cfg,
		next:    next,
		keySpec: newPartitionKeySpec(cfg),
	}
}

func (p *partitionByAttrsProcessor) Start(context.Context, component.Host) error {
	return nil
}

func (p *partitionByAttrsProcessor) Shutdown(context.Context) error {
	return nil
}

func (p *partitionByAttrsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *partitionByAttrsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	batches, err := p.splitLogs(ld)
	if err != nil {
		return err
	}

	for _, batch := range batches {
		if err := p.next.ConsumeLogs(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

type partitionKeyPart struct {
	source partitionSource
	key    string
}

type partitionKeySpec struct {
	parts []partitionKeyPart
}

func newPartitionKeySpec(cfg *Config) *partitionKeySpec {
	if !cfg.hasPartitionAttrs() {
		return nil
	}

	parts := make([]partitionKeyPart, 0, len(cfg.PartitionBy.Resource)+len(cfg.PartitionBy.Telemetry))
	for _, key := range cfg.PartitionBy.Resource {
		parts = append(parts, partitionKeyPart{source: partitionSourceResource, key: key})
	}
	for _, key := range cfg.PartitionBy.Telemetry {
		parts = append(parts, partitionKeyPart{source: partitionSourceTelemetry, key: key})
	}
	return &partitionKeySpec{parts: parts}
}

func (s *partitionKeySpec) resolve(source *logsPartitionResolver) (string, error) {
	var b strings.Builder
	for i, part := range s.parts {
		if i > 0 {
			b.WriteByte('|')
		}
		value, err := resolvePartitionPart(part, source)
		if err != nil {
			return "", err
		}
		b.WriteString(value)
	}
	return b.String(), nil
}

type logsPartitionResolver struct {
	resourceAttrs  pcommon.Map
	telemetryAttrs pcommon.Map
}

func (r *logsPartitionResolver) resourceValue(key string) (string, error) {
	value, ok := r.resourceAttrs.Get(key)
	if !ok {
		return "", fmt.Errorf("missing %s value for %s", partitionSourceResource, key)
	}
	return strconv.Quote(value.AsString()), nil
}

func (r *logsPartitionResolver) telemetryValue(key string) (string, error) {
	value, ok := r.telemetryAttrs.Get(key)
	if !ok {
		return "", fmt.Errorf("missing %s value for %s", partitionSourceTelemetry, key)
	}
	return strconv.Quote(value.AsString()), nil
}

func resolvePartitionPart(part partitionKeyPart, source *logsPartitionResolver) (string, error) {
	switch part.source {
	case partitionSourceResource:
		return source.resourceValue(part.key)
	case partitionSourceTelemetry:
		return source.telemetryValue(part.key)
	default:
		return "", fmt.Errorf("unsupported partition source %q", part.source)
	}
}

func (p *partitionByAttrsProcessor) splitLogs(ld plog.Logs) ([]plog.Logs, error) {
	if p.keySpec == nil {
		return p.splitUnpartitionedLogs(ld), nil
	}

	return p.partitionLogs(ld)
}

func (p *partitionByAttrsProcessor) splitUnpartitionedLogs(ld plog.Logs) []plog.Logs {
	batches := make([]plog.Logs, 0, ld.ResourceLogs().Len())
	for ri := 0; ri < ld.ResourceLogs().Len(); ri++ {
		resourceLogs := ld.ResourceLogs().At(ri)
		for si := 0; si < resourceLogs.ScopeLogs().Len(); si++ {
			scopeLogs := resourceLogs.ScopeLogs().At(si)
			for li := 0; li < scopeLogs.LogRecords().Len(); li++ {
				logRecord := scopeLogs.LogRecords().At(li)
				batch := plog.NewLogs()
				p.appendRecordToBatch(batch, resourceLogs, scopeLogs, logRecord)
				batches = append(batches, batch)
			}
		}
	}

	return batches
}

func (p *partitionByAttrsProcessor) appendRecordToBatch(outLogs plog.Logs, resourceLogs plog.ResourceLogs, scopeLogs plog.ScopeLogs, logRecord plog.LogRecord) {
	outRL := outLogs.ResourceLogs().AppendEmpty()
	resourceLogs.Resource().CopyTo(outRL.Resource())
	outRL.SetSchemaUrl(resourceLogs.SchemaUrl())

	outSL := outRL.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().CopyTo(outSL.Scope())
	outSL.SetSchemaUrl(scopeLogs.SchemaUrl())

	logRecord.CopyTo(outSL.LogRecords().AppendEmpty())
}

func (p *partitionByAttrsProcessor) partitionLogs(ld plog.Logs) ([]plog.Logs, error) {
	batches := make([]plog.Logs, 0)
	batchByKey := make(map[string]int)

	for ri := 0; ri < ld.ResourceLogs().Len(); ri++ {
		resourceLogs := ld.ResourceLogs().At(ri)
		resourceAttrs := resourceLogs.Resource().Attributes()
		for si := 0; si < resourceLogs.ScopeLogs().Len(); si++ {
			scopeLogs := resourceLogs.ScopeLogs().At(si)
			for li := 0; li < scopeLogs.LogRecords().Len(); li++ {
				logRecord := scopeLogs.LogRecords().At(li)

				key, include, err := p.partitionKey(resourceAttrs, logRecord.Attributes())
				if err != nil {
					return nil, err
				}
				if !include {
					continue
				}

				idx, ok := batchByKey[key]
				if !ok {
					batch := plog.NewLogs()
					p.appendRecordToBatch(batch, resourceLogs, scopeLogs, logRecord)
					batches = append(batches, batch)
					batchByKey[key] = len(batches) - 1
					continue
				}

				p.appendRecordToBatch(batches[idx], resourceLogs, scopeLogs, logRecord)
			}
		}
	}

	return batches, nil
}

func (p *partitionByAttrsProcessor) partitionKey(resourceAttrs pcommon.Map, telemetryAttrs pcommon.Map) (string, bool, error) {
	resolver := &logsPartitionResolver{
		resourceAttrs:  resourceAttrs,
		telemetryAttrs: telemetryAttrs,
	}
	key, err := p.keySpec.resolve(resolver)
	if err != nil {
		if p.cfg.shouldDropOnMissing() {
			return "", false, nil
		}
		return "", false, consumererror.NewPermanent(err)
	}
	return key, true, nil
}
