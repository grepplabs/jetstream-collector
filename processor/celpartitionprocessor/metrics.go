package celpartitionprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type metricsProcessor struct {
	compiled *compiledConfig
	next     consumer.Metrics
}

func (p *metricsProcessor) Start(context.Context, component.Host) error { return nil }
func (p *metricsProcessor) Shutdown(context.Context) error              { return nil }
func (p *metricsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
func (p *metricsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	batches := make([]pmetric.Metrics, 0)
	byKey := make(map[string]int)
	for ri := 0; ri < md.ResourceMetrics().Len(); ri++ {
		rm := md.ResourceMetrics().At(ri)
		resourceAttrs := rm.Resource().Attributes().AsRaw()
		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)
			for mi := 0; mi < sm.Metrics().Len(); mi++ {
				metric := sm.Metrics().At(mi)
				outByKey := make(map[string]pmetric.Metric)
				visit := func(attrs map[string]any, copyPoint func(pmetric.Metric)) error {
					key, err := p.compiled.key(p.compiled.metrics, attrs, resourceAttrs)
					if err != nil {
						if p.compiled.cfg.OnError == OnErrorDrop {
							return nil
						}
						return consumererror.NewPermanent(err)
					}
					idx, ok := byKey[key]
					if !ok {
						batches = append(batches, pmetric.NewMetrics())
						idx = len(batches) - 1
						byKey[key] = idx
					}
					out, ok := outByKey[key]
					if !ok {
						outRM := batches[idx].ResourceMetrics().AppendEmpty()
						rm.Resource().CopyTo(outRM.Resource())
						outRM.SetSchemaUrl(rm.SchemaUrl())
						outSM := outRM.ScopeMetrics().AppendEmpty()
						sm.Scope().CopyTo(outSM.Scope())
						outSM.SetSchemaUrl(sm.SchemaUrl())
						out = outSM.Metrics().AppendEmpty()
						out.SetName(metric.Name())
						out.SetDescription(metric.Description())
						out.SetUnit(metric.Unit())
						initMetricData(out, metric)
						outByKey[key] = out
					}
					copyPoint(out)
					return nil
				}
				var err error
				switch metric.Type() {
				case pmetric.MetricTypeEmpty:
					// Empty metrics contain no data points to partition.
				case pmetric.MetricTypeGauge:
					points := metric.Gauge().DataPoints()
					for i := 0; i < points.Len(); i++ {
						pt := points.At(i)
						err = visit(pt.Attributes().AsRaw(), func(m pmetric.Metric) { pt.CopyTo(m.Gauge().DataPoints().AppendEmpty()) })
						if err != nil {
							return err
						}
					}
				case pmetric.MetricTypeSum:
					points := metric.Sum().DataPoints()
					for i := 0; i < points.Len(); i++ {
						pt := points.At(i)
						err = visit(pt.Attributes().AsRaw(), func(m pmetric.Metric) { pt.CopyTo(m.Sum().DataPoints().AppendEmpty()) })
						if err != nil {
							return err
						}
					}
				case pmetric.MetricTypeHistogram:
					points := metric.Histogram().DataPoints()
					for i := 0; i < points.Len(); i++ {
						pt := points.At(i)
						err = visit(pt.Attributes().AsRaw(), func(m pmetric.Metric) { pt.CopyTo(m.Histogram().DataPoints().AppendEmpty()) })
						if err != nil {
							return err
						}
					}
				case pmetric.MetricTypeExponentialHistogram:
					points := metric.ExponentialHistogram().DataPoints()
					for i := 0; i < points.Len(); i++ {
						pt := points.At(i)
						err = visit(pt.Attributes().AsRaw(), func(m pmetric.Metric) { pt.CopyTo(m.ExponentialHistogram().DataPoints().AppendEmpty()) })
						if err != nil {
							return err
						}
					}
				case pmetric.MetricTypeSummary:
					points := metric.Summary().DataPoints()
					for i := 0; i < points.Len(); i++ {
						pt := points.At(i)
						err = visit(pt.Attributes().AsRaw(), func(m pmetric.Metric) { pt.CopyTo(m.Summary().DataPoints().AppendEmpty()) })
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	for _, batch := range batches {
		if err := p.next.ConsumeMetrics(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func initMetricData(out, source pmetric.Metric) {
	switch source.Type() {
	case pmetric.MetricTypeEmpty:
		// Empty metrics have no data point storage.
	case pmetric.MetricTypeGauge:
		out.SetEmptyGauge()
	case pmetric.MetricTypeSum:
		out.SetEmptySum().SetAggregationTemporality(source.Sum().AggregationTemporality())
		out.Sum().SetIsMonotonic(source.Sum().IsMonotonic())
	case pmetric.MetricTypeHistogram:
		out.SetEmptyHistogram().SetAggregationTemporality(source.Histogram().AggregationTemporality())
	case pmetric.MetricTypeExponentialHistogram:
		out.SetEmptyExponentialHistogram().SetAggregationTemporality(source.ExponentialHistogram().AggregationTemporality())
	case pmetric.MetricTypeSummary:
		out.SetEmptySummary()
	}
}
