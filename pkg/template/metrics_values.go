package template

import (
	"context"

	client "go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type MetricsValues struct {
	Metrics pmetric.Metrics
}

func (v MetricsValues) ResourceValues(_ context.Context, key string) ([]string, error) {
	values := make([]string, 0)
	for i := 0; i < v.Metrics.ResourceMetrics().Len(); i++ {
		attrs := v.Metrics.ResourceMetrics().At(i).Resource().Attributes()
		if value, ok := attrs.Get(key); ok {
			values = append(values, value.AsString())
		}
	}
	return values, nil
}

func (v MetricsValues) HeaderValues(ctx context.Context, key string) ([]string, error) {
	info := client.FromContext(ctx)
	return info.Metadata.Get(key), nil
}

func (v MetricsValues) TelemetryValues(_ context.Context, key string) ([]string, error) {
	values := make([]string, 0)
	appendAttributes := func(attrs pcommon.Map) {
		if value, ok := attrs.Get(key); ok {
			values = append(values, value.AsString())
		}
	}
	for ri := 0; ri < v.Metrics.ResourceMetrics().Len(); ri++ {
		rm := v.Metrics.ResourceMetrics().At(ri)
		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)
			for mi := 0; mi < sm.Metrics().Len(); mi++ {
				metric := sm.Metrics().At(mi)
				switch metric.Type() {
				case pmetric.MetricTypeEmpty:
					// Empty metrics contain no data points.
				case pmetric.MetricTypeGauge:
					points := metric.Gauge().DataPoints()
					for i := 0; i < points.Len(); i++ {
						appendAttributes(points.At(i).Attributes())
					}
				case pmetric.MetricTypeSum:
					points := metric.Sum().DataPoints()
					for i := 0; i < points.Len(); i++ {
						appendAttributes(points.At(i).Attributes())
					}
				case pmetric.MetricTypeHistogram:
					points := metric.Histogram().DataPoints()
					for i := 0; i < points.Len(); i++ {
						appendAttributes(points.At(i).Attributes())
					}
				case pmetric.MetricTypeExponentialHistogram:
					points := metric.ExponentialHistogram().DataPoints()
					for i := 0; i < points.Len(); i++ {
						appendAttributes(points.At(i).Attributes())
					}
				case pmetric.MetricTypeSummary:
					points := metric.Summary().DataPoints()
					for i := 0; i < points.Len(); i++ {
						appendAttributes(points.At(i).Attributes())
					}
				}
			}
		}
	}
	return values, nil
}
