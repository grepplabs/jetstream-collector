package template

import (
	"context"

	client "go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

type Source interface {
	HeaderValues(context.Context, string) ([]string, error)
	ResourceValues(context.Context, string) ([]string, error)
	TelemetryValues(context.Context, string) ([]string, error)
}

type LogValues struct {
	Logs plog.Logs
}

func (v LogValues) HeaderValues(ctx context.Context, key string) ([]string, error) {
	info := client.FromContext(ctx)
	return info.Metadata.Get(key), nil
}

func (v LogValues) ResourceValues(_ context.Context, key string) ([]string, error) {
	values := make([]string, 0)
	for i := 0; i < v.Logs.ResourceLogs().Len(); i++ {
		attrs := v.Logs.ResourceLogs().At(i).Resource().Attributes()
		attrs.Range(func(attrKey string, attrValue pcommon.Value) bool {
			if attrKey == key {
				values = append(values, attrValue.AsString())
			}
			return true
		})
	}
	return values, nil
}

func (v LogValues) TelemetryValues(_ context.Context, key string) ([]string, error) {
	values := make([]string, 0)
	for i := 0; i < v.Logs.ResourceLogs().Len(); i++ {
		rl := v.Logs.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				attrs := sl.LogRecords().At(k).Attributes()
				attrs.Range(func(attrKey string, attrValue pcommon.Value) bool {
					if attrKey == key {
						values = append(values, attrValue.AsString())
					}
					return true
				})
			}
		}
	}
	return values, nil
}
