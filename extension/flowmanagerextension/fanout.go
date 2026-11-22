// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package flowmanagerextension

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// These fan-out consumers follow the Collector's internal fanoutconsumer
// semantics while remaining in this module, outside the Collector's internal
// package boundary. See the upstream links in README.md.

func newLogsFanout(consumers []consumer.Logs) consumer.Logs {
	if len(consumers) == 1 && !consumers[0].Capabilities().MutatesData {
		return consumers[0]
	}
	fanout := logsFanout{}
	for _, next := range consumers {
		if next.Capabilities().MutatesData {
			fanout.mutable = append(fanout.mutable, next)
		} else {
			fanout.readonly = append(fanout.readonly, next)
		}
	}
	return fanout
}

type logsFanout struct {
	mutable  []consumer.Logs
	readonly []consumer.Logs
}

func (f logsFanout) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: len(f.mutable) > 0 && len(f.readonly) == 0}
}

func (f logsFanout) ConsumeLogs(ctx context.Context, data plog.Logs) error {
	var errs error
	for i, next := range f.mutable {
		if i == len(f.mutable)-1 && len(f.readonly) == 0 && !data.IsReadOnly() {
			errs = errors.Join(errs, next.ConsumeLogs(ctx, data))
		} else {
			errs = errors.Join(errs, next.ConsumeLogs(ctx, cloneLogs(data)))
		}
	}
	if len(f.readonly) > 1 && !data.IsReadOnly() {
		data.MarkReadOnly()
	}
	for _, next := range f.readonly {
		errs = errors.Join(errs, next.ConsumeLogs(ctx, data))
	}
	return errs
}

func cloneLogs(data plog.Logs) plog.Logs {
	clone := plog.NewLogs()
	data.CopyTo(clone)
	return clone
}

func newMetricsFanout(consumers []consumer.Metrics) consumer.Metrics {
	if len(consumers) == 1 && !consumers[0].Capabilities().MutatesData {
		return consumers[0]
	}
	fanout := metricsFanout{}
	for _, next := range consumers {
		if next.Capabilities().MutatesData {
			fanout.mutable = append(fanout.mutable, next)
		} else {
			fanout.readonly = append(fanout.readonly, next)
		}
	}
	return fanout
}

type metricsFanout struct {
	mutable  []consumer.Metrics
	readonly []consumer.Metrics
}

func (f metricsFanout) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: len(f.mutable) > 0 && len(f.readonly) == 0}
}

func (f metricsFanout) ConsumeMetrics(ctx context.Context, data pmetric.Metrics) error {
	var errs error
	for i, next := range f.mutable {
		if i == len(f.mutable)-1 && len(f.readonly) == 0 && !data.IsReadOnly() {
			errs = errors.Join(errs, next.ConsumeMetrics(ctx, data))
		} else {
			errs = errors.Join(errs, next.ConsumeMetrics(ctx, cloneMetrics(data)))
		}
	}
	if len(f.readonly) > 1 && !data.IsReadOnly() {
		data.MarkReadOnly()
	}
	for _, next := range f.readonly {
		errs = errors.Join(errs, next.ConsumeMetrics(ctx, data))
	}
	return errs
}

func cloneMetrics(data pmetric.Metrics) pmetric.Metrics {
	clone := pmetric.NewMetrics()
	data.CopyTo(clone)
	return clone
}

func newTracesFanout(consumers []consumer.Traces) consumer.Traces {
	if len(consumers) == 1 && !consumers[0].Capabilities().MutatesData {
		return consumers[0]
	}
	fanout := tracesFanout{}
	for _, next := range consumers {
		if next.Capabilities().MutatesData {
			fanout.mutable = append(fanout.mutable, next)
		} else {
			fanout.readonly = append(fanout.readonly, next)
		}
	}
	return fanout
}

type tracesFanout struct {
	mutable  []consumer.Traces
	readonly []consumer.Traces
}

func (f tracesFanout) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: len(f.mutable) > 0 && len(f.readonly) == 0}
}

func (f tracesFanout) ConsumeTraces(ctx context.Context, data ptrace.Traces) error {
	var errs error
	for i, next := range f.mutable {
		if i == len(f.mutable)-1 && len(f.readonly) == 0 && !data.IsReadOnly() {
			errs = errors.Join(errs, next.ConsumeTraces(ctx, data))
		} else {
			errs = errors.Join(errs, next.ConsumeTraces(ctx, cloneTraces(data)))
		}
	}
	if len(f.readonly) > 1 && !data.IsReadOnly() {
		data.MarkReadOnly()
	}
	for _, next := range f.readonly {
		errs = errors.Join(errs, next.ConsumeTraces(ctx, data))
	}
	return errs
}

func cloneTraces(data ptrace.Traces) ptrace.Traces {
	clone := ptrace.NewTraces()
	data.CopyTo(clone)
	return clone
}

func newProfilesFanout(consumers []xconsumer.Profiles) xconsumer.Profiles {
	if len(consumers) == 1 && !consumers[0].Capabilities().MutatesData {
		return consumers[0]
	}
	fanout := profilesFanout{}
	for _, next := range consumers {
		if next.Capabilities().MutatesData {
			fanout.mutable = append(fanout.mutable, next)
		} else {
			fanout.readonly = append(fanout.readonly, next)
		}
	}
	return fanout
}

type profilesFanout struct {
	mutable  []xconsumer.Profiles
	readonly []xconsumer.Profiles
}

func (f profilesFanout) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: len(f.mutable) > 0 && len(f.readonly) == 0}
}

func (f profilesFanout) ConsumeProfiles(ctx context.Context, data pprofile.Profiles) error {
	var errs error
	for i, next := range f.mutable {
		if i == len(f.mutable)-1 && len(f.readonly) == 0 && !data.IsReadOnly() {
			errs = errors.Join(errs, next.ConsumeProfiles(ctx, data))
		} else {
			errs = errors.Join(errs, next.ConsumeProfiles(ctx, cloneProfiles(data)))
		}
	}
	if len(f.readonly) > 1 && !data.IsReadOnly() {
		data.MarkReadOnly()
	}
	for _, next := range f.readonly {
		errs = errors.Join(errs, next.ConsumeProfiles(ctx, data))
	}
	return errs
}

func cloneProfiles(data pprofile.Profiles) pprofile.Profiles {
	clone := pprofile.NewProfiles()
	data.CopyTo(clone)
	return clone
}
