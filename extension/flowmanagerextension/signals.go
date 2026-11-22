package flowmanagerextension

import (
	"context"
	"fmt"
	"slices"
	"sort"

	flowv1 "github.com/grepplabs/jetstream-collector/api/telemetryflow/v1alpha1"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
)

type signalOperations[T any] struct {
	fanout          func([]T) T
	createExporter  func(context.Context, component.ID, component.Config) (component.Component, T, error)
	createProcessor func(context.Context, component.ID, component.Config, T) (component.Component, T, error)
	createReceiver  func(context.Context, component.ID, component.Config, T) (component.Component, error)
}

func buildSignalPipelines[T any](
	ctx context.Context,
	graph *runtimeGraph,
	cfg flowv1.CollectorConfig,
	pipelineIDs []string,
	receivers, processors, exporters decodedComponents,
	usedReceivers, usedProcessors, usedExporters map[component.ID]bool,
	ops signalOperations[T],
) error {
	receiverChildren := map[component.ID][]T{}
	usedPipelineProcessors := map[string]map[component.ID]bool{}
	usedPipelineExporters := map[string]map[component.ID]bool{}

	for _, pipelineID := range pipelineIDs {
		pipeline := cfg.Service.Pipelines[pipelineID]
		if len(pipeline.Receivers) == 0 || len(pipeline.Exporters) == 0 {
			return fmt.Errorf("pipeline %q must have at least one receiver and exporter", pipelineID)
		}

		usedPipelineProcessors[pipelineID] = map[component.ID]bool{}
		usedPipelineExporters[pipelineID] = map[component.ID]bool{}
		pipelineExporters := make([]T, 0, len(pipeline.Exporters))
		for _, rawID := range pipeline.Exporters {
			id, ok := exporters.ids[rawID]
			if !ok {
				return fmt.Errorf("pipeline %q references missing exporter %q", pipelineID, rawID)
			}
			if usedPipelineExporters[pipelineID][id] {
				return fmt.Errorf("pipeline %q references exporter %q more than once", pipelineID, rawID)
			}
			instance, next, err := ops.createExporter(ctx, id, exporters.configs[id])
			if err != nil {
				return fmt.Errorf("create exporter %s for pipeline %s: %w", id, pipelineID, err)
			}
			graph.components = append(graph.components, instance)
			pipelineExporters = append(pipelineExporters, next)
			usedPipelineExporters[pipelineID][id] = true
			usedExporters[id] = true
		}

		next := ops.fanout(pipelineExporters)
		for _, rawID := range slices.Backward(pipeline.Processors) {
			id, ok := processors.ids[rawID]
			if !ok {
				return fmt.Errorf("pipeline %q references missing processor %q", pipelineID, rawID)
			}
			if usedPipelineProcessors[pipelineID][id] {
				return fmt.Errorf("pipeline %q references processor %q more than once", pipelineID, rawID)
			}
			instance, downstream, err := ops.createProcessor(ctx, id, processors.configs[id], next)
			if err != nil {
				return fmt.Errorf("create processor %s for pipeline %s: %w", id, pipelineID, err)
			}
			graph.components = append(graph.components, instance)
			next = downstream
			usedPipelineProcessors[pipelineID][id] = true
			usedProcessors[id] = true
		}

		for _, rawID := range pipeline.Receivers {
			id, ok := receivers.ids[rawID]
			if !ok {
				return fmt.Errorf("pipeline %q references missing receiver %q", pipelineID, rawID)
			}
			receiverChildren[id] = append(receiverChildren[id], next)
			usedReceivers[id] = true
		}
	}

	receiverIDs := make([]component.ID, 0, len(receiverChildren))
	for id := range receiverChildren {
		receiverIDs = append(receiverIDs, id)
	}
	sort.Slice(receiverIDs, func(i, j int) bool { return receiverIDs[i].String() < receiverIDs[j].String() })
	for _, id := range receiverIDs {
		instance, err := ops.createReceiver(ctx, id, receivers.configs[id], ops.fanout(receiverChildren[id]))
		if err != nil {
			return fmt.Errorf("create receiver %s: %w", id, err)
		}
		graph.components = append(graph.components, instance)
	}
	return nil
}

func buildLogsOperations(
	e *flowManagerExtension,
	receivers, processors, exporters decodedComponents,
) signalOperations[consumer.Logs] {
	return signalOperations[consumer.Logs]{
		fanout: newLogsFanout,
		createExporter: func(ctx context.Context, id component.ID, cfg component.Config) (component.Component, consumer.Logs, error) {
			factory := exporters.factories[id.Type()].(exporter.Factory)
			instance, err := factory.CreateLogs(ctx, exporter.Settings{ID: id, TelemetrySettings: e.telemetry, BuildInfo: e.buildInfo}, cfg)
			return instance, instance, err
		},
		createProcessor: func(ctx context.Context, id component.ID, cfg component.Config, next consumer.Logs) (component.Component, consumer.Logs, error) {
			factory := processors.factories[id.Type()].(processor.Factory)
			instance, err := factory.CreateLogs(ctx, processor.Settings{ID: id, TelemetrySettings: e.telemetry, BuildInfo: e.buildInfo}, cfg, next)
			return instance, instance, err
		},
		createReceiver: func(ctx context.Context, id component.ID, cfg component.Config, next consumer.Logs) (component.Component, error) {
			factory := receivers.factories[id.Type()].(receiver.Factory)
			return factory.CreateLogs(ctx, receiver.Settings{ID: id, TelemetrySettings: e.telemetry, BuildInfo: e.buildInfo}, cfg, next)
		},
	}
}

func buildMetricsOperations(
	e *flowManagerExtension,
	receivers, processors, exporters decodedComponents,
) signalOperations[consumer.Metrics] {
	return signalOperations[consumer.Metrics]{
		fanout: newMetricsFanout,
		createExporter: func(ctx context.Context, id component.ID, cfg component.Config) (component.Component, consumer.Metrics, error) {
			factory := exporters.factories[id.Type()].(exporter.Factory)
			instance, err := factory.CreateMetrics(ctx, exporter.Settings{ID: id, TelemetrySettings: e.telemetry, BuildInfo: e.buildInfo}, cfg)
			return instance, instance, err
		},
		createProcessor: func(ctx context.Context, id component.ID, cfg component.Config, next consumer.Metrics) (component.Component, consumer.Metrics, error) {
			factory := processors.factories[id.Type()].(processor.Factory)
			instance, err := factory.CreateMetrics(ctx, processor.Settings{ID: id, TelemetrySettings: e.telemetry, BuildInfo: e.buildInfo}, cfg, next)
			return instance, instance, err
		},
		createReceiver: func(ctx context.Context, id component.ID, cfg component.Config, next consumer.Metrics) (component.Component, error) {
			factory := receivers.factories[id.Type()].(receiver.Factory)
			return factory.CreateMetrics(ctx, receiver.Settings{ID: id, TelemetrySettings: e.telemetry, BuildInfo: e.buildInfo}, cfg, next)
		},
	}
}

func buildTracesOperations(
	e *flowManagerExtension,
	receivers, processors, exporters decodedComponents,
) signalOperations[consumer.Traces] {
	return signalOperations[consumer.Traces]{
		fanout: newTracesFanout,
		createExporter: func(ctx context.Context, id component.ID, cfg component.Config) (component.Component, consumer.Traces, error) {
			factory := exporters.factories[id.Type()].(exporter.Factory)
			instance, err := factory.CreateTraces(ctx, exporter.Settings{ID: id, TelemetrySettings: e.telemetry, BuildInfo: e.buildInfo}, cfg)
			return instance, instance, err
		},
		createProcessor: func(ctx context.Context, id component.ID, cfg component.Config, next consumer.Traces) (component.Component, consumer.Traces, error) {
			factory := processors.factories[id.Type()].(processor.Factory)
			instance, err := factory.CreateTraces(ctx, processor.Settings{ID: id, TelemetrySettings: e.telemetry, BuildInfo: e.buildInfo}, cfg, next)
			return instance, instance, err
		},
		createReceiver: func(ctx context.Context, id component.ID, cfg component.Config, next consumer.Traces) (component.Component, error) {
			factory := receivers.factories[id.Type()].(receiver.Factory)
			return factory.CreateTraces(ctx, receiver.Settings{ID: id, TelemetrySettings: e.telemetry, BuildInfo: e.buildInfo}, cfg, next)
		},
	}
}
