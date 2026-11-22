package flowmanagerextension

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strings"
	"sync"

	flowv1 "github.com/grepplabs/jetstream-collector/api/telemetryflow/v1alpha1"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"
)

const (
	signalLogs    = "logs"
	signalMetrics = "metrics"
	signalTraces  = "traces"
)

type runtimeGraph struct {
	config       flowv1.CollectorConfig
	components   []component.Component // start in order, shut down in reverse order
	extensions   map[component.ID]component.Component
	started      int
	shutdownOnce sync.Once
}

func (e *flowManagerExtension) reconcileLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.reconcileCh:
		}
		if ctx.Err() != nil {
			return
		}
		if err := e.reconcile(ctx); err != nil {
			e.logger.Error("Could not apply TelemetryFlow update; active graph was preserved or restored", zap.Error(err))
		}
	}
}

func (e *flowManagerExtension) reconcile(ctx context.Context) error {
	flows, err := e.listFlows(ctx)
	if err != nil {
		return err
	}
	desired, err := mergeFlows(flows)
	if err != nil {
		return err
	}
	old := e.active.Load()
	if old != nil && reflect.DeepEqual(old.config, desired) {
		return nil
	}
	candidate, err := e.buildGraph(ctx, desired)
	if err != nil {
		return err
	}
	if old != nil {
		if err := old.shutdown(ctx); err != nil {
			e.logger.Warn("Could not cleanly stop previous flow graph", zap.Error(err))
		}
	}
	if err := candidate.start(ctx, e.host); err != nil {
		_ = candidate.shutdown(ctx)
		if old == nil {
			e.active.Store(nil)
			return fmt.Errorf("start new graph: %w", err)
		}
		restored, restoreErr := e.buildGraph(ctx, old.config)
		if restoreErr == nil {
			restoreErr = restored.start(ctx, e.host)
		}
		if restoreErr != nil {
			e.active.Store(nil)
			return errors.Join(fmt.Errorf("start new graph: %w", err), fmt.Errorf("restore previous graph: %w", restoreErr))
		}
		e.active.Store(restored)
		return fmt.Errorf("start new graph: %w", err)
	}
	e.active.Store(candidate)
	e.logger.Info("Activated TelemetryFlow graph", zap.Int("flows", len(flows)), zap.Int("components", len(candidate.components)))
	return nil
}

func mergeFlows(flows []*flowv1.TelemetryFlow) (flowv1.CollectorConfig, error) {
	sort.Slice(flows, func(i, j int) bool {
		if flows[i].Namespace == flows[j].Namespace {
			return flows[i].Name < flows[j].Name
		}
		return flows[i].Namespace < flows[j].Namespace
	})
	merged := flowv1.CollectorConfig{Receivers: flowv1.AnyConfig{Object: map[string]any{}}, Processors: flowv1.AnyConfig{Object: map[string]any{}}, Exporters: flowv1.AnyConfig{Object: map[string]any{}}, Extensions: flowv1.AnyConfig{Object: map[string]any{}}, Service: flowv1.ServiceConfig{Pipelines: map[string]flowv1.Pipeline{}}}
	seenExtensionRefs := map[string]bool{}
	for _, flow := range flows {
		key := flow.Namespace + "/" + flow.Name
		for _, entry := range []struct {
			name string
			src  map[string]any
			dst  map[string]any
		}{
			{name: "receivers", src: flow.Spec.Config.Receivers.Object, dst: merged.Receivers.Object},
			{name: "processors", src: flow.Spec.Config.Processors.Object, dst: merged.Processors.Object},
			{name: "exporters", src: flow.Spec.Config.Exporters.Object, dst: merged.Exporters.Object},
			{name: "extensions", src: flow.Spec.Config.Extensions.Object, dst: merged.Extensions.Object},
		} {
			for id, config := range entry.src {
				if _, exists := entry.dst[id]; exists {
					return flowv1.CollectorConfig{}, fmt.Errorf("flows %s and another selected flow both define %s component %q", key, entry.name, id)
				}
				entry.dst[id] = config
			}
		}
		for id, pipeline := range flow.Spec.Config.Service.Pipelines {
			if _, exists := merged.Service.Pipelines[id]; exists {
				return flowv1.CollectorConfig{}, fmt.Errorf("flows %s and another selected flow both define pipeline %q", key, id)
			}
			merged.Service.Pipelines[id] = pipeline
		}
		for _, extensionID := range flow.Spec.Config.Service.Extensions {
			if !seenExtensionRefs[extensionID] {
				merged.Service.Extensions = append(merged.Service.Extensions, extensionID)
				seenExtensionRefs[extensionID] = true
			}
		}
	}
	return merged, nil
}

func (e *flowManagerExtension) buildGraph(ctx context.Context, cfg flowv1.CollectorConfig) (*runtimeGraph, error) {
	graph := &runtimeGraph{config: cfg, extensions: map[component.ID]component.Component{}}
	configuredExtensions, err := decodeComponents(cfg.Extensions.Object, e.factories.extensions)
	if err != nil {
		return nil, fmt.Errorf("extensions: %w", err)
	}
	usedExtensions := map[component.ID]bool{}
	for _, rawID := range cfg.Service.Extensions {
		id, ok := configuredExtensions.ids[rawID]
		if !ok {
			return nil, fmt.Errorf("service.extensions references missing extension %q", rawID)
		}
		factory := configuredExtensions.factories[id.Type()].(extension.Factory)
		instance, err := factory.Create(ctx, extension.Settings{ID: id, TelemetrySettings: e.telemetry, BuildInfo: e.buildInfo}, configuredExtensions.configs[id])
		if err != nil {
			return nil, fmt.Errorf("create extension %s: %w", id, err)
		}
		graph.extensions[id] = instance
		graph.components = append(graph.components, instance)
		usedExtensions[id] = true
	}
	if err := rejectUnused("extension", configuredExtensions.ids, usedExtensions); err != nil {
		return nil, err
	}
	if len(cfg.Service.Pipelines) == 0 {
		return graph, nil
	}
	configuredReceivers, err := decodeComponents(cfg.Receivers.Object, e.factories.receivers)
	if err != nil {
		return nil, fmt.Errorf("receivers: %w", err)
	}
	configuredProcessors, err := decodeComponents(cfg.Processors.Object, e.factories.processors)
	if err != nil {
		return nil, fmt.Errorf("processors: %w", err)
	}
	configuredExporters, err := decodeComponents(cfg.Exporters.Object, e.factories.exporters)
	if err != nil {
		return nil, fmt.Errorf("exporters: %w", err)
	}
	if _, err := graph.host(e.host); err != nil {
		return nil, err
	}

	usedReceivers, usedProcessors, usedExporters := map[component.ID]bool{}, map[component.ID]bool{}, map[component.ID]bool{}
	pipelineIDs := make([]string, 0, len(cfg.Service.Pipelines))
	for id := range cfg.Service.Pipelines {
		pipelineIDs = append(pipelineIDs, id)
	}
	sort.Strings(pipelineIDs)
	pipelinesBySignal := map[string][]string{
		signalLogs:    {},
		signalMetrics: {},
		signalTraces:  {},
	}
	for _, pipelineID := range pipelineIDs {
		signal, err := pipelineSignal(pipelineID)
		if err != nil {
			return nil, err
		}
		pipelinesBySignal[signal] = append(pipelinesBySignal[signal], pipelineID)
	}
	if err := buildSignalPipelines(ctx, graph, cfg, pipelinesBySignal[signalLogs], configuredReceivers, configuredProcessors, configuredExporters, usedReceivers, usedProcessors, usedExporters, buildLogsOperations(e, configuredReceivers, configuredProcessors, configuredExporters)); err != nil {
		return nil, err
	}
	if err := buildSignalPipelines(ctx, graph, cfg, pipelinesBySignal[signalMetrics], configuredReceivers, configuredProcessors, configuredExporters, usedReceivers, usedProcessors, usedExporters, buildMetricsOperations(e, configuredReceivers, configuredProcessors, configuredExporters)); err != nil {
		return nil, err
	}
	if err := buildSignalPipelines(ctx, graph, cfg, pipelinesBySignal[signalTraces], configuredReceivers, configuredProcessors, configuredExporters, usedReceivers, usedProcessors, usedExporters, buildTracesOperations(e, configuredReceivers, configuredProcessors, configuredExporters)); err != nil {
		return nil, err
	}
	if err := rejectUnused("receiver", configuredReceivers.ids, usedReceivers); err != nil {
		return nil, err
	}
	if err := rejectUnused("processor", configuredProcessors.ids, usedProcessors); err != nil {
		return nil, err
	}
	if err := rejectUnused("exporter", configuredExporters.ids, usedExporters); err != nil {
		return nil, err
	}
	return graph, nil
}

type decodedComponents struct {
	ids       map[string]component.ID
	configs   map[component.ID]component.Config
	factories map[component.Type]component.Factory
}

func decodeComponents[F component.Factory](raw map[string]any, registry map[component.Type]F) (decodedComponents, error) {
	result := decodedComponents{ids: map[string]component.ID{}, configs: map[component.ID]component.Config{}, factories: map[component.Type]component.Factory{}}
	for rawID, value := range raw {
		var id component.ID
		if err := id.UnmarshalText([]byte(rawID)); err != nil {
			return result, fmt.Errorf("invalid component ID %q: %w", rawID, err)
		}
		factory, ok := registry[id.Type()]
		if !ok {
			return result, fmt.Errorf("component type %q is not in the dynamic factory allowlist", id.Type())
		}
		configMap, ok := value.(map[string]any)
		if !ok && value != nil {
			return result, fmt.Errorf("config for %s must be an object", id)
		}
		if configMap == nil {
			configMap = map[string]any{}
		}
		config := factory.CreateDefaultConfig()
		if err := confmap.NewFromStringMap(configMap).Unmarshal(config); err != nil {
			return result, fmt.Errorf("decode config for %s: %w", id, err)
		}
		if validator, ok := config.(interface{ Validate() error }); ok {
			if err := validator.Validate(); err != nil {
				return result, fmt.Errorf("validate config for %s: %w", id, err)
			}
		}
		result.ids[rawID] = id
		result.configs[id] = config
		result.factories[id.Type()] = factory
	}
	return result, nil
}

func validateLogsPipelineID(id string) error {
	signal, err := pipelineSignal(id)
	if err != nil {
		return err
	}
	if signal != signalLogs {
		return fmt.Errorf("pipeline %q uses unsupported signal %q; only logs are supported", id, signal)
	}
	return nil
}

func pipelineSignal(id string) (string, error) {
	signal, name, hasName := strings.Cut(id, "/")
	if !hasName || signal == "" || name == "" {
		return "", fmt.Errorf("pipeline ID %q must use signal/name form", id)
	}
	supportedSignals := map[string]struct{}{
		signalLogs:    {},
		signalMetrics: {},
		signalTraces:  {},
	}
	if _, ok := supportedSignals[signal]; ok {
		return signal, nil
	}
	return "", fmt.Errorf("pipeline %q uses unsupported signal %q; supported signals are %s, %s, and %s", id, signal, signalLogs, signalMetrics, signalTraces)
}

func rejectUnused(kind string, configured map[string]component.ID, used map[component.ID]bool) error {
	for raw, id := range configured {
		if !used[id] {
			return fmt.Errorf("configured %s %q is not referenced by a pipeline", kind, raw)
		}
	}
	return nil
}

func (g *runtimeGraph) start(ctx context.Context, host component.Host) error {
	childHost, err := g.host(host)
	if err != nil {
		return err
	}
	for _, component := range g.components {
		if err := component.Start(ctx, childHost); err != nil {
			return fmt.Errorf("start component: %w", err)
		}
		g.started++
	}
	return nil
}

func (g *runtimeGraph) host(base component.Host) (component.Host, error) {
	static := base.GetExtensions()
	all := make(map[component.ID]component.Component, len(static)+len(g.extensions))
	maps.Copy(all, static)
	for id, ext := range g.extensions {
		if _, exists := all[id]; exists {
			return nil, fmt.Errorf("flow extension %q conflicts with a bootstrap extension", id)
		}
		all[id] = ext
	}
	return extensionHost{Host: base, extensions: all}, nil
}

type extensionHost struct {
	component.Host
	extensions map[component.ID]component.Component
}

func (h extensionHost) GetExtensions() map[component.ID]component.Component { return h.extensions }

func (g *runtimeGraph) shutdown(ctx context.Context) error {
	var errs error
	g.shutdownOnce.Do(func() {
		for i := g.started - 1; i >= 0; i-- {
			errs = errors.Join(errs, g.components[i].Shutdown(ctx))
		}
		g.started = 0
	})
	return errs
}
