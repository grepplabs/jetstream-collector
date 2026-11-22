package flowmanagerextension

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	flowv1 "github.com/grepplabs/jetstream-collector/api/telemetryflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestFlowManagerWatchesSelectedFlows(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset; run task dynamic:test:envtest")
	}
	for _, binary := range []string{"kube-apiserver", "etcd"} {
		if _, err := os.Stat(filepath.Join(assets, binary)); err != nil {
			t.Skipf("envtest binary %q is unavailable in %q", binary, assets)
		}
	}
	crdDir, err := filepath.Abs("../../api/telemetryflow/config/crd/bases")
	require.NoError(t, err)
	environment := &envtest.Environment{BinaryAssetsDirectory: assets, CRDDirectoryPaths: []string{crdDir}}
	restConfig, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, environment.Stop())
	})

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, flowv1.AddToScheme(scheme))
	apiClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, apiClient.Create(ctx, &corev1.Namespace{Name: "observability"}))
	manager := &flowManagerExtension{
		cfg:                &Config{Namespace: "observability", Selector: SelectorConfig{MatchLabels: map[string]string{"collector": "production"}}},
		logger:             zap.NewExample(),
		telemetry:          componenttest.NewNopTelemetrySettings(),
		factories:          defaultFactories(),
		host:               componenttest.NewNopHost(),
		restConfigOverride: restConfig,
		reconcileCh:        make(chan struct{}, 1),
	}
	require.NoError(t, manager.Start(ctx, manager.host))
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		assert.NoError(t, manager.Shutdown(shutdownCtx))
	})

	flow := testTelemetryFlow("orders", map[string]string{"collector": "other"}, "debug/orders")
	require.NoError(t, apiClient.Create(ctx, flow))
	time.Sleep(200 * time.Millisecond)
	flows, err := manager.listFlows(ctx)
	require.NoError(t, err)
	assert.Empty(t, flows, "flows with a non-matching label should not be selected")

	flow.Labels["collector"] = "production"
	require.NoError(t, apiClient.Update(ctx, flow))
	require.Eventually(t, func() bool {
		active := manager.active.Load()
		flows, err := manager.listFlows(ctx)
		return err == nil && len(flows) == 1 && active != nil && len(active.config.Service.Pipelines) == 1
	}, 10*time.Second, 25*time.Millisecond)

	delete(flow.Spec.Config.Exporters.Object, "debug/orders")
	flow.Spec.Config.Exporters.Object["debug/updated"] = map[string]any{}
	flow.Spec.Config.Service.Pipelines["logs/orders"] = flowv1.Pipeline{Receivers: []string{"nop/orders"}, Exporters: []string{"debug/updated"}}
	require.NoError(t, apiClient.Update(ctx, flow))
	require.Eventually(t, func() bool {
		active := manager.active.Load()
		flows, err := manager.listFlows(ctx)
		return err == nil && len(flows) == 1 && active != nil && active.config.Service.Pipelines["logs/orders"].Exporters[0] == "debug/updated" && strings.Contains(flows[0].Spec.Config.Service.Pipelines["logs/orders"].Exporters[0], "debug/updated")
	}, 10*time.Second, 25*time.Millisecond)

	require.NoError(t, apiClient.Delete(ctx, flow))
	require.Eventually(t, func() bool {
		active := manager.active.Load()
		flows, err := manager.listFlows(ctx)
		return err == nil && len(flows) == 0 && active != nil && len(active.config.Service.Pipelines) == 0
	}, 10*time.Second, 25*time.Millisecond)
}

func testTelemetryFlow(name string, labels map[string]string, exporterID string) *flowv1.TelemetryFlow {
	return &flowv1.TelemetryFlow{
		Namespace: "observability", Name: name, Labels: labels,
		Spec: flowv1.TelemetryFlowSpec{Config: flowv1.CollectorConfig{
			Receivers: flowv1.AnyConfig{Object: map[string]any{"nop/orders": map[string]any{}}},
			Exporters: flowv1.AnyConfig{Object: map[string]any{exporterID: map[string]any{}}},
			Service: flowv1.ServiceConfig{Pipelines: map[string]flowv1.Pipeline{
				"logs/orders": {Receivers: []string{"nop/orders"}, Exporters: []string{exporterID}},
			}},
		}},
	}
}
