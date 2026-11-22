package flowmanagerextension

import (
	"context"
	"strings"
	"testing"
	"time"

	flowv1 "github.com/grepplabs/jetstream-collector/api/telemetryflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMergeFlowsMergesDefinitionsAndDeduplicatesExtensions(t *testing.T) {
	flows := []*flowv1.TelemetryFlow{
		{
			ObjectMeta: objectMeta("orders"),
			Spec: flowv1.TelemetryFlowSpec{Config: flowv1.CollectorConfig{
				Receivers: flowv1.AnyConfig{Object: map[string]any{"otlp/orders": map[string]any{}}},
				Service: flowv1.ServiceConfig{
					Extensions: []string{"health_check"},
					Pipelines:  map[string]flowv1.Pipeline{"logs/orders": {Receivers: []string{"otlp/orders"}, Exporters: []string{"debug/orders"}}},
				},
			}},
		},
		{
			ObjectMeta: objectMeta("payments"),
			Spec: flowv1.TelemetryFlowSpec{Config: flowv1.CollectorConfig{
				Exporters: flowv1.AnyConfig{Object: map[string]any{"debug/orders": map[string]any{}}},
				Service: flowv1.ServiceConfig{
					Extensions: []string{"health_check", "file_storage"},
					Pipelines:  map[string]flowv1.Pipeline{"logs/payments": {Receivers: []string{"otlp/orders"}, Exporters: []string{"debug/orders"}}},
				},
			}},
		},
	}

	merged, err := mergeFlows(flows)
	require.NoError(t, err)
	assert.Len(t, merged.Receivers.Object, 1)
	assert.Len(t, merged.Exporters.Object, 1)
	assert.Len(t, merged.Service.Pipelines, 2)
	assert.Equal(t, "health_check,file_storage", strings.Join(merged.Service.Extensions, ","))
}

func TestMergeFlowsRejectsConflictingComponentIDs(t *testing.T) {
	flows := []*flowv1.TelemetryFlow{
		{ObjectMeta: objectMeta("first"), Spec: flowv1.TelemetryFlowSpec{Config: flowv1.CollectorConfig{Receivers: flowv1.AnyConfig{Object: map[string]any{"otlp/shared": map[string]any{}}}}}},
		{ObjectMeta: objectMeta("second"), Spec: flowv1.TelemetryFlowSpec{Config: flowv1.CollectorConfig{Receivers: flowv1.AnyConfig{Object: map[string]any{"otlp/shared": map[string]any{}}}}}},
	}
	_, err := mergeFlows(flows)
	require.ErrorContains(t, err, `component "otlp/shared"`)
}

func TestValidateLogsPipelineID(t *testing.T) {
	for _, tc := range []struct {
		id      string
		wantErr bool
	}{
		{id: "logs/orders"},
		{id: "metrics/orders", wantErr: true},
		{id: "orders", wantErr: true},
	} {
		t.Run(tc.id, func(t *testing.T) {
			err := validateLogsPipelineID(tc.id)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReconcileLoopDoesNotReconcileAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	e := &flowManagerExtension{reconcileCh: make(chan struct{}, 1)}
	e.reconcileCh <- struct{}{}
	cancel()

	done := make(chan struct{})
	panicCh := make(chan any, 1)
	go func() {
		defer close(done)
		defer func() {
			if recovered := recover(); recovered != nil {
				panicCh <- recovered
			}
		}()
		e.reconcileLoop(ctx)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "reconcile loop did not stop after cancellation")
	}
	select {
	case recovered := <-panicCh:
		assert.Fail(t, "reconcile loop attempted reconciliation after cancellation", "%v", recovered)
	default:
	}
}

func objectMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: "observability", Name: name}
}
