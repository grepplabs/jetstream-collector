package v1alpha1

import (
	"encoding/json"
	"testing"
)

func TestAnyConfigRoundTripAndDeepCopy(t *testing.T) {
	var config AnyConfig
	if err := json.Unmarshal([]byte(`{"actions":[{"key":"team","value":"orders"}]}`), &config); err != nil {
		t.Fatal(err)
	}
	copy := new(AnyConfig)
	config.DeepCopyInto(copy)
	copy.Object["other"] = true
	if _, exists := config.Object["other"]; exists {
		t.Fatal("AnyConfig deep copy shares its map")
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	if _, exists := result["actions"]; !exists {
		t.Fatalf("config was not preserved: %s", encoded)
	}
}

func TestTelemetryFlowDeepCopy(t *testing.T) {
	flow := &TelemetryFlow{Spec: TelemetryFlowSpec{Config: CollectorConfig{
		Receivers: AnyConfig{Object: map[string]any{"otlp/input": map[string]any{"protocols": map[string]any{"grpc": map[string]any{"endpoint": "0.0.0.0:4317"}}}}},
		Service:   ServiceConfig{Extensions: []string{"health_check"}, Pipelines: map[string]Pipeline{"logs/main": {Receivers: []string{"otlp/input"}, Processors: []string{"batch"}, Exporters: []string{"debug"}}}},
	}}}
	copy := flow.DeepCopy()
	copy.Spec.Config.Service.Pipelines["logs/main"].Processors[0] = "filter"
	copy.Spec.Config.Receivers.Object["other"] = map[string]any{}
	if flow.Spec.Config.Service.Pipelines["logs/main"].Processors[0] != "batch" {
		t.Fatal("pipeline slices are shared")
	}
	if _, exists := flow.Spec.Config.Receivers.Object["other"]; exists {
		t.Fatal("component config map is shared")
	}
}
