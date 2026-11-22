// Package v1alpha1 defines the Kubernetes API for dynamic telemetry flows.
package v1alpha1

import (
	"encoding/json"
	"maps"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	Group   = "telemetry.grepplabs.io"
	Version = "v1alpha1"
	Kind    = "TelemetryFlow"
)

var GroupVersion = schema.GroupVersion{Group: Group, Version: Version}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=tflow
// +kubebuilder:printcolumn:name="Generation",type="integer",JSONPath=".metadata.generation"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// TelemetryFlow describes a set of component configurations and logical
// pipelines. The component implementations come from the Collector distribution.
type TelemetryFlow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +kubebuilder:validation:Required
	Spec   TelemetryFlowSpec   `json:"spec,omitempty"`
	Status TelemetryFlowStatus `json:"status,omitempty"`
}

type TelemetryFlowSpec struct {
	Config CollectorConfig `json:"config"`
}

// CollectorConfig follows the component-and-pipeline structure of Collector
// configuration while leaving component-specific fields opaque.
type CollectorConfig struct {
	// +kubebuilder:validation:Type=object
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Receivers AnyConfig `json:"receivers"`
	// +kubebuilder:validation:Type=object
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Processors AnyConfig `json:"processors,omitempty"`
	// +kubebuilder:validation:Type=object
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Exporters AnyConfig `json:"exporters"`
	// +kubebuilder:validation:Type=object
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Extensions AnyConfig     `json:"extensions,omitempty"`
	Service    ServiceConfig `json:"service"`
}

// AnyConfig is an opaque map whose schema is supplied by the selected
// Collector component factory.
type AnyConfig struct {
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Object map[string]any `json:"-"`
}

// ServiceConfig holds logical pipelines, keyed by signal/name (for example
// logs/orders), like native Collector service.pipelines.
type ServiceConfig struct {
	Extensions []string            `json:"extensions,omitempty"`
	Pipelines  map[string]Pipeline `json:"pipelines"`
}

// Pipeline references configured component IDs in execution order.
type Pipeline struct {
	Receivers  []string `json:"receivers"`
	Processors []string `json:"processors,omitempty"`
	Exporters  []string `json:"exporters"`
}

func (c *AnyConfig) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		c.Object = nil
		return nil
	}
	return json.Unmarshal(data, &c.Object)
}

func (c AnyConfig) MarshalJSON() ([]byte, error) {
	if c.Object == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(c.Object)
}

func (c *AnyConfig) DeepCopyInto(out *AnyConfig) {
	*out = AnyConfig{}
	if c == nil || c.Object == nil {
		return
	}
	encoded, err := json.Marshal(c.Object)
	if err == nil {
		_ = json.Unmarshal(encoded, &out.Object)
		return
	}
	out.Object = make(map[string]any, len(c.Object))
	maps.Copy(out.Object, c.Object)
}

type TelemetryFlowStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type TelemetryFlowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TelemetryFlow `json:"items"`
}

func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &TelemetryFlow{}, &TelemetryFlowList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
