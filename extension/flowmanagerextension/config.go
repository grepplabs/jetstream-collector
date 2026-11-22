package flowmanagerextension

import (
	"fmt"
	"net"

	"go.opentelemetry.io/collector/component"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Config struct {
	Namespace     string         `mapstructure:"namespace"`
	Selector      SelectorConfig `mapstructure:"selector"`
	Kubeconfig    string         `mapstructure:"kubeconfig"`
	DebugEndpoint string         `mapstructure:"debug_endpoint"`
}

type SelectorConfig struct {
	MatchLabels      map[string]string                 `mapstructure:"matchLabels"`
	MatchExpressions []metav1.LabelSelectorRequirement `mapstructure:"matchExpressions"`
}

func (s SelectorConfig) labelSelector() metav1.LabelSelector {
	return metav1.LabelSelector{MatchLabels: s.MatchLabels, MatchExpressions: s.MatchExpressions}
}

var _ component.Config = (*Config)(nil)

func NewDefaultConfig() *Config { return &Config{Namespace: "default"} }

func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}
	if c.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	selector := c.Selector.labelSelector()
	if len(selector.MatchLabels) == 0 && len(selector.MatchExpressions) == 0 {
		return fmt.Errorf("selector must select TelemetryFlow resources")
	}
	if _, err := metav1.LabelSelectorAsSelector(&selector); err != nil {
		return fmt.Errorf("selector: %w", err)
	}
	if c.DebugEndpoint != "" {
		if _, _, err := net.SplitHostPort(c.DebugEndpoint); err != nil {
			return fmt.Errorf("debug_endpoint must be a host:port address: %w", err)
		}
	}
	return nil
}
