package flowmanagerextension

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConfigRequiresNamespaceAndSelector(t *testing.T) {
	cases := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{name: "valid", cfg: &Config{Namespace: "observability", Selector: SelectorConfig{MatchLabels: map[string]string{"telemetry.grepplabs.io/collector": "prod"}}}},
		{name: "missing namespace", cfg: &Config{Selector: SelectorConfig{MatchLabels: map[string]string{"app": "collector"}}}, wantErr: true},
		{name: "missing selector", cfg: &Config{Namespace: "observability"}, wantErr: true},
		{name: "invalid expression", cfg: &Config{Namespace: "observability", Selector: SelectorConfig{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: "Invalid"}}}}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			require.Equal(t, tc.wantErr, err != nil)
		})
	}
}
