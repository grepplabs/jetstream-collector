package kubemappingprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestExtractValue(t *testing.T) {
	obj := unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"id": "router-42", "enabled": true, "count": int64(7), "complex": map[string]any{"x": "y"}}, "metadata": map[string]any{"labels": map[string]any{"tenant": "acme"}}}}
	for _, tc := range []struct {
		cfg  ValueConfig
		want string
	}{{ValueConfig{Field: "spec.id"}, "router-42"}, {ValueConfig{Field: "spec.enabled"}, "true"}, {ValueConfig{Field: "spec.count"}, "7"}, {ValueConfig{Label: "tenant"}, "acme"}} {
		got, ok, err := extractValue(obj, tc.cfg)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, tc.want, got)
	}
	_, ok, err := extractValue(obj, ValueConfig{Field: "spec.missing"})
	require.NoError(t, err)
	require.False(t, ok)
	_, _, err = extractValue(obj, ValueConfig{Field: "spec.complex"})
	require.Error(t, err)
}
