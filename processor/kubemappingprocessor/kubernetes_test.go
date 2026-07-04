package kubemappingprocessor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

func TestGetRESTConfigOverridesContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	err := os.WriteFile(path, []byte(`apiVersion: v1
kind: Config
clusters:
  - name: first
    cluster:
      server: https://first.example.test
  - name: second
    cluster:
      server: https://second.example.test
contexts:
  - name: first
    context:
      cluster: first
      user: test
  - name: second
    context:
      cluster: second
      user: test
current-context: first
users:
  - name: test
    user:
      token: test
`), 0o600)
	require.NoError(t, err)

	cfg, err := getRESTConfig(KubeConfig{Kubeconfig: path, Context: "second"})
	require.NoError(t, err)
	require.Equal(t, "https://second.example.test", cfg.Host)
}

func TestCacheObjectsGroupsNamespacesAndIgnoresMappingSelectors(t *testing.T) {
	groupVersion := schema.GroupVersion{Group: "routing.example.io", Version: "v1"}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{groupVersion})
	mapper.Add(groupVersion.WithKind("Router"), meta.RESTScopeNamespace)
	mapper.Add(groupVersion.WithKind("ClusterRouter"), meta.RESTScopeRoot)

	objects, err := cacheObjects(mapper, []MappingConfig{
		{Resource: ResourceConfig{Group: groupVersion.Group, Version: groupVersion.Version, Kind: "Router", Namespace: "first"}, Selector: SelectorConfig{Labels: []string{"app=first"}}},
		{Resource: ResourceConfig{Group: groupVersion.Group, Version: groupVersion.Version, Kind: "Router", Namespace: "second"}, Selector: SelectorConfig{Labels: []string{"app=second"}}},
		{Resource: ResourceConfig{Group: groupVersion.Group, Version: groupVersion.Version, Kind: "ClusterRouter"}, Selector: SelectorConfig{Fields: []string{"metadata.name=router"}}},
	})
	require.NoError(t, err)
	require.Len(t, objects, 2)
	found := make(map[schema.GroupVersionKind]cache.ByObject, len(objects))
	for object, options := range objects {
		found[object.GetObjectKind().GroupVersionKind()] = options
	}
	require.Contains(t, found[groupVersion.WithKind("Router")].Namespaces, "first")
	require.Contains(t, found[groupVersion.WithKind("Router")].Namespaces, "second")
	require.Nil(t, found[groupVersion.WithKind("ClusterRouter")].Namespaces)
}

func TestConfiguredFieldIndexesAreUniquePerGVK(t *testing.T) {
	resource := ResourceConfig{Group: "routing.example.io", Version: "v1", Kind: "Router"}
	indexes, err := configuredFieldIndexes([]MappingConfig{
		{Resource: resource, Selector: SelectorConfig{Fields: []string{"metadata.name=${header:Name}", "spec.routerId=one"}}},
		{Resource: resource, Selector: SelectorConfig{Fields: []string{"spec.routerId=two"}}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"metadata.name", "spec.routerId"}, indexes[schema.GroupVersionKind{Group: resource.Group, Version: resource.Version, Kind: resource.Kind}])
}

func TestUnstructuredFieldIndexIndexesDynamicValues(t *testing.T) {
	index := unstructuredFieldIndex("spec.routerId")
	first := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"routerId": "router-1"}}}
	second := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"routerId": "router-2"}}}

	require.Equal(t, []string{"router-1"}, index(first))
	require.Equal(t, []string{"router-2"}, index(second))
}

func TestGetRESTConfigReportsInvalidExplicitPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	_, err := getRESTConfig(KubeConfig{Kubeconfig: path})
	require.ErrorContains(t, err, path)
}
