package kubemappingprocessor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestKubernetesCacheInvalidatesMappingAfterObjectUpdate(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset; run make integration-test-envtest")
	}
	for _, binary := range []string{"kube-apiserver", "etcd"} {
		if _, err := os.Stat(filepath.Join(assets, binary)); err != nil {
			t.Skipf("envtest binary %q is unavailable in %q", binary, assets)
		}
	}

	environment := &envtest.Environment{BinaryAssetsDirectory: assets}
	config, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	apiClient, err := ctrlclient.New(config, ctrlclient.Options{Scheme: scheme})
	require.NoError(t, err)

	mapping := MappingConfig{
		Resource: ResourceConfig{Version: "v1", Kind: "ConfigMap", Namespace: "default"},
		Selector: SelectorConfig{Labels: []string{"mapping-id=${header:Token}"}},
		Value:    ValueConfig{Field: "data.tenant"},
	}
	ctx := context.Background()
	object := &corev1.ConfigMap{
		Namespace: "default",
		Name:      "router",
		Labels:    map[string]string{"mapping-id": "router-1"},
		Data:      map[string]string{"tenant": "tenant-before"},
	}
	require.NoError(t, apiClient.Create(ctx, object))

	lookup, err := newDynamicResourceLookupForConfig(config, LoggerConfig{}, []MappingConfig{mapping})
	require.NoError(t, err)
	mapper := newMapper(lookup, CacheConfig{TTL: time.Minute, Capacity: 10}).(*cachedMapper)
	configureCacheInvalidation(lookup, mapper, true)
	require.NoError(t, lookup.Start(ctx))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, lookup.Shutdown(shutdownCtx))
	})

	metadata := integrationMetadata("router-1")
	result, err := mapper.Map(ctx, mapping, metadata)
	require.NoError(t, err)
	require.Equal(t, "tenant-before", result.value)
	result, err = mapper.Map(ctx, mapping, metadata)
	require.NoError(t, err)
	require.Equal(t, "tenant-before", result.value)

	current := &corev1.ConfigMap{}
	require.NoError(t, apiClient.Get(ctx, ctrlclient.ObjectKeyFromObject(object), current))
	current.Data["tenant"] = "tenant-after"
	require.NoError(t, apiClient.Update(ctx, current))

	require.Eventually(t, func() bool {
		result, err := mapper.Map(ctx, mapping, metadata)
		return err == nil && result.value == "tenant-after"
	}, 10*time.Second, 25*time.Millisecond)
}

func integrationMetadata(token string) client.Metadata {
	return client.NewMetadata(map[string][]string{"Token": {token}})
}
