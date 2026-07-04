package kubemappingprocessor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type cacheLookup struct {
	objects   []unstructured.Unstructured
	err       error
	listCalls int
}

func (*cacheLookup) Start(context.Context) error    { return nil }
func (*cacheLookup) Shutdown(context.Context) error { return nil }

func (l *cacheLookup) List(context.Context, ResourceConfig, selectors) ([]unstructured.Unstructured, error) {
	l.listCalls++
	return l.objects, l.err
}

func cacheTestMapping() MappingConfig {
	return MappingConfig{
		Resource: ResourceConfig{Version: "v1", Kind: "Router", Namespace: "default"},
		Selector: SelectorConfig{Labels: []string{"token=${header:Token}"}},
		Value:    ValueConfig{Field: "spec.id"},
		Target:   "X-Router-ID",
	}
}

func TestSelectorsString(t *testing.T) {
	mapping := cacheTestMapping()
	md := client.NewMetadata(map[string][]string{"Token": {"abc"}})
	selection, ok, err := buildSelectors(context.Background(), mapping.Selector, md)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "labelSelector:token=abc", selection.String())

	mapping.Selector.Fields = []string{"metadata.name=${header:Token}"}
	selection, ok, err = buildSelectors(context.Background(), mapping.Selector, md)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "labelSelector:token=abc;fieldSelector:metadata.name=abc", selection.String())

	mapping.Selector.Labels = nil
	selection, ok, err = buildSelectors(context.Background(), mapping.Selector, md)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "fieldSelector:metadata.name=abc", selection.String())
}

func TestCachedMapperCachesExtractedValues(t *testing.T) {
	lookup := &cacheLookup{objects: []unstructured.Unstructured{{Object: map[string]any{"spec": map[string]any{"id": "one", "name": "first"}}}}}
	m := newMapper(lookup, CacheConfig{TTL: time.Minute, Capacity: 10})
	mapping := cacheTestMapping()
	md := client.NewMetadata(map[string][]string{"Token": {"abc"}})

	result, err := m.Map(context.Background(), mapping, md)
	require.NoError(t, err)
	require.True(t, result.found)
	require.Equal(t, "one", result.value)
	require.Equal(t, "labelSelector:token=abc", result.selector)
	require.Equal(t, `value="one" found=true selector="labelSelector:token=abc"`, result.String())
	lookup.objects[0].Object["spec"].(map[string]any)["id"] = "changed"
	result, err = m.Map(context.Background(), mapping, md)
	require.NoError(t, err)
	require.True(t, result.found)
	require.Equal(t, "one", result.value)
	require.Equal(t, "labelSelector:token=abc", result.selector)
	require.Equal(t, 1, lookup.listCalls)

	// The configured extraction path is part of the key; the target is not.
	mapping.Value.Field = "spec.name"
	mapping.Target = "X-Other"
	result, err = m.Map(context.Background(), mapping, md)
	require.NoError(t, err)
	require.True(t, result.found)
	require.Equal(t, "first", result.value)
	require.Equal(t, 2, lookup.listCalls)
	mapping.Target = "X-Another"
	_, err = m.Map(context.Background(), mapping, md)
	require.NoError(t, err)
	require.Equal(t, 2, lookup.listCalls)
}

func TestCachedMapperDoesNotCachePermanentMisses(t *testing.T) {
	lookup := &cacheLookup{}
	m := newMapper(lookup, CacheConfig{TTL: time.Minute, Capacity: 10})
	mapping := cacheTestMapping()
	md := client.NewMetadata(map[string][]string{"Token": {"abc"}})

	result, err := m.Map(context.Background(), mapping, md)
	require.ErrorContains(t, err, "matched no objects")
	require.True(t, consumererror.IsPermanent(err))
	require.False(t, result.found)
	lookup.objects = []unstructured.Unstructured{{Object: map[string]any{"spec": map[string]any{"id": "one"}}}}
	result, err = m.Map(context.Background(), mapping, md)
	require.NoError(t, err)
	require.True(t, result.found)
	require.Equal(t, "labelSelector:token=abc", result.selector)
	require.Equal(t, 2, lookup.listCalls)
}

func TestCachedMapperDoesNotCacheErrors(t *testing.T) {
	lookup := &cacheLookup{err: errors.New("forbidden")}
	m := newMapper(lookup, CacheConfig{TTL: time.Minute, Capacity: 10})
	mapping := cacheTestMapping()
	md := client.NewMetadata(map[string][]string{"Token": {"abc"}})

	result, _ := m.Map(context.Background(), mapping, md)
	require.Equal(t, "labelSelector:token=abc", result.selector)
	_, _ = m.Map(context.Background(), mapping, md)
	require.Equal(t, 2, lookup.listCalls)
}

func TestCachedMapperKeyExpirationAndDisable(t *testing.T) {
	lookup := &cacheLookup{objects: []unstructured.Unstructured{{Object: map[string]any{"spec": map[string]any{"id": "one"}}}}}
	disabled := newMapper(lookup, CacheConfig{})
	require.IsType(t, &resourceMapper{}, disabled)
	mapping := cacheTestMapping()

	_, _ = disabled.Map(context.Background(), mapping, client.NewMetadata(map[string][]string{"Token": {"one"}}))
	_, _ = disabled.Map(context.Background(), mapping, client.NewMetadata(map[string][]string{"Token": {"one"}}))
	require.Equal(t, 2, lookup.listCalls)

	cached := newMapper(lookup, CacheConfig{TTL: time.Millisecond, Capacity: 10})
	_, _ = cached.Map(context.Background(), mapping, client.NewMetadata(map[string][]string{"Token": {"one"}}))
	_, _ = cached.Map(context.Background(), mapping, client.NewMetadata(map[string][]string{"Token": {"two"}}))
	require.Equal(t, 4, lookup.listCalls)
	time.Sleep(5 * time.Millisecond)
	_, _ = cached.Map(context.Background(), mapping, client.NewMetadata(map[string][]string{"Token": {"one"}}))
	require.Equal(t, 5, lookup.listCalls)
}
