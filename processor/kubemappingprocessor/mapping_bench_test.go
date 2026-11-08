package kubemappingprocessor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"go.opentelemetry.io/collector/client"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const benchmarkWorkingSet = 10_000

type benchmarkLookup struct {
	object    unstructured.Unstructured
	listCalls int
}

func (*benchmarkLookup) Start(context.Context) error    { return nil }
func (*benchmarkLookup) Shutdown(context.Context) error { return nil }

func (l *benchmarkLookup) List(context.Context, ResourceConfig, selectors) ([]unstructured.Unstructured, error) {
	l.listCalls++
	return []unstructured.Unstructured{l.object}, nil
}

func BenchmarkMappingCache(b *testing.B) {
	b.Run("warm_selector/uncached", func(b *testing.B) {
		benchmarkWarmSelector(b, CacheConfig{})
	})
	b.Run("fixed_working_set_10000/uncached", func(b *testing.B) {
		benchmarkUniqueSelectors(b, CacheConfig{})
	})
	for _, capacity := range []uint64{128, 10_000, 100_000} {
		b.Run(fmt.Sprintf("capacity_%d", capacity), func(b *testing.B) {
			b.Run("warm_selector/cached", func(b *testing.B) {
				benchmarkWarmSelector(b, CacheConfig{TTL: time.Minute, Capacity: capacity})
			})
			b.Run("fixed_working_set_10000/cached", func(b *testing.B) {
				benchmarkUniqueSelectors(b, CacheConfig{TTL: time.Minute, Capacity: capacity})
			})
			b.Run("invalidate_full_cache/cached", func(b *testing.B) {
				benchmarkFullCacheInvalidation(b, capacity)
			})
		})
	}
}

func benchmarkWarmSelector(b *testing.B, cache CacheConfig) {
	b.Helper()
	lookup, mapping, mapper := newBenchmarkMapping(cache)
	metadata := benchmarkMetadata("same")
	if _, err := mapper.Map(context.Background(), mapping, metadata); err != nil {
		b.Fatal(err)
	}
	lookup.listCalls = 0 // Exclude the untimed cache-prime lookup from lookups/op.
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := mapper.Map(context.Background(), mapping, metadata); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(lookup.listCalls)/float64(b.N), "lookups/op")
}

func benchmarkUniqueSelectors(b *testing.B, cache CacheConfig) {
	b.Helper()
	lookup, mapping, mapper := newBenchmarkMapping(cache)
	metadata := make([]client.Metadata, benchmarkWorkingSet)
	for i := range metadata {
		metadata[i] = benchmarkMetadata(fmt.Sprintf("selector-%d", i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := mapper.Map(context.Background(), mapping, metadata[i%len(metadata)]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(lookup.listCalls)/float64(b.N), "lookups/op")
}

func benchmarkFullCacheInvalidation(b *testing.B, capacity uint64) {
	b.Helper()
	_, mapping, baseMapper := newBenchmarkMapping(CacheConfig{TTL: time.Minute, Capacity: capacity})
	mapper := baseMapper.(*cachedMapper)
	identity := objectIdentity{GVK: mapping.Resource.GVK(), Namespace: "default", Name: "router"}
	entries := int(capacity)
	mapper.mu.Lock()
	for entry := 0; entry < entries; entry++ {
		key := mappingCacheKey{
			Resource: mapping.Resource,
			Value:    mapping.Value,
			Selector: fmt.Sprintf("benchmark-selector-%d", entry),
		}
		cached := mappingCacheValue{value: "tenant-1", found: true, object: identity}
		mapper.cache.Set(key, cached, ttlcache.DefaultTTL)
		mapper.addCacheKeyLocked(key, identity)
	}
	mapper.mu.Unlock()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mapper.InvalidateObject(identity)
	}
	b.StopTimer()
	b.ReportMetric(float64(entries), "entries/op")
}

func newBenchmarkMapping(cache CacheConfig) (*benchmarkLookup, MappingConfig, mapper) {
	mapping := cacheTestMapping()
	object := unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "router", "namespace": "default"},
		"spec":     map[string]any{"id": "tenant-1"},
	}}
	object.SetGroupVersionKind(mapping.Resource.GVK())
	lookup := &benchmarkLookup{object: object}
	return lookup, mapping, newMapper(lookup, cache)
}

func benchmarkMetadata(token string) client.Metadata {
	return client.NewMetadata(map[string][]string{"Token": {token}})
}
