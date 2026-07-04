package kubemappingprocessor

import (
	"context"
	"fmt"

	"github.com/jellydator/ttlcache/v3"
	"go.opentelemetry.io/collector/client"
)

type mapper interface {
	Map(context.Context, MappingConfig, client.Metadata) (mappingResult, error)
}

type mappingResult struct {
	value    string
	found    bool
	selector string
}

func (r mappingResult) String() string {
	return fmt.Sprintf("value=%q found=%t selector=%q", r.value, r.found, r.selector)
}

type resourceMapper struct {
	lookup ResourceLookup
}

func (m *resourceMapper) Map(ctx context.Context, mapping MappingConfig, md client.Metadata) (mappingResult, error) {
	selection, err := resolveSelectors(ctx, mapping.Selector, md)
	if err != nil {
		return mappingResult{}, err
	}
	result := mappingResult{selector: selection.String()}
	result.value, result.found, err = executeResolvedMapping(ctx, m.lookup, mapping, selection)
	return result, err
}

type mappingCacheKey struct {
	Resource ResourceConfig
	Value    ValueConfig
	Selector string
}

type mappingCacheValue struct {
	value string
	found bool
}

type cachedMapper struct {
	lookup      ResourceLookup
	cache       *ttlcache.Cache[mappingCacheKey, mappingCacheValue]
	cacheMisses bool
}

func newMapper(lookup ResourceLookup, cfg CacheConfig) mapper {
	if cfg.TTL <= 0 {
		return &resourceMapper{lookup: lookup}
	}
	return &cachedMapper{
		lookup:      lookup,
		cacheMisses: cfg.CacheMisses,
		cache: ttlcache.New[mappingCacheKey, mappingCacheValue](
			ttlcache.WithTTL[mappingCacheKey, mappingCacheValue](cfg.TTL),
			ttlcache.WithCapacity[mappingCacheKey, mappingCacheValue](cfg.Capacity),
			ttlcache.WithDisableTouchOnHit[mappingCacheKey, mappingCacheValue](),
		),
	}
}

func (m *cachedMapper) Map(ctx context.Context, mapping MappingConfig, md client.Metadata) (mappingResult, error) {
	selection, err := resolveSelectors(ctx, mapping.Selector, md)
	if err != nil {
		return mappingResult{}, err
	}
	selector := selection.String()
	key := mappingCacheKey{
		Resource: mapping.Resource,
		Value:    mapping.Value,
		Selector: selector,
	}
	if item := m.cache.Get(key); item != nil {
		cached := item.Value()
		return mappingResult{value: cached.value, found: cached.found, selector: selector}, nil
	}
	value, found, err := executeResolvedMapping(ctx, m.lookup, mapping, selection)
	if err != nil {
		return mappingResult{selector: selector}, err
	}
	if found || m.cacheMisses {
		m.cache.Set(key, mappingCacheValue{value: value, found: found}, ttlcache.DefaultTTL)
	}
	return mappingResult{value: value, found: found, selector: selector}, nil
}
