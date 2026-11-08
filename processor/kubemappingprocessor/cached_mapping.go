package kubemappingprocessor

import (
	"context"
	"fmt"
	"sync"

	"github.com/jellydator/ttlcache/v3"
	"go.opentelemetry.io/collector/client"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type mapper interface {
	Map(context.Context, MappingConfig, client.Metadata) (mappingResult, error)
}

type mappingResult struct {
	value    string
	found    bool
	selector string
}

type objectIdentity struct {
	GVK       schema.GroupVersionKind
	Namespace string
	Name      string
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
	result.value, result.found, _, err = executeResolvedMapping(ctx, m.lookup, mapping, selection)
	return result, err
}

type mappingCacheKey struct {
	Resource ResourceConfig
	Value    ValueConfig
	Selector string
}

type mappingCacheValue struct {
	value         string
	found         bool
	object        objectIdentity
	objectVersion uint64
}

type cachedMapper struct {
	lookup           ResourceLookup
	cache            *ttlcache.Cache[mappingCacheKey, mappingCacheValue]
	capacity         uint64
	cacheMisses      bool
	mu               sync.Mutex
	generation       uint64
	keysByObject     map[objectIdentity]map[mappingCacheKey]struct{}
	objectByCacheKey map[mappingCacheKey]indexedObject
	objectVersions   map[objectIdentity]uint64
}

type indexedObject struct {
	identity objectIdentity
}

func newMapper(lookup ResourceLookup, cfg CacheConfig) mapper {
	if cfg.TTL <= 0 {
		return &resourceMapper{lookup: lookup}
	}
	m := &cachedMapper{
		lookup:           lookup,
		capacity:         cfg.Capacity,
		cacheMisses:      cfg.CacheMisses,
		keysByObject:     make(map[objectIdentity]map[mappingCacheKey]struct{}),
		objectByCacheKey: make(map[mappingCacheKey]indexedObject),
		objectVersions:   make(map[objectIdentity]uint64),
		cache: ttlcache.New[mappingCacheKey, mappingCacheValue](
			ttlcache.WithTTL[mappingCacheKey, mappingCacheValue](cfg.TTL),
			ttlcache.WithCapacity[mappingCacheKey, mappingCacheValue](cfg.Capacity),
			ttlcache.WithDisableTouchOnHit[mappingCacheKey, mappingCacheValue](),
		),
	}
	return m
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
	m.mu.Lock()
	if item := m.cache.Get(key); item != nil {
		cached := item.Value()
		if cached.objectVersion == m.objectVersions[cached.object] {
			m.mu.Unlock()
			return mappingResult{value: cached.value, found: cached.found, selector: selector}, nil
		}
	}
	generation := m.generation
	m.mu.Unlock()
	value, found, object, err := executeResolvedMapping(ctx, m.lookup, mapping, selection)
	if err != nil {
		return mappingResult{selector: selector}, err
	}
	if found || m.cacheMisses {
		m.mu.Lock()
		if generation == m.generation {
			m.removeCacheKeyLocked(key)
			entry := mappingCacheValue{value: value, found: found, object: object, objectVersion: m.objectVersions[object]}
			m.cache.Set(key, entry, ttlcache.DefaultTTL)
			if found {
				m.addCacheKeyLocked(key, object)
				m.pruneCacheIndexesLocked()
			}
		}
		m.mu.Unlock()
	}
	return mappingResult{value: value, found: found, selector: selector}, nil
}

func (m *cachedMapper) InvalidateObject(object objectIdentity) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.generation++
	if len(m.keysByObject[object]) > 0 {
		m.objectVersions[object]++
	}
}

func (m *cachedMapper) addCacheKeyLocked(key mappingCacheKey, object objectIdentity) {
	if m.keysByObject[object] == nil {
		m.keysByObject[object] = make(map[mappingCacheKey]struct{})
	}
	m.keysByObject[object][key] = struct{}{}
	m.objectByCacheKey[key] = indexedObject{identity: object}
}

func (m *cachedMapper) removeCacheKeyLocked(key mappingCacheKey) {
	indexed, ok := m.objectByCacheKey[key]
	if !ok {
		return
	}
	delete(m.objectByCacheKey, key)
	keys := m.keysByObject[indexed.identity]
	delete(keys, key)
	if len(keys) == 0 {
		delete(m.keysByObject, indexed.identity)
		delete(m.objectVersions, indexed.identity)
	}
}

func (m *cachedMapper) pruneCacheIndexesLocked() {
	threshold := int(m.capacity / 2)
	if threshold < 64 {
		threshold = 64
	}
	if len(m.objectByCacheKey) <= int(m.capacity)+threshold {
		return
	}
	for key := range m.objectByCacheKey {
		if m.cache.Get(key) == nil {
			m.removeCacheKeyLocked(key)
		}
	}
}
