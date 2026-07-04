package kubemappingprocessor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

type ResourceLookup interface {
	Start(context.Context) error
	Shutdown(context.Context) error
	List(context.Context, ResourceConfig, selectors) ([]unstructured.Unstructured, error)
}

// newDynamicResourceLookup uses controller-runtime for standard in-cluster or
// kubeconfig loading, REST discovery, and unstructured resource access.
func newDynamicResourceLookup(kubeConfig KubeConfig, logger LoggerConfig, mappings []MappingConfig) (ResourceLookup, error) {
	cfg, err := getRESTConfig(kubeConfig)
	if err != nil {
		return nil, err
	}
	return newDynamicResourceLookupForConfig(cfg, logger, mappings)
}

func getRESTConfig(kubeConfig KubeConfig) (*rest.Config, error) {
	if kubeConfig.Kubeconfig == "" && kubeConfig.Context == "" {
		cfg, err := ctrlconfig.GetConfig()
		if err != nil {
			return nil, fmt.Errorf("load default Kubernetes configuration: %w", err)
		}
		return cfg, nil
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeConfig.Kubeconfig != "" {
		loadingRules.ExplicitPath = kubeConfig.Kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: kubeConfig.Context}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig %q with context %q: %w", kubeConfig.Kubeconfig, kubeConfig.Context, err)
	}
	return cfg, nil
}

func newDynamicResourceLookupForConfig(cfg *rest.Config, logger LoggerConfig, mappings []MappingConfig) (ResourceLookup, error) {
	if err := configureControllerRuntimeLogger(logger); err != nil {
		return nil, fmt.Errorf("configure controller-runtime logger: %w", err)
	}
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes HTTP client: %w", err)
	}
	mapper, err := apiutil.NewDynamicRESTMapper(cfg, httpClient)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes REST mapper: %w", err)
	}
	byObject, err := cacheObjects(mapper, mappings)
	if err != nil {
		return nil, err
	}
	fieldIndexes, err := configuredFieldIndexes(mappings)
	if err != nil {
		return nil, err
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: runtime.NewScheme(),
		MapperProvider: func(*rest.Config, *http.Client) (meta.RESTMapper, error) {
			return mapper, nil
		},
		Cache: cache.Options{
			ByObject:                    byObject,
			ReaderFailOnMissingInformer: true,
		},
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
	})
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes manager: %w", err)
	}
	cachedObjects := make([]ctrlclient.Object, 0, len(byObject))
	for obj := range byObject {
		cachedObjects = append(cachedObjects, obj)
	}
	return &resourceLookup{manager: mgr, mapper: mapper, cachedObjects: cachedObjects, fieldIndexes: fieldIndexes}, nil
}

type resourceLookup struct {
	manager       manager.Manager
	mapper        meta.RESTMapper
	cachedObjects []ctrlclient.Object
	fieldIndexes  map[schema.GroupVersionKind][]string
	cancel        context.CancelFunc
	done          chan error
}

func configuredFieldIndexes(mappings []MappingConfig) (map[schema.GroupVersionKind][]string, error) {
	indexes := make(map[schema.GroupVersionKind]map[string]struct{})
	for i, mapping := range mappings {
		paths, err := configuredFieldPaths(mapping.Selector.Fields)
		if err != nil {
			return nil, fmt.Errorf("mappings[%d]: field selectors: %w", i, err)
		}
		gvk := schema.GroupVersionKind{Group: mapping.Resource.Group, Version: mapping.Resource.Version, Kind: mapping.Resource.Kind}
		if indexes[gvk] == nil {
			indexes[gvk] = make(map[string]struct{})
		}
		for _, path := range paths {
			indexes[gvk][path] = struct{}{}
		}
	}
	result := make(map[schema.GroupVersionKind][]string, len(indexes))
	for gvk, fields := range indexes {
		for field := range fields {
			result[gvk] = append(result[gvk], field)
		}
		sort.Strings(result[gvk])
	}
	return result, nil
}

type cachedResource struct {
	gvk        schema.GroupVersionKind
	namespaced bool
	all        bool
	namespaces map[string]cache.Config
}

func cacheObjects(mapper meta.RESTMapper, mappings []MappingConfig) (map[ctrlclient.Object]cache.ByObject, error) {
	resources := make(map[schema.GroupVersionKind]*cachedResource)
	for i, mapping := range mappings {
		gvk := schema.GroupVersionKind{Group: mapping.Resource.Group, Version: mapping.Resource.Version, Kind: mapping.Resource.Kind}
		restMapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, fmt.Errorf("mappings[%d]: resolve %s: %w", i, gvk, err)
		}
		resource := resources[gvk]
		if resource == nil {
			resource = &cachedResource{gvk: gvk, namespaced: restMapping.Scope.Name() == meta.RESTScopeNameNamespace, namespaces: map[string]cache.Config{}}
			resources[gvk] = resource
		}
		if resource.namespaced {
			if mapping.Resource.Namespace == "" {
				resource.all = true
			} else {
				resource.namespaces[mapping.Resource.Namespace] = cache.Config{}
			}
		}
	}

	objects := make(map[ctrlclient.Object]cache.ByObject, len(resources))
	for _, resource := range resources {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(resource.gvk)
		options := cache.ByObject{}
		if resource.namespaced && !resource.all {
			options.Namespaces = resource.namespaces
		}
		objects[obj] = options
	}
	return objects, nil
}

func (l *resourceLookup) List(ctx context.Context, resource ResourceConfig, selection selectors) ([]unstructured.Unstructured, error) {
	mapping, err := l.mapper.RESTMapping(schema.GroupKind{Group: resource.Group, Kind: resource.Kind}, resource.Version)
	if err != nil {
		return nil, fmt.Errorf("resolve %s/%s, Kind=%s: %w", resource.Group, resource.Version, resource.Kind, err)
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: resource.Group, Version: resource.Version, Kind: resource.Kind + "List"})
	listOptions := &ctrlclient.ListOptions{
		LabelSelector: selection.labels,
		FieldSelector: selection.fields,
	}
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		listOptions.Namespace = resource.Namespace
	}
	err = l.manager.GetClient().List(ctx, list, listOptions)
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (l *resourceLookup) Start(ctx context.Context) error {
	for _, obj := range l.cachedObjects {
		gvk := obj.GetObjectKind().GroupVersionKind()
		for _, field := range l.fieldIndexes[gvk] {
			if err := l.manager.GetFieldIndexer().IndexField(ctx, obj, field, unstructuredFieldIndex(field)); err != nil {
				return fmt.Errorf("index Kubernetes field %q for %s: %w", field, gvk, err)
			}
		}
		if _, err := l.manager.GetCache().GetInformer(ctx, obj); err != nil {
			return fmt.Errorf("create Kubernetes informer for %s: %w", obj.GetObjectKind().GroupVersionKind(), err)
		}
	}
	managerCtx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.done = make(chan error, 1)
	go func() { l.done <- l.manager.Start(managerCtx) }()
	if !l.manager.GetCache().WaitForCacheSync(ctx) {
		cancel()
		return errors.New("Kubernetes cache sync failed")
	}
	select {
	case err := <-l.done:
		cancel()
		if err != nil {
			return fmt.Errorf("start Kubernetes manager: %w", err)
		}
		return errors.New("Kubernetes manager stopped during startup")
	default:
		return nil
	}
}

func unstructuredFieldIndex(fieldPath string) ctrlclient.IndexerFunc {
	path := strings.Split(fieldPath, ".")
	return func(object ctrlclient.Object) []string {
		unstructuredObject, ok := object.(*unstructured.Unstructured)
		if !ok {
			return nil
		}
		value, found, err := unstructured.NestedFieldNoCopy(unstructuredObject.Object, path...)
		if err != nil || !found || value == nil {
			return nil
		}
		text, err := scalarString(value, fieldPath)
		if err != nil {
			return nil
		}
		return []string{text}
	}
}

func (l *resourceLookup) Shutdown(ctx context.Context) error {
	if l.cancel == nil {
		return nil
	}
	l.cancel()
	select {
	case err := <-l.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
