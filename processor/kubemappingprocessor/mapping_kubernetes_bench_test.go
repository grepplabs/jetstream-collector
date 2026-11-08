package kubemappingprocessor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/client"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

type countingResourceLookup struct {
	ResourceLookup
	listCalls atomic.Uint64
}

func (l *countingResourceLookup) List(ctx context.Context, resource ResourceConfig, selection selectors) ([]unstructured.Unstructured, error) {
	l.listCalls.Add(1)
	return l.ResourceLookup.List(ctx, resource, selection)
}

func BenchmarkKubernetesCacheMapping(b *testing.B) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		b.Skip("KUBEBUILDER_ASSETS is unset; run make bench-kube")
	}
	for _, binary := range []string{"kube-apiserver", "etcd"} {
		if _, err := os.Stat(filepath.Join(assets, binary)); err != nil {
			b.Skipf("envtest binary %q is unavailable in %q", binary, assets)
		}
	}

	environment := &envtest.Environment{BinaryAssetsDirectory: assets}
	config, err := environment.Start()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			b.Errorf("stop envtest: %v", err)
		}
	})
	config.QPS = 500
	config.Burst = 1000

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		b.Fatal(err)
	}
	apiClient, err := ctrlclient.New(config, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		b.Fatal(err)
	}

	createBenchmarkNamespace(b, apiClient, "bench-small")
	createBenchmarkNamespace(b, apiClient, "bench-medium")
	createBenchmarkNamespace(b, apiClient, "bench-large")
	createBenchmarkConfigMaps(b, apiClient, "bench-small", 1)
	createBenchmarkConfigMaps(b, apiClient, "bench-medium", 1_000)
	createBenchmarkConfigMaps(b, apiClient, "bench-large", 10_000)

	b.Run("objects_1", func(b *testing.B) {
		benchmarkRealCacheMapping(b, config, "bench-small")
	})
	b.Run("objects_1000", func(b *testing.B) {
		benchmarkRealCacheMapping(b, config, "bench-medium")
	})
	b.Run("objects_10000", func(b *testing.B) {
		benchmarkRealCacheMapping(b, config, "bench-large")
	})
}

func benchmarkRealCacheMapping(b *testing.B, config *rest.Config, namespace string) {
	b.Helper()
	mapping := MappingConfig{
		Resource: ResourceConfig{Version: "v1", Kind: "ConfigMap", Namespace: namespace},
		Selector: SelectorConfig{Labels: []string{"mapping-id=${header:Token}"}},
		Value:    ValueConfig{Field: "data.tenant"},
	}
	resourceLookup, err := newDynamicResourceLookupForConfig(config, LoggerConfig{}, []MappingConfig{mapping})
	if err != nil {
		b.Fatal(err)
	}
	if err := resourceLookup.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := resourceLookup.Shutdown(ctx); err != nil {
			b.Errorf("stop Kubernetes cache: %v", err)
		}
	}()

	lookup := &countingResourceLookup{ResourceLookup: resourceLookup}
	metadata := client.NewMetadata(map[string][]string{"Token": {"router-1"}})
	uncached := newMapper(lookup, CacheConfig{})
	cached := newMapper(lookup, CacheConfig{TTL: time.Minute, Capacity: 10})
	if _, err := cached.Map(context.Background(), mapping, metadata); err != nil {
		b.Fatal(err)
	}

	b.Run("real_list/uncached", func(b *testing.B) {
		lookup.listCalls.Store(0)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := uncached.Map(context.Background(), mapping, metadata); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(lookup.listCalls.Load())/float64(b.N), "lists/op")
	})

	b.Run("warm_mapping_cache/cached", func(b *testing.B) {
		lookup.listCalls.Store(0)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := cached.Map(context.Background(), mapping, metadata); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(lookup.listCalls.Load())/float64(b.N), "lists/op")
	})
}

func createBenchmarkNamespace(b *testing.B, apiClient ctrlclient.Client, name string) {
	b.Helper()
	if err := apiClient.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		b.Fatal(err)
	}
}

func createBenchmarkConfigMaps(b *testing.B, apiClient ctrlclient.Client, namespace string, count int) {
	b.Helper()
	const workerCount = 32
	jobs := make(chan int)
	errors := make(chan error, count)
	var workers sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				name := fmt.Sprintf("router-%05d", index)
				mappingID := fmt.Sprintf("decoy-%05d", index)
				if index == 0 {
					mappingID = "router-1"
				}
				object := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: map[string]string{"mapping-id": mappingID}},
					Data:       map[string]string{"tenant": "tenant-1"},
				}
				errors <- apiClient.Create(context.Background(), object)
			}
		}()
	}
	for index := 0; index < count; index++ {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			b.Fatal(err)
		}
	}
}
