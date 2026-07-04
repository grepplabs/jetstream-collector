package kubemappingprocessor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type fakeLookup struct {
	objects []unstructured.Unstructured
	err     error
	labels  []string
}

func (f *fakeLookup) Start(context.Context) error    { return nil }
func (f *fakeLookup) Shutdown(context.Context) error { return nil }

func (f *fakeLookup) List(_ context.Context, _ ResourceConfig, selection selectors) ([]unstructured.Unstructured, error) {
	f.labels = append(f.labels, selection.labels.String())
	return f.objects, f.err
}

func TestExecuteMappingMatchStates(t *testing.T) {
	m := validConfig().Mappings[0]
	md := client.NewMetadata(map[string][]string{"Token": {"abc"}})
	f := &fakeLookup{}
	_, ok, err := executeMapping(context.Background(), f, m, md)
	require.ErrorContains(t, err, "matched no objects")
	require.True(t, consumererror.IsPermanent(err))
	require.False(t, ok)
	f.objects = []unstructured.Unstructured{{Object: map[string]any{"spec": map[string]any{"routerId": "router-42"}}}}
	got, ok, err := executeMapping(context.Background(), f, m, md)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "router-42", got)
	require.Equal(t, "token=abc", f.labels[1])
	f.objects = append(f.objects, f.objects[0])
	_, _, err = executeMapping(context.Background(), f, m, md)
	require.ErrorContains(t, err, `source /v1, Kind=Router selector "labelSelector:token=abc" matched 2 objects`)
	require.True(t, consumererror.IsPermanent(err))
	f.err = errors.New("forbidden")
	_, _, err = executeMapping(context.Background(), f, m, md)
	require.ErrorContains(t, err, "forbidden")
	require.False(t, consumererror.IsPermanent(err))
}

func TestSequentialMappingsObserveEarlierHeaders(t *testing.T) {
	cfg := validConfig()
	second := cfg.Mappings[0]
	second.Selector.Labels = []string{"router=${header:X-Tenant-ID}"}
	second.Target = "X-Final"
	cfg.Mappings = append(cfg.Mappings, second)
	f := &fakeLookup{objects: []unstructured.Unstructured{{Object: map[string]any{"spec": map[string]any{"routerId": "router-42"}}}}}
	p := &kubeMappingProcessor{cfg: cfg, lookup: f, mapper: newMapper(f, CacheConfig{}), logger: zap.NewNop()}
	ctx := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"Token": {"abc"}, "Keep": {"yes"}})})
	out, err := p.mapContext(ctx)
	require.NoError(t, err)
	md := client.FromContext(out).Metadata
	require.Equal(t, []string{"router-42"}, md.Get("X-Final"))
	require.Equal(t, []string{"yes"}, md.Get("Keep"))
	require.Equal(t, []string{"token=abc", "router=router-42"}, f.labels)
}

func TestMapContextErrorModes(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"Token": {"abc"}})})

	lookup := &fakeLookup{}
	cfg := validConfig()
	p := &kubeMappingProcessor{cfg: cfg, lookup: lookup, mapper: newMapper(lookup, CacheConfig{}), logger: zap.NewNop()}
	_, err := p.mapContext(ctx)
	require.Error(t, err)
	require.True(t, consumererror.IsPermanent(err))

	cfg.ErrorMode = ErrorModeIgnore
	_, err = p.mapContext(ctx)
	require.NoError(t, err)

	cfg.ErrorMode = ErrorModePropagate
	lookup.err = errors.New("temporarily unavailable")
	_, err = p.mapContext(ctx)
	require.ErrorContains(t, err, "temporarily unavailable")
	require.False(t, consumererror.IsPermanent(err))
}
