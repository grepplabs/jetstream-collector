package openbaoprovider

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

type fakeStore struct {
	config      string
	version     int
	err         error
	versionErr  error
	ref         SecretRef
	versionRefs []SecretRef
}

type staticProvider struct {
	scheme string
	value  any
}

func (p *staticProvider) Retrieve(context.Context, string, confmap.WatcherFunc) (*confmap.Retrieved, error) {
	return confmap.NewRetrieved(p.value)
}

func (p *staticProvider) Scheme() string { return p.scheme }

func (*staticProvider) Shutdown(context.Context) error { return nil }

func (s *fakeStore) GetValue(_ context.Context, ref SecretRef) (storeValue, error) {
	s.ref = ref
	return storeValue{Value: s.config, Version: s.version}, s.err
}

func (s *fakeStore) CurrentVersion(_ context.Context, ref SecretRef) (int, error) {
	s.versionRefs = append(s.versionRefs, ref)
	return s.version, s.versionErr
}

func TestProviderScheme(t *testing.T) {
	p := NewFactory().Create(confmaptest.NewNopProviderSettings())
	require.NoError(t, confmaptest.ValidateProviderScheme(p))
	assert.Equal(t, schemeName, p.Scheme())
}

func TestFactoryWithConfigDoesNotReadEnvironment(t *testing.T) {
	clearOpenBaoEnvironment(t)
	factory := NewFactory(WithConfig(Config{Address: "https://explicit:8200"}))
	p := factory.Create(confmaptest.NewNopProviderSettings())

	_, err := p.Retrieve(context.Background(), "openbao:secret/config", nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "auth.token is required")
	assert.NotContains(t, err.Error(), envAddress)
	assert.NotContains(t, err.Error(), envToken)
}

func TestProviderRetrieve(t *testing.T) {
	backend := &fakeStore{config: "receivers:\n  otlp: {}\n", version: 1}
	p := &provider{newStore: func() (store, WatchConfig, error) { return backend, WatchConfig{}, nil }}

	retrieved, err := p.Retrieve(context.Background(), "openbao:secret/otel/collector", nil)
	require.NoError(t, err)
	assert.Equal(t, SecretRef{Mount: "secret", Path: "otel/collector"}, backend.ref)
	conf, err := retrieved.AsConf()
	require.NoError(t, err)
	assert.True(t, conf.IsSet("receivers::otlp"))
}

func TestProviderRetrieveField(t *testing.T) {
	backend := &fakeStore{config: "Bearer secret-token", version: 1}
	p := &provider{newStore: func() (store, WatchConfig, error) { return backend, WatchConfig{}, nil }}

	retrieved, err := p.Retrieve(context.Background(), "openbao:secret/otel/credentials#token", nil)
	require.NoError(t, err)
	assert.Equal(t, SecretRef{Mount: "secret", Path: "otel/credentials", Field: "token"}, backend.ref)
	value, err := retrieved.AsString()
	require.NoError(t, err)
	assert.Equal(t, "Bearer secret-token", value)
}

func TestProviderFieldInlineSubstitution(t *testing.T) {
	backend := &fakeStore{config: "Bearer secret-token", version: 1}
	openBaoFactory := confmap.NewProviderFactory(func(confmap.ProviderSettings) confmap.Provider {
		return &provider{newStore: func() (store, WatchConfig, error) { return backend, WatchConfig{}, nil }}
	})
	inputFactory := confmap.NewProviderFactory(func(confmap.ProviderSettings) confmap.Provider {
		return &staticProvider{
			scheme: "input",
			value: map[string]any{
				"headers": map[string]any{
					"Authorization": "${openbao:secret/otel/credentials#token}",
				},
			},
		}
	})

	resolver, err := confmap.NewResolver(confmap.ResolverSettings{
		URIs:              []string{"input:"},
		ProviderFactories: []confmap.ProviderFactory{inputFactory, openBaoFactory},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, resolver.Shutdown(context.Background()))
	})

	resolved, err := resolver.Resolve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "Bearer secret-token", resolved.Get("headers::Authorization"))
}

func TestProviderRetrieveErrors(t *testing.T) {
	sentinel := errors.New("backend unavailable")
	tests := []struct {
		name       string
		ctx        func() context.Context
		uri        string
		newStore   storeFactory
		wantErr    error
		wantErrMsg string
	}{
		{
			name:       "invalid URI",
			ctx:        context.Background,
			uri:        "openbao:secret",
			newStore:   func() (store, WatchConfig, error) { return &fakeStore{}, WatchConfig{}, nil },
			wantErrMsg: "missing secret path",
		},
		{
			name:       "store construction",
			ctx:        context.Background,
			uri:        "openbao:secret/config",
			newStore:   func() (store, WatchConfig, error) { return nil, WatchConfig{}, sentinel },
			wantErr:    sentinel,
			wantErrMsg: "configure client",
		},
		{
			name: "store read",
			ctx:  context.Background,
			uri:  "openbao:secret/config",
			newStore: func() (store, WatchConfig, error) {
				return &fakeStore{err: sentinel}, WatchConfig{}, nil
			},
			wantErr:    sentinel,
			wantErrMsg: `read secret "config" from mount "secret"`,
		},
		{
			name: "cancelled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			uri:        "openbao:secret/config",
			newStore:   func() (store, WatchConfig, error) { return &fakeStore{}, WatchConfig{}, nil },
			wantErr:    context.Canceled,
			wantErrMsg: "context canceled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &provider{newStore: tt.newStore}
			_, err := p.Retrieve(tt.ctx(), tt.uri, nil)
			require.Error(t, err)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			}
			assert.ErrorContains(t, err, tt.wantErrMsg)
		})
	}
}

func TestProviderInvalidYAMLIsNotACompleteConfiguration(t *testing.T) {
	p := &provider{newStore: func() (store, WatchConfig, error) {
		return &fakeStore{config: "[invalid:,", version: 1}, WatchConfig{}, nil
	}}

	retrieved, err := p.Retrieve(context.Background(), "openbao:secret/config", nil)
	require.NoError(t, err)
	_, err = retrieved.AsConf()
	assert.Error(t, err)
}

func TestProviderShutdown(t *testing.T) {
	p := &provider{}
	assert.NoError(t, p.Shutdown(context.Background()))
}

var _ confmap.Provider = (*provider)(nil)

type manualWatchTicker struct {
	ch      chan time.Time
	stopped chan struct{}
}

func newManualWatchTicker() *manualWatchTicker {
	return &manualWatchTicker{
		ch:      make(chan time.Time, 1),
		stopped: make(chan struct{}),
	}
}

func (t *manualWatchTicker) C() <-chan time.Time { return t.ch }
func (t *manualWatchTicker) Stop() {
	select {
	case <-t.stopped:
	default:
		close(t.stopped)
	}
}
func (t *manualWatchTicker) Tick() { t.ch <- time.Now() }

type versionResponse struct {
	version int
	err     error
}

type watchTestStore struct {
	initial   storeValue
	responses chan versionResponse
	calls     chan SecretRef
}

func newWatchTestStore(initialVersion int) *watchTestStore {
	return &watchTestStore{
		initial:   storeValue{Value: "receivers:\n  otlp: {}\n", Version: initialVersion},
		responses: make(chan versionResponse, 8),
		calls:     make(chan SecretRef, 8),
	}
}

func (s *watchTestStore) GetValue(context.Context, SecretRef) (storeValue, error) {
	return s.initial, nil
}

func (s *watchTestStore) CurrentVersion(ctx context.Context, ref SecretRef) (int, error) {
	select {
	case response := <-s.responses:
		s.calls <- ref
		return response.version, response.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func TestProviderWatchDisabledDoesNotPoll(t *testing.T) {
	backend := newWatchTestStore(1)
	tickerCreated := false
	p := &provider{
		newStore: func() (store, WatchConfig, error) {
			return backend, WatchConfig{Enabled: false, Interval: time.Second}, nil
		},
		newTicker: func(time.Duration) watchTicker {
			tickerCreated = true
			return newManualWatchTicker()
		},
	}

	retrieved, err := p.Retrieve(context.Background(), "openbao:secret/config", func(*confmap.ChangeEvent) {})
	require.NoError(t, err)
	require.NoError(t, retrieved.Close(context.Background()))
	assert.False(t, tickerCreated)
	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestProviderWatchSignalsOnlyOnVersionChange(t *testing.T) {
	backend := newWatchTestStore(17)
	ticker := newManualWatchTicker()
	p := &provider{
		newStore: func() (store, WatchConfig, error) {
			return backend, WatchConfig{Enabled: true, Interval: time.Second}, nil
		},
		newTicker: func(time.Duration) watchTicker {
			return ticker
		},
	}

	changes := make(chan *confmap.ChangeEvent, 4)
	retrieved, err := p.Retrieve(context.Background(), "openbao:secret/otel/collector", func(event *confmap.ChangeEvent) {
		changes <- event
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, p.Shutdown(context.Background())) })

	backend.responses <- versionResponse{version: 17}
	ticker.Tick()
	assert.Equal(t, SecretRef{Mount: "secret", Path: "otel/collector"}, <-backend.calls)
	select {
	case event := <-changes:
		t.Fatalf("unexpected change event for unchanged version: %#v", event)
	default:
	}

	backend.responses <- versionResponse{version: 18}
	ticker.Tick()
	<-backend.calls
	select {
	case event := <-changes:
		require.NotNil(t, event)
		assert.NoError(t, event.Error)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for version change event")
	}

	backend.responses <- versionResponse{version: 18}
	ticker.Tick()
	<-backend.calls
	select {
	case event := <-changes:
		t.Fatalf("unexpected duplicate change event for unchanged version: %#v", event)
	default:
	}

	backend.responses <- versionResponse{version: 19}
	ticker.Tick()
	<-backend.calls
	select {
	case <-changes:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second version change event")
	}

	require.NoError(t, retrieved.Close(context.Background()))
	select {
	case <-ticker.stopped:
	case <-time.After(time.Second):
		t.Fatal("watch ticker was not stopped when Retrieved was closed")
	}
}

func TestProviderWatchContinuesAfterVersionCheckError(t *testing.T) {
	backend := newWatchTestStore(1)
	ticker := newManualWatchTicker()
	p := &provider{
		newStore: func() (store, WatchConfig, error) {
			return backend, WatchConfig{Enabled: true, Interval: time.Second}, nil
		},
		newTicker: func(time.Duration) watchTicker {
			return ticker
		},
	}

	changes := make(chan *confmap.ChangeEvent, 1)
	retrieved, err := p.Retrieve(context.Background(), "openbao:secret/config", func(event *confmap.ChangeEvent) {
		changes <- event
	})
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, retrieved.Close(context.Background()))
		assert.NoError(t, p.Shutdown(context.Background()))
	}()

	backend.responses <- versionResponse{err: errors.New("temporary failure")}
	ticker.Tick()
	<-backend.calls
	select {
	case event := <-changes:
		t.Fatalf("unexpected change event for version-check error: %#v", event)
	default:
	}

	backend.responses <- versionResponse{version: 2}
	ticker.Tick()
	<-backend.calls
	select {
	case <-changes:
	case <-time.After(time.Second):
		t.Fatal("watch did not recover after temporary version-check error")
	}
}

func TestProviderRetrievedCloseStopsWatch(t *testing.T) {
	backend := newWatchTestStore(1)
	ticker := newManualWatchTicker()
	p := &provider{
		newStore: func() (store, WatchConfig, error) {
			return backend, WatchConfig{Enabled: true, Interval: time.Second}, nil
		},
		newTicker: func(time.Duration) watchTicker {
			return ticker
		},
	}

	retrieved, err := p.Retrieve(context.Background(), "openbao:secret/config", func(*confmap.ChangeEvent) {})
	require.NoError(t, err)
	require.NoError(t, retrieved.Close(context.Background()))

	select {
	case <-ticker.stopped:
	case <-time.After(time.Second):
		t.Fatal("watch ticker was not stopped")
	}
	assert.NoError(t, p.Shutdown(context.Background()))
}

func TestProviderShutdownStopsActiveWatch(t *testing.T) {
	backend := newWatchTestStore(1)
	ticker := newManualWatchTicker()
	p := &provider{
		newStore: func() (store, WatchConfig, error) {
			return backend, WatchConfig{Enabled: true, Interval: time.Second}, nil
		},
		newTicker: func(time.Duration) watchTicker {
			return ticker
		},
	}

	_, err := p.Retrieve(context.Background(), "openbao:secret/config", func(*confmap.ChangeEvent) {})
	require.NoError(t, err)
	require.NoError(t, p.Shutdown(context.Background()))

	select {
	case <-ticker.stopped:
	case <-time.After(time.Second):
		t.Fatal("watch ticker was not stopped by provider shutdown")
	}

	_, err = p.Retrieve(context.Background(), "openbao:secret/config", nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "after shutdown")
}
