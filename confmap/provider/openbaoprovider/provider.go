package openbaoprovider

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/confmap"
)

const defaultWatchInterval = 30 * time.Second

var _ confmap.Provider = (*provider)(nil)

type storeValue struct {
	Value   string
	Version int
}

type store interface {
	GetValue(context.Context, SecretRef) (storeValue, error)
	CurrentVersion(context.Context, SecretRef) (int, error)
}

type storeFactory func() (store, WatchConfig, error)

type watchTicker interface {
	C() <-chan time.Time
	Stop()
}

type realWatchTicker struct {
	ticker *time.Ticker
}

func (t realWatchTicker) C() <-chan time.Time { return t.ticker.C }
func (t realWatchTicker) Stop()               { t.ticker.Stop() }

type tickerFactory func(time.Duration) watchTicker

type provider struct {
	newStore  storeFactory
	newTicker tickerFactory

	mu          sync.Mutex
	watchCtx    context.Context
	watchCancel context.CancelFunc
	shutdown    bool
	wg          sync.WaitGroup
}

func (p *provider) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (*confmap.Retrieved, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.isShutdown() {
		return nil, errors.New("openbao provider: retrieve called after shutdown")
	}

	ref, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}

	backend, watchConfig, err := p.newStore()
	if err != nil {
		return nil, fmt.Errorf("openbao provider: configure client: %w", err)
	}

	result, err := backend.GetValue(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("openbao provider: read secret %q from mount %q: %w", ref.Path, ref.Mount, err)
	}

	var opts []confmap.RetrievedOption
	if watcher != nil && watchConfig.Enabled {
		closeWatch, err := p.startWatch(backend, ref, result.Version, watchConfig.Interval, watcher)
		if err != nil {
			return nil, err
		}
		opts = append(opts, confmap.WithRetrievedClose(closeWatch))
	}

	if ref.Field != "" {
		retrieved, err := confmap.NewRetrieved(result.Value, opts...)
		if err != nil {
			return nil, fmt.Errorf("openbao provider: create retrieved value for field %q: %w", ref.Field, err)
		}
		return retrieved, nil
	}

	retrieved, err := confmap.NewRetrievedFromYAML([]byte(result.Value), opts...)
	if err != nil {
		return nil, fmt.Errorf("openbao provider: create retrieved configuration: %w", err)
	}
	return retrieved, nil
}

func (*provider) Scheme() string {
	return schemeName
}

func (p *provider) Shutdown(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	if !p.shutdown {
		p.shutdown = true
		if p.watchCancel != nil {
			p.watchCancel()
		}
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *provider) startWatch(backend store, ref SecretRef, version int, interval time.Duration, watcher confmap.WatcherFunc) (confmap.CloseFunc, error) {
	p.mu.Lock()
	if p.shutdown {
		p.mu.Unlock()
		return nil, errors.New("openbao provider: cannot start watch after shutdown")
	}
	if p.watchCtx == nil {
		p.watchCtx, p.watchCancel = context.WithCancel(context.Background())
	}
	watchCtx, cancel := context.WithCancel(p.watchCtx)
	p.wg.Add(1)
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer p.wg.Done()
		defer close(done)
		p.watch(watchCtx, backend, ref, version, interval, watcher)
	}()

	return func(ctx context.Context) error {
		cancel()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, nil
}

func (p *provider) watch(ctx context.Context, backend store, ref SecretRef, version int, interval time.Duration, watcher confmap.WatcherFunc) {
	newTicker := p.newTicker
	if newTicker == nil {
		newTicker = func(interval time.Duration) watchTicker {
			return realWatchTicker{ticker: time.NewTicker(interval)}
		}
	}

	ticker := newTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			current, err := backend.CurrentVersion(ctx, ref)
			if err != nil {
				continue
			}
			if current == version {
				continue
			}
			version = current
			watcher(&confmap.ChangeEvent{})
		}
	}
}

func (p *provider) isShutdown() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.shutdown
}
