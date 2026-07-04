package openbaoprovider

import "go.opentelemetry.io/collector/confmap"

type factorySettings struct {
	config *Config
}

// FactoryOption configures the OpenBao provider factory.
type FactoryOption interface {
	apply(*factorySettings)
}

type factoryOptionFunc func(*factorySettings)

func (option factoryOptionFunc) apply(settings *factorySettings) {
	option(settings)
}

// WithConfig configures the provider explicitly instead of loading its
// bootstrap configuration from BAO_CONFIG and BAO_* environment variables.
func WithConfig(cfg Config) FactoryOption {
	return factoryOptionFunc(func(settings *factorySettings) {
		settings.config = &cfg
	})
}

// NewFactory returns a factory for an OpenBao configuration provider.
func NewFactory(options ...FactoryOption) confmap.ProviderFactory {
	settings := factorySettings{}
	for _, option := range options {
		option.apply(&settings)
	}
	return confmap.NewProviderFactory(func(confmap.ProviderSettings) confmap.Provider {
		return &provider{newStore: func() (store, WatchConfig, error) {
			cfg := Config{}
			if settings.config != nil {
				cfg = *settings.config
				if cfg.Watch.Interval == 0 {
					cfg.Watch.Interval = defaultWatchInterval
				}
			} else {
				var err error
				cfg, err = ConfigFromEnvironment()
				if err != nil {
					return nil, WatchConfig{}, err
				}
			}

			backend, err := newOpenBaoStore(cfg)
			if err != nil {
				return nil, WatchConfig{}, err
			}
			return backend, cfg.Watch, nil
		}}
	})
}
