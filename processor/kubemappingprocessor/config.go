package kubemappingprocessor

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

type Config struct {
	KubeConfig `mapstructure:",squash"`
	Cache      CacheConfig     `mapstructure:"cache"`
	ErrorMode  ErrorMode       `mapstructure:"error_mode"`
	Logger     LoggerConfig    `mapstructure:"logger"`
	Mappings   []MappingConfig `mapstructure:"mappings"`
}

type ErrorMode string

const (
	ErrorModePropagate ErrorMode = "propagate"
	ErrorModeIgnore    ErrorMode = "ignore"
)

type CacheConfig struct {
	TTL         time.Duration `mapstructure:"ttl"`
	Capacity    uint64        `mapstructure:"capacity"`
	CacheMisses bool          `mapstructure:"cache_misses"`
}

type LoggerConfig struct {
	Development     bool   `mapstructure:"development"`
	Encoder         string `mapstructure:"encoder"`
	Level           string `mapstructure:"level"`
	StacktraceLevel string `mapstructure:"stacktrace_level"`
}

type KubeConfig struct {
	Kubeconfig string `mapstructure:"kubeconfig"`
	Context    string `mapstructure:"context"`
}

type MappingConfig struct {
	Resource ResourceConfig `mapstructure:"resource"`
	Selector SelectorConfig `mapstructure:"selector"`
	Value    ValueConfig    `mapstructure:"value"`
	Target   string         `mapstructure:"target"`
}

type ResourceConfig struct {
	Group     string `mapstructure:"group"`
	Version   string `mapstructure:"version"`
	Kind      string `mapstructure:"kind"`
	Namespace string `mapstructure:"namespace"`
}

func (r ResourceConfig) GVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: r.Group, Version: r.Version, Kind: r.Kind}
}

type SelectorConfig struct {
	Labels []string `mapstructure:"labels"`
	Fields []string `mapstructure:"fields"`
}

type ValueConfig struct {
	Field string `mapstructure:"field"`
	Label string `mapstructure:"label"`
}

func NewDefaultConfig() *Config {
	return &Config{
		Cache: CacheConfig{
			TTL:         time.Minute,
			Capacity:    10_000,
			CacheMisses: true,
		},
		ErrorMode: ErrorModePropagate,
		Mappings:  []MappingConfig{},
	}
}

func (c *Config) Validate() error {
	if c == nil {
		return errors.New("config is nil")
	}
	var all error
	if c.Cache.TTL < 0 {
		all = errors.Join(all, errors.New("cache.ttl cannot be negative"))
	}
	if c.Cache.TTL > 0 && c.Cache.Capacity == 0 {
		all = errors.Join(all, errors.New("cache.capacity must be greater than zero when cache is enabled"))
	}
	if c.ErrorMode != "" && c.ErrorMode != ErrorModePropagate && c.ErrorMode != ErrorModeIgnore {
		all = errors.Join(all, fmt.Errorf("error_mode must be %q or %q", ErrorModePropagate, ErrorModeIgnore))
	}
	if err := c.Logger.Validate(); err != nil {
		all = errors.Join(all, err)
	}
	for i, m := range c.Mappings {
		var err error
		if strings.TrimSpace(m.Resource.Version) == "" {
			err = errors.Join(err, errors.New("resource.version is required"))
		}
		if strings.TrimSpace(m.Resource.Kind) == "" {
			err = errors.Join(err, errors.New("resource.kind is required"))
		}
		if len(m.Selector.Labels)+len(m.Selector.Fields) == 0 {
			err = errors.Join(err, errors.New("at least one selector is required"))
		}
		for j, s := range append(append([]string{}, m.Selector.Labels...), m.Selector.Fields...) {
			if strings.TrimSpace(s) == "" {
				err = errors.Join(err, fmt.Errorf("selector[%d] cannot be empty", j))
				continue
			}
			if e := validateHeaderTemplate(s); e != nil {
				err = errors.Join(err, fmt.Errorf("selector[%d]: %w", j, e))
			}
		}
		if _, e := configuredFieldPaths(m.Selector.Fields); e != nil {
			err = errors.Join(err, fmt.Errorf("field selectors: %w", e))
		}
		field, label := strings.TrimSpace(m.Value.Field), strings.TrimSpace(m.Value.Label)
		if (field == "") == (label == "") {
			err = errors.Join(err, errors.New("value must contain exactly one of field or label"))
		}
		if field != "" {
			if e := validateFieldPath(field); e != nil {
				err = errors.Join(err, e)
			}
		}
		if strings.TrimSpace(m.Target) == "" {
			err = errors.Join(err, errors.New("target is required"))
		}
		if err != nil {
			all = errors.Join(all, fmt.Errorf("mappings[%d]: %w", i, err))
		}
	}
	return all
}

func (c LoggerConfig) Validate() error {
	var all error
	if !isOneOf(c.Encoder, "", "json", "console") {
		all = errors.Join(all, errors.New(`logger.encoder must be "json" or "console"`))
	}
	if !isOneOf(c.Level, "", "debug", "info", "warn", "error") {
		all = errors.Join(all, errors.New(`logger.level must be one of "debug", "info", "warn", "error"`))
	}
	if !isOneOf(c.StacktraceLevel, "", "debug", "info", "warn", "error") {
		all = errors.Join(all, errors.New(`logger.stacktrace_level must be one of "debug", "info", "warn", "error"`))
	}
	return all
}

func isOneOf(v string, allowed ...string) bool {
	for _, a := range allowed {
		if strings.EqualFold(strings.TrimSpace(v), a) {
			return true
		}
	}
	return false
}

func validateFieldPath(path string) error {
	for p := range strings.SplitSeq(path, ".") {
		if strings.TrimSpace(p) == "" {
			return fmt.Errorf("invalid field path %q", path)
		}
	}
	return nil
}
