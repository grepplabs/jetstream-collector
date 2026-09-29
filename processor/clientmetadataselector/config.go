package clientmetadataselector

import (
	"fmt"
	"strings"
)

const (
	strategyRoundRobin = "round_robin"
	strategyRandom     = "random"
)

type Config struct {
	MetadataKey string   `mapstructure:"metadata_key"`
	Values      []string `mapstructure:"values"`
	Strategy    string   `mapstructure:"strategy"`
}

func NewDefaultConfig() *Config {
	return &Config{Strategy: strategyRoundRobin}
}

func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}
	if strings.TrimSpace(c.MetadataKey) == "" {
		return fmt.Errorf("metadata_key cannot be empty")
	}
	if c.MetadataKey != strings.TrimSpace(c.MetadataKey) {
		return fmt.Errorf("metadata_key cannot have leading or trailing whitespace")
	}
	if len(c.Values) == 0 {
		return fmt.Errorf("values must contain at least one value")
	}
	for i, value := range c.Values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("values[%d] cannot be empty", i)
		}
		if value != strings.TrimSpace(value) {
			return fmt.Errorf("values[%d] cannot have leading or trailing whitespace", i)
		}
	}
	switch c.Strategy {
	case "", strategyRoundRobin, strategyRandom:
		return nil
	default:
		return fmt.Errorf("unsupported strategy %q (supported: round_robin, random)", c.Strategy)
	}
}
