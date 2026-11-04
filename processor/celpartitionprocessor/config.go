package celpartitionprocessor

import (
	"errors"
	"fmt"
	"strings"
)

const (
	OnErrorReject = "reject"
	OnErrorDrop   = "drop"
)

type PartitionByConfig struct {
	Logs    []string `mapstructure:"logs"`
	Metrics []string `mapstructure:"metrics"`
	Traces  []string `mapstructure:"traces"`
}

type Config struct {
	PartitionBy PartitionByConfig `mapstructure:"partition_by"`
	OnError     string            `mapstructure:"on_error"`
}

func NewDefaultConfig() *Config { return &Config{OnError: OnErrorDrop} }

func (c *Config) Validate() error {
	var errs error
	for name, expressions := range map[string][]string{"logs": c.PartitionBy.Logs, "metrics": c.PartitionBy.Metrics, "traces": c.PartitionBy.Traces} {
		for i, expression := range expressions {
			if strings.TrimSpace(expression) == "" {
				errs = errors.Join(errs, fmt.Errorf("partition_by.%s[%d] cannot be empty", name, i))
			}
		}
	}
	switch strings.ToLower(strings.TrimSpace(c.OnError)) {
	case "":
		c.OnError = OnErrorDrop
	case OnErrorReject, OnErrorDrop:
		c.OnError = strings.ToLower(strings.TrimSpace(c.OnError))
	default:
		errs = errors.Join(errs, fmt.Errorf("invalid on_error %q: must be reject or drop", c.OnError))
	}
	return errs
}
