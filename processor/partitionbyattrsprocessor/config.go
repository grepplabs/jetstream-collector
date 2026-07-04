package partitionbyattrsprocessor

import (
	"errors"
	"fmt"
	"strings"
)

type MissingAttributeAction string

const (
	MissingAttributeActionError MissingAttributeAction = "error"
	MissingAttributeActionDrop  MissingAttributeAction = "drop"
)

type PartitionByConfig struct {
	Resource  []string `mapstructure:"resource"`
	Telemetry []string `mapstructure:"telemetry"`
}

type Config struct {
	PartitionBy            PartitionByConfig      `mapstructure:"partition_by"`
	MissingAttributeAction MissingAttributeAction `mapstructure:"missing_attribute_action"`
}

func (c *Config) Validate() error {
	var errs error

	var err error
	c.PartitionBy.Resource, err = normalizeAttributeNames("partition_by.resource", c.PartitionBy.Resource)
	errs = errors.Join(errs, err)
	c.PartitionBy.Telemetry, err = normalizeAttributeNames("partition_by.telemetry", c.PartitionBy.Telemetry)
	errs = errors.Join(errs, err)

	switch strings.ToLower(strings.TrimSpace(string(c.MissingAttributeAction))) {
	case "":
		c.MissingAttributeAction = MissingAttributeActionError
	case string(MissingAttributeActionError):
		c.MissingAttributeAction = MissingAttributeActionError
	case string(MissingAttributeActionDrop):
		c.MissingAttributeAction = MissingAttributeActionDrop
	default:
		errs = errors.Join(errs, fmt.Errorf("invalid missing_attribute_action %q: must be %q or %q",
			c.MissingAttributeAction, MissingAttributeActionError, MissingAttributeActionDrop))
	}

	return errs
}

func normalizeAttributeNames(field string, attrs []string) ([]string, error) {
	normalized := make([]string, 0, len(attrs))
	seen := make(map[string]struct{}, len(attrs))
	var errs error

	for i, attr := range attrs {
		name := strings.TrimSpace(attr)
		if name == "" {
			errs = errors.Join(errs, fmt.Errorf("%s[%d] cannot be empty", field, i))
			continue
		}
		if _, ok := seen[name]; ok {
			errs = errors.Join(errs, fmt.Errorf("%s contains duplicate attribute %q", field, name))
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}

	return normalized, errs
}

func (c *Config) hasPartitionAttrs() bool {
	return len(c.PartitionBy.Resource) > 0 || len(c.PartitionBy.Telemetry) > 0
}

func (c *Config) shouldDropOnMissing() bool {
	return c.MissingAttributeAction == MissingAttributeActionDrop
}

func (c *Config) missingAttributeError(kind, name string) error {
	return fmt.Errorf("missing %s attribute %q", kind, name)
}
