package clientmetadataprocessor

import (
	"errors"
	"fmt"
	"strings"
)

const (
	clientMetadataSourceResource  = "resource"
	clientMetadataSourceTelemetry = "telemetry"
)

type Config struct {
	ClientMetadata []string `mapstructure:"client_metadata"`
}

func NewDefaultConfig() *Config {
	return &Config{
		ClientMetadata: []string{},
	}
}

func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}

	if _, err := parseClientMetadataSpecs(c.ClientMetadata); err != nil {
		return err
	}

	return nil
}

type clientMetadataSpec struct {
	source string
	key    string
}

func (s clientMetadataSpec) String() string {
	return s.source + ":" + s.key
}

func parseClientMetadataSpecs(entries []string) ([]clientMetadataSpec, error) {
	specs := make([]clientMetadataSpec, 0, len(entries))
	seenKeys := make(map[string]struct{}, len(entries))
	var errs error

	for i, entry := range entries {
		spec, err := parseClientMetadataSpec(entry)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("client_metadata[%d]: %w", i, err))
			continue
		}
		if _, ok := seenKeys[spec.key]; ok {
			errs = errors.Join(errs, fmt.Errorf("client_metadata contains duplicate key %q", spec.key))
			continue
		}
		seenKeys[spec.key] = struct{}{}
		specs = append(specs, spec)
	}

	return specs, errs
}

func parseClientMetadataSpec(entry string) (clientMetadataSpec, error) {
	value := strings.TrimSpace(entry)
	if value == "" {
		return clientMetadataSpec{}, fmt.Errorf("cannot be empty")
	}

	source, key, ok := strings.Cut(value, ":")
	if !ok {
		return clientMetadataSpec{}, fmt.Errorf("expected ${source:key} form")
	}

	source = strings.TrimSpace(source)
	key = strings.TrimSpace(key)
	if source == "" || key == "" {
		return clientMetadataSpec{}, fmt.Errorf("expected ${source:key} form")
	}

	switch source {
	case clientMetadataSourceResource, clientMetadataSourceTelemetry:
		return clientMetadataSpec{source: source, key: key}, nil
	default:
		return clientMetadataSpec{}, fmt.Errorf("unsupported client_metadata source %q", source)
	}
}
