package template

import (
	"context"
	"fmt"
	"strings"
)

type Part struct {
	Literal string
	Source  string
	Key     string
}

type Resolver interface {
	ResolvePart(context.Context, Part) (string, error)
}

func Parse(pattern string, validate func(Part) error) ([]Part, error) {
	if pattern == "" {
		return nil, nil
	}

	parts := make([]Part, 0, 4)
	remaining := pattern
	for len(remaining) > 0 {
		start := strings.Index(remaining, "${")
		if start < 0 {
			parts = append(parts, Part{Literal: remaining})
			break
		}

		if start > 0 {
			parts = append(parts, Part{Literal: remaining[:start]})
		}

		rest := remaining[start+2:]
		end := strings.IndexByte(rest, '}')
		if end < 0 {
			return nil, fmt.Errorf("invalid placeholder %q: missing closing }", remaining[start:])
		}

		raw := rest[:end]
		source, key, ok := strings.Cut(raw, ":")
		if !ok || source == "" || key == "" {
			return nil, fmt.Errorf("invalid placeholder %q: expected ${source:key}", remaining[start:start+end+3])
		}

		part := Part{Source: source, Key: key}
		if validate != nil {
			if err := validate(part); err != nil {
				return nil, err
			}
		}

		parts = append(parts, part)
		remaining = rest[end+1:]
	}

	return parts, nil
}

func Resolve(ctx context.Context, parts []Part, resolver Resolver) (string, error) {
	if resolver == nil {
		return "", fmt.Errorf("resolver is nil")
	}

	var b strings.Builder
	for _, part := range parts {
		if part.Source == "" {
			b.WriteString(part.Literal)
			continue
		}

		value, err := resolver.ResolvePart(ctx, part)
		if err != nil {
			return "", err
		}
		b.WriteString(value)
	}
	return b.String(), nil
}

func ResolveValue(kind, source, key string, values []string, stable bool) (string, error) {
	if len(values) == 0 {
		return "", fmt.Errorf("missing %s value for %s:%s", kind, source, key)
	}

	if !stable {
		return values[0], nil
	}

	want := values[0]
	for _, value := range values[1:] {
		if value != want {
			return "", fmt.Errorf("conflicting %s values for %s:%s", kind, source, key)
		}
	}
	return want, nil
}
