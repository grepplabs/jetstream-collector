package kubemappingprocessor

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/client"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

type selectors struct {
	labels labels.Selector
	fields fields.Selector
}

func (s selectors) String() string {
	parts := make([]string, 0, 2)
	if selector := s.labels.String(); selector != "" {
		parts = append(parts, "labelSelector:"+selector)
	}
	if selector := s.fields.String(); selector != "" {
		parts = append(parts, "fieldSelector:"+selector)
	}
	return strings.Join(parts, ";")
}

func buildSelectors(ctx context.Context, cfg SelectorConfig, md client.Metadata) (selectors, bool, error) {
	expand := func(in []string) ([]string, bool, error) {
		out := make([]string, 0, len(in))
		for _, item := range in {
			value, ok, err := expandHeaderTemplate(ctx, item, md)
			if err != nil || !ok {
				return nil, ok, err
			}
			out = append(out, value)
		}
		return out, true, nil
	}
	ls, ok, err := expand(cfg.Labels)
	if err != nil || !ok {
		return selectors{}, ok, err
	}
	fs, ok, err := expand(cfg.Fields)
	if err != nil || !ok {
		return selectors{}, ok, err
	}
	labelSelector, err := labels.Parse(strings.Join(ls, ","))
	if err != nil {
		return selectors{}, false, err
	}
	fieldSelector, err := fields.ParseSelector(strings.Join(fs, ","))
	if err != nil {
		return selectors{}, false, err
	}
	if err := validateEqualityFieldSelector(fieldSelector); err != nil {
		return selectors{}, false, err
	}
	return selectors{labels: labelSelector, fields: fieldSelector}, true, nil
}

func configuredFieldPaths(expressions []string) ([]string, error) {
	unique := make(map[string]struct{})
	for _, expression := range expressions {
		parts, err := parseHeaderTemplate(expression)
		if err != nil {
			return nil, err
		}
		var shape strings.Builder
		for _, part := range parts {
			if part.Source == "" {
				shape.WriteString(part.Literal)
			} else {
				shape.WriteString("index-value")
			}
		}
		selector, err := fields.ParseSelector(shape.String())
		if err != nil {
			return nil, err
		}
		if err := validateEqualityFieldSelector(selector); err != nil {
			return nil, err
		}
		for _, requirement := range selector.Requirements() {
			unique[requirement.Field] = struct{}{}
		}
	}
	paths := make([]string, 0, len(unique))
	for path := range unique {
		paths = append(paths, path)
	}
	return paths, nil
}

func validateEqualityFieldSelector(selector fields.Selector) error {
	for _, requirement := range selector.Requirements() {
		if requirement.Operator != selection.Equals && requirement.Operator != selection.DoubleEquals {
			return fmt.Errorf("field selector %q must use equality", selector.String())
		}
	}
	return nil
}
