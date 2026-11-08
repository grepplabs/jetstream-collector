package kubemappingprocessor

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer/consumererror"
)

func executeMapping(ctx context.Context, lookup ResourceLookup, mapping MappingConfig, md client.Metadata) (string, bool, error) {
	value, found, _, err := executeMappingWithIdentity(ctx, lookup, mapping, md)
	return value, found, err
}

func executeMappingWithIdentity(ctx context.Context, lookup ResourceLookup, mapping MappingConfig, md client.Metadata) (string, bool, objectIdentity, error) {
	selectors, err := resolveSelectors(ctx, mapping.Selector, md)
	if err != nil {
		return "", false, objectIdentity{}, err
	}
	return executeResolvedMapping(ctx, lookup, mapping, selectors)
}

func resolveSelectors(ctx context.Context, cfg SelectorConfig, md client.Metadata) (selectors, error) {
	selectors, ok, err := buildSelectors(ctx, cfg, md)
	if err != nil {
		return selectors, consumererror.NewPermanent(fmt.Errorf("build selector: %w", err))
	}
	if !ok {
		return selectors, consumererror.NewPermanent(errors.New("selector source metadata is missing"))
	}
	return selectors, nil
}

func executeResolvedMapping(ctx context.Context, lookup ResourceLookup, mapping MappingConfig, selectors selectors) (string, bool, objectIdentity, error) {
	objects, err := lookup.List(ctx, mapping.Resource, selectors)
	if err != nil {
		return "", false, objectIdentity{}, fmt.Errorf("source %s selector %q: %w", mapping.Resource.GVK(), selectors.String(), err)
	}
	switch len(objects) {
	case 0:
		return "", false, objectIdentity{}, consumererror.NewPermanent(fmt.Errorf("source %s selector %q matched no objects", mapping.Resource.GVK(), selectors.String()))
	case 1:
		value, found, err := extractValue(objects[0], mapping.Value)
		if err != nil {
			return "", false, objectIdentity{}, consumererror.NewPermanent(fmt.Errorf("extract value from source %s for selector %q: %w", mapping.Resource.GVK(), selectors.String(), err))
		}
		if !found {
			return "", false, objectIdentity{}, consumererror.NewPermanent(fmt.Errorf("value from source %s for selector %q was not found", mapping.Resource.GVK(), selectors.String()))
		}
		obj := objects[0]
		return value, true, objectIdentity{GVK: obj.GroupVersionKind(), Namespace: obj.GetNamespace(), Name: obj.GetName()}, nil
	default:
		return "", false, objectIdentity{}, consumererror.NewPermanent(fmt.Errorf("source %s selector %q matched %d objects", mapping.Resource.GVK(), selectors.String(), len(objects)))
	}
}
