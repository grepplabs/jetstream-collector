package jetstreamreceiver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/grepplabs/jetstream-collector/pkg/payloadresolver"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
)

// inlinePayloadResolver is the default resolver for messages that already
// contain an OTLP payload.
type inlinePayloadResolver struct{}

var errUnsupportedPayloadMediaType = errors.New("unsupported content-type")

func unsupportedPayloadMediaType(mediaType string, cause error) error {
	if cause != nil {
		return fmt.Errorf("%w %q: %w", errUnsupportedPayloadMediaType, mediaType, cause)
	}
	return fmt.Errorf("%w %q", errUnsupportedPayloadMediaType, mediaType)
}

func (inlinePayloadResolver) Resolve(_ context.Context, input payloadresolver.Input) (payloadresolver.ResolvedPayload, error) {
	return payloadresolver.ResolvedPayload{Data: input.Data, MediaType: input.MediaType}, nil
}

func (r *jetstreamReceiver) resolvePayload(ctx context.Context, msg decodedMessage) (decodedMessage, error) {
	mediaType, err := payloadMediaTypeFromHeaders(msg.Headers)
	if err != nil {
		return msg, consumererror.NewPermanent(unsupportedPayloadMediaType(headerValue(msg.Headers, sharedjetstream.HeaderContentType), err))
	}
	resolver, ok := r.resolvers[mediaType]
	if !ok {
		if _, isOTLP := payloadFormatsByMediaType[mediaType]; !isOTLP {
			return msg, consumererror.NewPermanent(unsupportedPayloadMediaType(mediaType, errors.New("no payload resolver registered")))
		}
		resolver = inlinePayloadResolver{}
	}
	resolved, err := resolver.Resolve(ctx, payloadresolver.Input{Data: msg.Payload, MediaType: mediaType})
	if err != nil {
		return msg, err
	}
	if resolved.MediaType == "" {
		return msg, consumererror.NewPermanent(unsupportedPayloadMediaType(resolved.MediaType, errors.New("payload resolver returned an empty media type")))
	}
	resolvedMediaType, err := payloadresolver.NormalizeMediaType(resolved.MediaType)
	if err != nil {
		return msg, consumererror.NewPermanent(unsupportedPayloadMediaType(resolved.MediaType, err))
	}
	if _, ok := payloadFormatsByMediaType[resolvedMediaType]; !ok {
		return msg, consumererror.NewPermanent(unsupportedPayloadMediaType(resolvedMediaType, errors.New("unsupported resolved payload media type")))
	}
	msg.Payload = resolved.Data
	if resolvedMediaType != mediaType {
		msg.Headers = cloneHeaders(msg.Headers)
		msg.Headers.Set(sharedjetstream.HeaderContentType, resolvedMediaType)
	}
	return msg, nil
}

func buildResolverRegistry(host component.Host, resolverIDs []component.ID) (map[string]payloadresolver.Resolver, error) {
	registry := make(map[string]payloadresolver.Resolver)
	if len(resolverIDs) == 0 {
		return registry, nil
	}
	if host == nil {
		return nil, fmt.Errorf("resolver extensions are configured but component host is nil")
	}
	extensions := host.GetExtensions()

	owners := make(map[string]string)
	registrationNames := make(map[string]struct{})
	for _, id := range resolverIDs {
		extension, ok := extensions[id]
		if !ok {
			return nil, fmt.Errorf("resolver extension %q is not available", id)
		}
		provider, ok := extension.(payloadresolver.ResolverExtension)
		if !ok {
			return nil, fmt.Errorf("extension %q does not provide payload resolvers", id)
		}
		for _, registration := range provider.Resolvers() {
			name := strings.TrimSpace(registration.Name)
			if name == "" {
				return nil, fmt.Errorf("resolver extension %q returned a registration with an empty name", id)
			}
			registrationKey := id.String() + "/" + name
			if _, exists := registrationNames[registrationKey]; exists {
				return nil, fmt.Errorf("resolver extension %q registered duplicate resolver name %q", id, name)
			}
			registrationNames[registrationKey] = struct{}{}
			if registration.Resolver == nil {
				return nil, fmt.Errorf("resolver %q from extension %q has no implementation", name, id)
			}
			if len(registration.MediaTypes) == 0 {
				return nil, fmt.Errorf("resolver %q from extension %q declares no media types", name, id)
			}
			for _, declaredMediaType := range registration.MediaTypes {
				mediaType, err := payloadresolver.NormalizeMediaType(declaredMediaType)
				if err != nil {
					return nil, fmt.Errorf("resolver %q from extension %q: %w", name, id, err)
				}
				if owner, exists := owners[mediaType]; exists {
					return nil, fmt.Errorf("media type %q is registered by both %s and %s/%s", mediaType, owner, id, name)
				}
				owners[mediaType] = id.String() + "/" + name
				registry[mediaType] = registration.Resolver
			}
		}
	}
	return registry, nil
}

func cloneHeaders(headers nats.Header) nats.Header {
	cloned := make(nats.Header, len(headers))
	for name, values := range headers {
		cloned[name] = append([]string(nil), values...)
	}
	return cloned
}

var _ payloadresolver.Resolver = inlinePayloadResolver{}
