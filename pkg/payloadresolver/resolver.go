// Package payloadresolver defines the transport-neutral contract for resolving
// message payloads into inline OTLP payloads.
package payloadresolver

import (
	"context"
	"fmt"
	"mime"
	"strings"
)

// Input is the message body after transport-level decoding, together with its
// normalized media type (lowercase media type without parameters). A resolver
// may interpret the body as an envelope and retrieve the actual payload from
// another system.
type Input struct {
	Data      []byte
	MediaType string
}

// ResolvedPayload contains the inline OTLP payload and the media type that describes
// those bytes. Data must be ready for the receiver's OTLP unmarshaler.
type ResolvedPayload struct {
	Data      []byte
	MediaType string
}

// Resolver converts an input message representation into an inline OTLP
// payload. Implementations can perform external retrieval, such as downloading
// an object referenced by a CloudEvent.
type Resolver interface {
	Resolve(context.Context, Input) (ResolvedPayload, error)
}

// Registration associates a name and one or more input media types with a
// resolver supplied by an extension.
type Registration struct {
	Name       string
	MediaTypes []string
	Resolver   Resolver
}

// ResolverProvider is implemented by an extension that provides one or more
// payload resolvers. Receivers build a media-type-to-resolver map from these
// registrations and reject duplicate media-type claims.
type ResolverProvider interface {
	Resolvers() []Registration
}

// ResolverExtension is the capability an extension exposes for resolver
// discovery. The extension's Collector component lifecycle is separate from
// this payload-resolution contract.
type ResolverExtension interface {
	ResolverProvider
}

// NormalizeMediaType parses a media type, removes parameters, and lowercases
// it for stable registry lookup.
func NormalizeMediaType(value string) (string, error) {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(value))
	if err != nil {
		return "", fmt.Errorf("parse media type %q: %w", value, err)
	}
	return strings.ToLower(mediaType), nil
}
