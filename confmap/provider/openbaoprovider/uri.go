package openbaoprovider

import (
	"fmt"
	"strings"
	"unicode"
)

const schemeName = "openbao"

// SecretRef identifies a secret in an OpenBao secrets engine mount.
type SecretRef struct {
	Mount string
	Path  string
	Field string
}

// ParseURI parses an OpenBao provider URI in the form
// "openbao:<mount>/<path>[#<field>]".
func ParseURI(uri string) (SecretRef, error) {
	scheme, opaque, ok := strings.Cut(uri, ":")
	if !ok {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: missing scheme", uri)
	}
	if scheme != schemeName {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: unsupported scheme %q", uri, scheme)
	}
	if opaque == "" {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: missing mount and secret path", uri)
	}
	if strings.HasPrefix(opaque, "//") {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: URL-style syntax is not supported", uri)
	}
	if strings.Contains(opaque, "?") {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: query parameters are not supported", uri)
	}
	if strings.Count(opaque, "#") > 1 {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: field selector contains an unexpected fragment delimiter", uri)
	}
	opaque, field, hasField := strings.Cut(opaque, "#")
	if hasField && field == "" {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: field selector is empty", uri)
	}
	if strings.IndexFunc(opaque, unicode.IsSpace) >= 0 || strings.IndexFunc(field, unicode.IsSpace) >= 0 {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: whitespace is not allowed", uri)
	}

	mount, path, ok := strings.Cut(opaque, "/")
	if !ok {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: missing secret path", uri)
	}
	if mount == "" {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: missing mount", uri)
	}
	if path == "" {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: missing secret path", uri)
	}
	if strings.HasPrefix(path, "/") || strings.HasSuffix(path, "/") || strings.Contains(path, "//") {
		return SecretRef{}, fmt.Errorf("invalid openbao URI %q: secret path contains an empty segment", uri)
	}

	return SecretRef{Mount: mount, Path: path, Field: field}, nil
}
