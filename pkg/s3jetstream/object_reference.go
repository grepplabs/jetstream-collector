package s3jetstream

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

const ObjectReferenceVersion = 1

const (
	EncodingProto   = "proto"
	CompressionNone = "none"
	CompressionGzip = "gzip"
)

type Signal string

const (
	SignalTraces   Signal = "traces"
	SignalMetrics  Signal = "metrics"
	SignalLogs     Signal = "logs"
	SignalProfiles Signal = "profiles"
)

// ObjectReference is the stable JetStream payload for both processing and cleanup.
// Size and SHA256 describe the stored (possibly compressed) object bytes.
type ObjectReference struct {
	Version     int    `json:"version"`
	ID          string `json:"id"`
	Signal      Signal `json:"signal"`
	Tenant      string `json:"tenant"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	Encoding    string `json:"encoding"`
	Compression string `json:"compression"`
	Size        int64  `json:"size,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
}

var tenantSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func ValidateTenant(tenant string) error {
	if tenant == "." || tenant == ".." || !tenantSegmentPattern.MatchString(tenant) {
		return fmt.Errorf("tenant must be a path-safe segment of 1 to 128 ASCII letters, digits, dots, underscores, or hyphens")
	}
	return nil
}

// NewUUIDv7 creates a time-ordered UUID v7 using the current time.
func NewUUIDv7() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate UUID v7: %w", err)
	}
	return id.String(), nil
}

func BuildObjectKey(prefix string, at time.Time, tenant, id string) (string, error) {
	if err := ValidateTenant(tenant); err != nil {
		return "", err
	}
	if !isUUIDv7(id) {
		return "", errors.New("id must be a UUID v7")
	}
	parts := make([]string, 0, 6)
	if p, err := normalizePrefix(prefix); err != nil {
		return "", err
	} else if p != "" {
		parts = append(parts, p)
	}
	utc := at.UTC()
	parts = append(parts, "telemetry", utc.Format("2006"), utc.Format("01"), utc.Format("02"), tenant, id+".otlp")
	return strings.Join(parts, "/"), nil
}

func ValidatePrefix(prefix string) error {
	_, err := normalizePrefix(prefix)
	return err
}

func (r ObjectReference) Validate(maxSize int64, allowedBucket, prefix string) error {
	if r.Version != ObjectReferenceVersion {
		return fmt.Errorf("unsupported object reference version %d", r.Version)
	}
	if !isUUIDv7(r.ID) {
		return errors.New("id must be a UUID v7")
	}
	switch r.Signal {
	case SignalTraces, SignalMetrics, SignalLogs, SignalProfiles:
	default:
		return fmt.Errorf("unsupported signal %q", r.Signal)
	}
	if err := ValidateTenant(r.Tenant); err != nil {
		return err
	}
	if r.Bucket == "" || (allowedBucket != "" && r.Bucket != allowedBucket) {
		return errors.New("object reference bucket is empty or not allowed")
	}
	if r.Encoding != EncodingProto {
		return fmt.Errorf("unsupported encoding %q", r.Encoding)
	}
	if r.Compression != CompressionNone && r.Compression != CompressionGzip {
		return fmt.Errorf("unsupported compression %q", r.Compression)
	}
	if r.Size < 0 || (maxSize > 0 && r.Size > maxSize) {
		return fmt.Errorf("object size %d is outside the permitted range", r.Size)
	}
	if r.SHA256 != "" {
		if len(r.SHA256) != 64 {
			return errors.New("sha256 must contain 64 hexadecimal characters")
		}
		if _, err := hex.DecodeString(r.SHA256); err != nil {
			return fmt.Errorf("invalid sha256: %w", err)
		}
	}
	return validateS3Key(r.Key, prefix)
}

func validateS3Key(key, prefix string) error {
	p, err := normalizePrefix(prefix)
	if err != nil {
		return err
	}
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "\\") {
		return errors.New("object key must be a relative S3 key")
	}
	if p != "" {
		if !strings.HasPrefix(key, p+"/") {
			return errors.New("object reference key is outside the allowed prefix")
		}
		key = strings.TrimPrefix(key, p+"/")
	}
	for segment := range strings.SplitSeq(key, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errors.New("object key contains an unsafe path segment")
		}
	}
	return nil
}

func normalizePrefix(prefix string) (string, error) {
	p := strings.Trim(prefix, "/")
	if p == "" {
		return "", nil
	}
	for segment := range strings.SplitSeq(p, "/") {
		if segment == "" || segment == "." || segment == ".." || strings.TrimSpace(segment) != segment || strings.Contains(segment, "\\") {
			return "", fmt.Errorf("invalid S3 prefix %q", prefix)
		}
	}
	return p, nil
}

func (r ObjectReference) Marshal() ([]byte, error) { return json.Marshal(r) }

func UnmarshalObjectReference(data []byte) (ObjectReference, error) {
	var ref ObjectReference
	if err := json.Unmarshal(data, &ref); err != nil {
		return ref, fmt.Errorf("decode object reference: %w", err)
	}
	return ref, nil
}

func isUUIDv7(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.Version() == 7 && id.Variant() == uuid.RFC4122
}
