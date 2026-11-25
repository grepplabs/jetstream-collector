// Package s3event defines transport-neutral events emitted after S3 object uploads.
package s3event

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

const (
	CloudEventsVersion   = "1.0"
	CloudEventsMediaType = "application/cloudevents+json"
	ObjectCreatedType    = "com.grepplabs.s3.object.created.v1"
	ObjectSchema         = "urn:grepplabs:s3-object-reference:v1"
)

// CloudEvent is a CloudEvents 1.0 structured event carrying an S3 object reference.
type CloudEvent struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	Time            time.Time       `json:"time"`
	DataContentType string          `json:"datacontenttype"`
	DataSchema      string          `json:"dataschema"`
	Data            ObjectReference `json:"data"`
}

// ObjectReference identifies the stored bytes without embedding the telemetry payload.
type ObjectReference struct {
	Bucket          string         `json:"bucket"`
	Key             string         `json:"key"`
	Signal          string         `json:"signal"`
	API             string         `json:"api"`
	ContentType     string         `json:"content_type"`
	ContentEncoding string         `json:"content_encoding,omitempty"`
	Size            int64          `json:"size"`
	SHA256          string         `json:"sha256,omitempty"`
	UploadedAt      time.Time      `json:"uploaded_at"`
	Attributes      map[string]any `json:"attributes,omitempty"`
}

// Notifier accepts a CloudEvent. Implementations should return only after their
// delivery attempt has completed; delivery guarantees are implementation-specific.
type Notifier interface {
	Notify(context.Context, CloudEvent) error
}

// NewObjectCreated creates a versioned CloudEvent with an ID stable for a given
// bucket and key, allowing consumers to deduplicate Collector retries.
func NewObjectCreated(source, bucket, key string, ref ObjectReference) (CloudEvent, error) {
	if source == "" || bucket == "" || key == "" {
		return CloudEvent{}, fmt.Errorf("source, bucket, and key are required")
	}
	if ref.Bucket != bucket || ref.Key != key {
		return CloudEvent{}, fmt.Errorf("object reference bucket and key must match event identity")
	}
	h := sha256.Sum256([]byte(bucket + "\x00" + key))
	return CloudEvent{
		SpecVersion:     CloudEventsVersion,
		ID:              hex.EncodeToString(h[:]),
		Source:          source,
		Type:            ObjectCreatedType,
		Time:            ref.UploadedAt,
		DataContentType: "application/json",
		DataSchema:      ObjectSchema,
		Data:            ref,
	}, nil
}
