package s3jetstream

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestObjectReferenceRoundTripAndValidation(t *testing.T) {
	at := time.Date(2026, 11, 18, 12, 0, 0, 0, time.FixedZone("east", 3600))
	id, err := NewUUIDv7()
	require.NoError(t, err)
	key, err := BuildObjectKey("custom", at, "tenant-1", id)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(key, "custom/telemetry/2026/11/18/"), "unexpected key %q", key)
	ref := ObjectReference{Version: ObjectReferenceVersion, ID: id, Signal: SignalTraces, Tenant: "tenant-1", Bucket: "otlp", Key: key, Encoding: EncodingProto, Compression: CompressionGzip, Size: 42}
	encoded, err := ref.Marshal()
	require.NoError(t, err)
	decoded, err := UnmarshalObjectReference(encoded)
	require.NoError(t, err)
	require.NoError(t, decoded.Validate(100, "otlp", "custom"))
	decoded.Key = "custom/objects/v2/renamed-object.data"
	require.NoError(t, decoded.Validate(100, "otlp", "custom"), "safe alternate key layout should be accepted")
}

func TestObjectReferenceRejectsInvalidValues(t *testing.T) {
	id, err := NewUUIDv7()
	require.NoError(t, err)
	base := ObjectReference{Version: ObjectReferenceVersion, ID: id, Signal: SignalTraces, Tenant: "tenant", Bucket: "otlp", Key: "telemetry/2026/11/18/tenant/" + id + ".otlp", Encoding: EncodingProto, Compression: CompressionNone, Size: 1}
	tests := []struct {
		name   string
		mutate func(*ObjectReference)
	}{
		{"version", func(r *ObjectReference) { r.Version++ }},
		{"signal", func(r *ObjectReference) { r.Signal = "unknown" }},
		{"tenant traversal", func(r *ObjectReference) { r.Tenant = "../other" }},
		{"key traversal", func(r *ObjectReference) { r.Key = "telemetry/../other/" + r.Tenant + "/" + r.ID + ".otlp" }},
		{"bucket", func(r *ObjectReference) { r.Bucket = "other" }},
		{"size", func(r *ObjectReference) { r.Size = 101 }},
		{"compression", func(r *ObjectReference) { r.Compression = "br" }},
		{"encoding", func(r *ObjectReference) { r.Encoding = "json" }},
		{"checksum", func(r *ObjectReference) { r.SHA256 = "not-a-checksum" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref := base
			tt.mutate(&ref)
			require.Error(t, ref.Validate(100, "otlp", ""))
		})
	}
	ref := base
	ref.Key = "other/telemetry/2026/11/18/tenant/" + ref.ID + ".otlp"
	require.Error(t, ref.Validate(100, "otlp", "telemetry"), "key outside configured prefix should be rejected")
}

func TestBuildObjectKeyRejectsUnsafeTenantAndPrefix(t *testing.T) {
	id, err := NewUUIDv7()
	require.NoError(t, err)
	for _, tenant := range []string{"", "..", "a/b", "a b"} {
		_, err := BuildObjectKey("telemetry", time.Now(), tenant, id)
		require.Error(t, err, "tenant %q should be rejected", tenant)
	}
	_, err = BuildObjectKey("a/../b", time.Now(), "tenant", id)
	require.Error(t, err, "unsafe prefix should be rejected")
}
