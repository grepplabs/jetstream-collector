package s3jetstreamreceiver

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	protocol "github.com/grepplabs/jetstream-collector/pkg/s3jetstream"
	"github.com/stretchr/testify/require"
)

func TestDecodeGzipObjectWithGzipExtension(t *testing.T) {
	want := []byte("OTLP protobuf payload")
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, err := zw.Write(want)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	stored := compressed.Bytes()

	id, err := protocol.NewUUIDv7()
	require.NoError(t, err)
	key, err := protocol.BuildObjectKey("telemetry", time.Now().UTC(), "tenant-a", id)
	require.NoError(t, err)
	key += ".gz"
	sum := sha256.Sum256(stored)
	ref := protocol.ObjectReference{
		Version: protocol.ObjectReferenceVersion, ID: id, Signal: protocol.SignalLogs,
		Tenant: "tenant-a", Bucket: "otlp", Key: key, Encoding: protocol.EncodingProto,
		Compression: protocol.CompressionGzip, Size: int64(len(stored)), SHA256: hex.EncodeToString(sum[:]),
	}
	require.NoError(t, ref.Validate(1024, "otlp", "telemetry"))

	got, err := decodeStoredObject(stored, ref.Compression, 1024)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
