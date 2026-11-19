package s3

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGzipBytesRoundTrip(t *testing.T) {
	want := []byte("OTLP payload")
	compressed, err := GzipBytes(want)
	require.NoError(t, err)

	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	defer reader.Close()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
