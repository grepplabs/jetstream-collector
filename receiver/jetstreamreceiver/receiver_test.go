package jetstreamreceiver

import (
	"bytes"
	"compress/gzip"
	"context"
	"testing"
	"time"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type testMsg struct {
	data       []byte
	headers    nats.Header
	acked      bool
	nacked     bool
	nakDelay   time.Duration
	termReason string
}

func (m *testMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *testMsg) Data() []byte                              { return m.data }
func (m *testMsg) Headers() nats.Header                      { return m.headers }
func (m *testMsg) Subject() string                           { return "subject" }
func (m *testMsg) Reply() string                             { return "reply" }
func (m *testMsg) Ack() error                                { m.acked = true; return nil }
func (m *testMsg) DoubleAck(context.Context) error           { m.acked = true; return nil }
func (m *testMsg) Nak() error                                { m.nacked = true; return nil }
func (m *testMsg) NakWithDelay(delay time.Duration) error {
	m.nacked = true
	m.nakDelay = delay
	return nil
}
func (m *testMsg) InProgress() error                  { return nil }
func (m *testMsg) Term() error                        { return nil }
func (m *testMsg) TermWithReason(reason string) error { m.termReason = reason; return nil }

func TestNormalizeCompression(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: sharedjetstream.CompressionNone},
		{name: "none", in: "none", want: sharedjetstream.CompressionNone},
		{name: "identity", in: "identity", want: sharedjetstream.CompressionNone},
		{name: "gzip", in: "gzip", want: sharedjetstream.CompressionGzip},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sharedjetstream.NormalizeCompression(tt.in)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestPayloadCompressionFromHeaders(t *testing.T) {
	tests := []struct {
		name    string
		headers nats.Header
		def     string
		want    string
	}{
		{name: "default none", headers: nil, def: sharedjetstream.CompressionNone, want: sharedjetstream.CompressionNone},
		{name: "default gzip", headers: nil, def: sharedjetstream.CompressionGzip, want: sharedjetstream.CompressionGzip},
		{name: "header gzip overrides none", headers: nats.Header{sharedjetstream.HeaderContentEncoding: []string{"gzip"}}, def: sharedjetstream.CompressionNone, want: sharedjetstream.CompressionGzip},
		{name: "header identity overrides gzip", headers: nats.Header{sharedjetstream.HeaderContentEncoding: []string{"identity"}}, def: sharedjetstream.CompressionGzip, want: sharedjetstream.CompressionNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := payloadCompressionFromHeaders(tt.headers, tt.def)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
func TestConsumerName(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		want    string
		wantErr bool
	}{
		{name: "consumer name", cfg: &Config{ConsumerName: "shared"}, want: "shared"},
		{name: "missing", cfg: &Config{}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.cfg.consumerName()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParallelism(t *testing.T) {
	require.Equal(t, 1, (&jetstreamReceiver{cfg: &Config{}}).parallelism())
	require.Equal(t, 4, (&jetstreamReceiver{cfg: &Config{Workers: 4}}).parallelism())
}

func TestPayloadFormatFromHeaders(t *testing.T) {
	tests := []struct {
		name    string
		headers nats.Header
		want    string
		wantErr bool
	}{
		{name: "default proto", headers: nil, want: sharedjetstream.ContentTypeProto},
		{name: "json", headers: nats.Header{sharedjetstream.HeaderContentType: []string{"application/json"}}, want: sharedjetstream.ContentTypeJSON},
		{name: "json with charset", headers: nats.Header{sharedjetstream.HeaderContentType: []string{"application/json; charset=utf-8"}}, want: sharedjetstream.ContentTypeJSON},
		{name: "proto", headers: nats.Header{sharedjetstream.HeaderContentType: []string{"application/x-protobuf"}}, want: sharedjetstream.ContentTypeProto},
		{name: "unknown", headers: nats.Header{sharedjetstream.HeaderContentType: []string{"application/yaml"}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := payloadFormatFromHeaders(tt.headers)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDecodePayloadGzip(t *testing.T) {
	body := []byte("hello world")
	compressed := gzipBody(t, body)
	msg := &testMsg{data: compressed, headers: nats.Header{sharedjetstream.HeaderContentEncoding: []string{"gzip"}}}

	decoded, err := decodePayload(msg, sharedjetstream.CompressionNone)
	require.NoError(t, err)
	require.Equal(t, body, decoded)
}

func gzipBody(t *testing.T, body []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	_, err := zw.Write(body)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return b.Bytes()
}
