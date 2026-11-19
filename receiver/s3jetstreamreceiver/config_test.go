package s3jetstreamreceiver

import (
	"testing"

	protocol "github.com/grepplabs/jetstream-collector/pkg/s3jetstream"
	"github.com/stretchr/testify/require"
)

func validReceiverConfig() *Config {
	c := NewDefaultConfig()
	c.S3.Bucket = "otlp"
	c.S3.Credentials.AccessKeyID = "access"
	c.S3.Credentials.SecretAccessKey = "secret"
	c.JetStream.Stream = "OTLP"
	c.JetStream.Consumer = "traces"
	c.JetStream.Subject = "custom.traces"
	c.Cleanup.Subject = "custom.cleanup"
	c.Signal = protocol.SignalTraces
	return c
}

func TestReceiverConfigRequiresSignalAndValidNamespace(t *testing.T) {
	c := validReceiverConfig()
	require.NoError(t, c.Validate())
	c.Signal = "mixed"
	require.ErrorContains(t, c.Validate(), "signal")
	c = validReceiverConfig()
	c.S3.Prefix = "unsafe/../prefix"
	require.ErrorContains(t, c.Validate(), "s3.prefix")
}

func TestMaxObjectSizeDefaultsWhenOmitted(t *testing.T) {
	c := validReceiverConfig()
	c.Processing.MaxObjectSize = 0
	require.NoError(t, c.Validate())
	require.Equal(t, defaultMaxObjectSize, c.Processing.MaxObjectSize)
	c.Processing.MaxObjectSize = -1
	require.ErrorContains(t, c.Validate(), "must not be negative")
}

func TestAckProgressIsOptIn(t *testing.T) {
	c := NewDefaultConfig()
	require.False(t, c.Processing.AckProgress)
	c.Processing.AckProgress = true
	require.True(t, c.Processing.AckProgress)
}
