package s3jetstreamexporter

import (
	"testing"

	protocol "github.com/grepplabs/jetstream-collector/pkg/s3jetstream"
	"github.com/stretchr/testify/require"
)

func validConfig() *Config {
	c := NewDefaultConfig()
	c.S3.Bucket = "otlp"
	c.S3.Credentials.AccessKeyID = "access"
	c.S3.Credentials.SecretAccessKey = "secret"
	c.Tenant.ID = "tenant-a"
	c.JetStream.Subject = "custom.traces"
	c.Signal = protocol.SignalTraces
	return c
}

func TestConfigValidateTenantSignalAndPrefix(t *testing.T) {
	c := validConfig()
	require.NoError(t, c.Validate())
	c.Tenant.MetadataKey = "x-tenant"
	require.ErrorContains(t, c.Validate(), "exactly one")
	c = validConfig()
	c.Signal = "mixed"
	require.ErrorContains(t, c.Validate(), "signal")
	c = validConfig()
	c.S3.Prefix = "safe/../unsafe"
	require.ErrorContains(t, c.Validate(), "s3.prefix")
}
