package jetstreamreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
)

func TestContextWithJetStreamSubjects(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"existing": {"keep"}}),
	})

	ctx = contextWithJetStreamSubjects(ctx, "otel.logs.tenant-a", "plain", "otel.logs.tenant-a")
	metadata := client.FromContext(ctx).Metadata

	require.Equal(t, []string{"keep"}, metadata.Get("existing"))
	require.Equal(t, []string{"otel.logs.tenant-a", "plain"}, metadata.Get(JetStreamSubjectMetadataKey))
	require.Equal(t, []string{"tenant-a", "plain"}, metadata.Get(JetStreamSubjectLastTokenMetadataKey))
}

func TestJetStreamSubjectLastToken(t *testing.T) {
	require.Equal(t, "tenant-a", jetStreamSubjectLastToken("otel.logs.tenant-a"))
	require.Equal(t, "subject", jetStreamSubjectLastToken("subject"))
}

func TestContextWithJetStreamMessageSubjects(t *testing.T) {
	ctx := contextWithJetStreamMessageSubjects(context.Background(), []decodedMessage{
		{Subject: "otel.logs.tenant-a"},
		{Subject: "otel.logs.tenant-b"},
	})
	metadata := client.FromContext(ctx).Metadata

	require.Equal(t, []string{"otel.logs.tenant-a", "otel.logs.tenant-b"}, metadata.Get(JetStreamSubjectMetadataKey))
	require.Equal(t, []string{"tenant-a", "tenant-b"}, metadata.Get(JetStreamSubjectLastTokenMetadataKey))
}
