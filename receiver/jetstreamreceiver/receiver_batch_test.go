package jetstreamreceiver

import (
	"context"
	"testing"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.uber.org/zap"
)

func TestHandleMessagesProcessesBatch(t *testing.T) {
	called := false
	r := &jetstreamReceiver{
		logger:   zap.NewNop(),
		kind:     payloadLogs,
		cfg:      &Config{Compression: sharedjetstream.CompressionNone},
		nextLogs: mustLogsConsumer(t, &called),
	}

	msg1 := encodedOTLPMessage(t, mustMarshalProto(t, plogotlp.NewExportRequest()), "application/x-protobuf", "", false)
	msg2 := encodedOTLPMessage(t, mustMarshalProto(t, plogotlp.NewExportRequest()), "application/x-protobuf", "", false)
	msgs := []jetstream.Msg{msg1, msg2}

	require.NoError(t, r.handleMessages(context.Background(), msgs, false))
	require.True(t, called)
	for _, msg := range []*testMsg{msg1, msg2} {
		require.True(t, msg.acked)
		require.Empty(t, msg.termReason)
	}
}

func TestGroupDecodedMessagesBySubject(t *testing.T) {
	msgs := []decodedMessage{
		{Subject: "resource.cpu"},
		{Subject: "resource.memory"},
		{Subject: "resource.cpu"},
		{Subject: "resource.disk"},
	}

	subjects, groups := groupDecodedMessagesBySubject(msgs)

	require.Equal(t, []string{"resource.cpu", "resource.memory", "resource.disk"}, subjects)
	require.Len(t, groups["resource.cpu"], 2)
	require.Len(t, groups["resource.memory"], 1)
	require.Len(t, groups["resource.disk"], 1)
}
