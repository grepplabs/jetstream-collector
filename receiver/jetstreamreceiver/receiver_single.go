package jetstreamreceiver

import (
	"context"
	"fmt"
	"time"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/zap"
)

func (r *jetstreamReceiver) workerLoop(ctx context.Context) {
	defer r.workersWG.Done()

	for {
		select {
		case msg, ok := <-r.jobs:
			if !ok {
				return
			}
			if err := r.handleMessage(ctx, msg); err != nil {
				r.logger.Error("failed to handle jetstream message", zap.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *jetstreamReceiver) handleMessage(ctx context.Context, msg jetstream.Msg) error {
	start := time.Now()
	signal := signalForKind(r.kind)
	subject := msg.Subject()
	r.metrics.recordConsumeAttempt(ctx, signal, subject)
	defer func() {
		r.metrics.recordConsumeDuration(ctx, signal, subject, time.Since(start))
	}()
	logReceivedMessage(msg, r.logger)

	decoded, err := decodeMessage(msg, r.cfg.Compression, r.logger)
	if err != nil {
		_ = msg.TermWithReason(reasonInvalidCompressedPayload)
		r.metrics.recordConsumeFailure(ctx, signal, "decode_payload", subject)
		return fmt.Errorf("decode jetstream payload: %w", err)
	}
	return r.handleDecodedMessage(ctx, decoded)
}

func (r *jetstreamReceiver) handleDecodedMessage(ctx context.Context, msg decodedMessage) error {
	signal := signalForKind(r.kind)
	subject := msg.Subject
	ctx = contextWithJetStreamSubjects(ctx, subject)

	switch r.kind {
	case payloadLogs:
		req, err := r.buildLogsExportRequest(ctx, signal, msg)
		if err != nil {
			return err
		}
		consumeErr := r.nextLogs.ConsumeLogs(ctx, req.Logs())
		if consumeErr != nil {
			return r.retryOrReturnError(ctx, msg, signal, operationForKind(r.kind), consumeErr)
		}
	case payloadMetrics:
		req, err := r.buildMetricsExportRequest(ctx, signal, msg)
		if err != nil {
			return err
		}
		consumeErr := r.nextMetrics.ConsumeMetrics(ctx, req.Metrics())
		if consumeErr != nil {
			return r.retryOrReturnError(ctx, msg, signal, operationForKind(r.kind), consumeErr)
		}
	case payloadTraces:
		req, err := r.buildTracesExportRequest(ctx, signal, msg)
		if err != nil {
			return err
		}
		consumeErr := r.nextTraces.ConsumeTraces(ctx, req.Traces())
		if consumeErr != nil {
			return r.retryOrReturnError(ctx, msg, signal, operationForKind(r.kind), consumeErr)
		}
	case payloadProfiles:
		req, err := r.buildProfilesExportRequest(ctx, signal, msg)
		if err != nil {
			return err
		}
		consumeErr := r.nextProfiles.ConsumeProfiles(ctx, req.Profiles())
		if consumeErr != nil {
			return r.retryOrReturnError(ctx, msg, signal, operationForKind(r.kind), consumeErr)
		}
	default:
		_ = msg.TermWithReason(reasonUnsupportedPayloadKind)
		r.metrics.recordConsumeFailure(ctx, signal, failureStageUnsupportedKind, subject)
		return fmt.Errorf("unsupported payload kind")
	}

	return r.ackDecodedMessage(ctx, signal, msg)
}

func (r *jetstreamReceiver) ackDecodedMessage(ctx context.Context, signal string, msg decodedMessage) error {
	msg.logAckMessage()
	if err := msg.Ack(); err != nil {
		r.metrics.recordConsumeFailure(ctx, signal, failureStageAck, msg.Subject)
		return err
	}
	r.metrics.recordPayloadSize(ctx, signal, msg.Subject, len(msg.Payload))
	r.metrics.recordConsumeSuccess(ctx, signal, msg.Subject)
	return nil
}

func operationForKind(kind payloadKind) string {
	switch kind {
	case payloadLogs:
		return "consume logs"
	case payloadMetrics:
		return "consume metrics"
	case payloadTraces:
		return "consume traces"
	case payloadProfiles:
		return "consume profiles"
	default:
		return "consume batch"
	}
}

func (r *jetstreamReceiver) retryOrReturnError(ctx context.Context, msg decodedMessage, signal, operation string, err error) error {
	if consumererror.IsPermanent(err) {
		r.metrics.recordConsumeFailure(ctx, signal, failureStageConsumePermanent, msg.Subject)
		_ = msg.TermWithReason(reasonPermanentConsumerError)
		return fmt.Errorf("%s: %w", operation, err)
	}

	r.metrics.recordConsumeFailure(ctx, signal, failureStageConsumeRetryable, msg.Subject)
	msg.logRetryMessage(operation, err)

	if r.cfg.ConsumeRetryDelay > 0 {
		if nakErr := msg.NakWithDelay(r.cfg.ConsumeRetryDelay); nakErr != nil {
			return fmt.Errorf("%s: nak message: %w", operation, nakErr)
		}
	} else if nakErr := msg.Nak(); nakErr != nil {
		return fmt.Errorf("%s: nak message: %w", operation, nakErr)
	}
	return nil
}

func unmarshalPayload(req interface {
	UnmarshalProto([]byte) error
	UnmarshalJSON([]byte) error
}, payload []byte, format string) error {
	switch format {
	case sharedjetstream.ContentTypeJSON:
		return req.UnmarshalJSON(payload)
	case sharedjetstream.ContentTypeProto:
		return req.UnmarshalProto(payload)
	default:
		return fmt.Errorf("unsupported payload format %q", format)
	}
}

func (r *jetstreamReceiver) buildLogsExportRequest(ctx context.Context, signal string, msg decodedMessage) (*plogotlp.ExportRequest, error) {
	req := plogotlp.NewExportRequest()
	format, err := payloadFormatFromHeaders(msg.Headers)
	if err != nil {
		_ = msg.TermWithReason(reasonUnsupportedContentType)
		r.metrics.recordConsumeFailure(ctx, signal, failureStageParseContentType, msg.Subject)
		return nil, err
	}
	if err := unmarshalPayload(req, msg.Payload, format); err != nil {
		_ = msg.TermWithReason(reasonInvalidLogsPayload)
		r.metrics.recordConsumeFailure(ctx, signal, failureStageUnmarshal, msg.Subject)
		return nil, fmt.Errorf("unmarshal otlp logs request: %w", err)
	}
	return &req, nil
}

func (r *jetstreamReceiver) buildMetricsExportRequest(ctx context.Context, signal string, msg decodedMessage) (*pmetricotlp.ExportRequest, error) {
	req := pmetricotlp.NewExportRequest()
	format, err := payloadFormatFromHeaders(msg.Headers)
	if err != nil {
		_ = msg.TermWithReason(reasonUnsupportedContentType)
		r.metrics.recordConsumeFailure(ctx, signal, failureStageParseContentType, msg.Subject)
		return nil, err
	}
	if err := unmarshalPayload(req, msg.Payload, format); err != nil {
		_ = msg.TermWithReason(reasonInvalidMetricsPayload)
		r.metrics.recordConsumeFailure(ctx, signal, failureStageUnmarshal, msg.Subject)
		return nil, fmt.Errorf("unmarshal otlp metrics request: %w", err)
	}
	return &req, nil
}

func (r *jetstreamReceiver) buildTracesExportRequest(ctx context.Context, signal string, msg decodedMessage) (*ptraceotlp.ExportRequest, error) {
	req := ptraceotlp.NewExportRequest()
	format, err := payloadFormatFromHeaders(msg.Headers)
	if err != nil {
		_ = msg.TermWithReason(reasonUnsupportedContentType)
		r.metrics.recordConsumeFailure(ctx, signal, failureStageParseContentType, msg.Subject)
		return nil, err
	}
	if err := unmarshalPayload(req, msg.Payload, format); err != nil {
		_ = msg.TermWithReason(reasonInvalidTracesPayload)
		r.metrics.recordConsumeFailure(ctx, signal, failureStageUnmarshal, msg.Subject)
		return nil, fmt.Errorf("unmarshal otlp traces request: %w", err)
	}
	return &req, nil
}

func (r *jetstreamReceiver) buildProfilesExportRequest(ctx context.Context, signal string, msg decodedMessage) (*pprofileotlp.ExportRequest, error) {
	req := pprofileotlp.NewExportRequest()
	format, err := payloadFormatFromHeaders(msg.Headers)
	if err != nil {
		_ = msg.TermWithReason(reasonUnsupportedContentType)
		r.metrics.recordConsumeFailure(ctx, signal, failureStageParseContentType, msg.Subject)
		return nil, err
	}
	if err := unmarshalPayload(req, msg.Payload, format); err != nil {
		_ = msg.TermWithReason(reasonInvalidProfilesPayload)
		r.metrics.recordConsumeFailure(ctx, signal, failureStageUnmarshal, msg.Subject)
		return nil, fmt.Errorf("unmarshal otlp profiles request: %w", err)
	}
	return &req, nil
}
