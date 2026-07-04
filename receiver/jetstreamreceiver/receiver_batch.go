package jetstreamreceiver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/zap"
)

func (r *jetstreamReceiver) batchWorkerLoop(ctx context.Context, consumer jetstream.Consumer) {
	defer r.workersWG.Done()

	for {
		if ctx.Err() != nil {
			return
		}
		err := r.processBatchOnce(ctx, consumer)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Error("failed to process jetstream batch", zap.Error(err))
		}
	}
}

func (r *jetstreamReceiver) processBatchOnce(ctx context.Context, consumer jetstream.Consumer) error {
	fetchCtx, fetchCancel := context.WithTimeout(ctx, r.cfg.BatchMaxWait)
	defer fetchCancel()

	r.logger.Debug("fetching batch messages",
		zap.String("stream", r.cfg.Stream),
		zap.String("consumer", r.cfg.ConsumerName),
	)

	batch, err := consumer.Fetch(int(r.cfg.BatchMaxMessages), jetstream.FetchContext(fetchCtx))
	if err != nil {
		return fmt.Errorf("fetch jetstream batch: %w", err)
	}

	msgs := make([]jetstream.Msg, 0, r.cfg.BatchMaxMessages)
	for msg := range batch.Messages() {
		msgs = append(msgs, msg)
	}

	if batchErr := batch.Error(); batchErr != nil && ctx.Err() == nil {
		r.logger.Error("jetstream batch error", zap.Error(batchErr))
	}
	if len(msgs) == 0 {
		return nil
	}

	r.logger.Debug("handling batch messages",
		zap.String("stream", r.cfg.Stream),
		zap.String("consumer", r.cfg.ConsumerName),
		zap.Int("size", len(msgs)),
	)

	for i := range msgs {
		if err := msgs[i].InProgress(); err != nil {
			r.logger.Error("failed to mark jetstream message in progress", zap.Error(err))
			continue
		}
	}

	if err := r.handleMessages(ctx, msgs, r.cfg.BatchGroupBySubject); err != nil {
		if ctx.Err() != nil {
			return err
		}
		return fmt.Errorf("handle jetstream batch: %w", err)
	}

	return nil
}

func (r *jetstreamReceiver) handleMessages(ctx context.Context, msgs []jetstream.Msg, groupBySubject bool) error {
	signal := signalForKind(r.kind)
	start := time.Now()
	r.metrics.recordBatchAttempt(ctx, signal, len(msgs))

	decoded := make([]decodedMessage, 0, len(msgs))
	for i := range msgs {
		logReceivedMessage(msgs[i], r.logger)
		r.metrics.recordConsumeAttempt(ctx, signal, msgs[i].Subject())

		msg, err := decodeMessage(msgs[i], r.cfg.Compression, r.logger)
		if err != nil {
			_ = msgs[i].TermWithReason(reasonInvalidCompressedPayload)
			r.metrics.recordConsumeFailure(ctx, signal, "decode_payload", msgs[i].Subject())
			r.metrics.recordBatchDropped(ctx, signal, 1)
			continue
		}
		decoded = append(decoded, msg)
	}

	if len(decoded) == 0 {
		return nil
	}

	if groupBySubject {
		subjects, groups := groupDecodedMessagesBySubject(decoded)
		for _, subject := range subjects {
			if err := r.handleDecodedMessages(ctx, groups[subject]); err != nil {
				r.metrics.recordBatchFailure(ctx, signal, batchStageConsume, time.Since(start))
				return err
			}
		}
	} else {
		if err := r.handleDecodedMessages(ctx, decoded); err != nil {
			r.metrics.recordBatchFailure(ctx, signal, batchStageConsume, time.Since(start))
			return err
		}
	}

	r.metrics.recordBatchSuccess(ctx, signal, time.Since(start))
	return nil
}

func groupDecodedMessagesBySubject(msgs []decodedMessage) ([]string, map[string][]decodedMessage) {
	groups := make(map[string][]decodedMessage)
	subjects := make([]string, 0, len(msgs))

	for i := range msgs {
		subject := msgs[i].Subject
		groups[subject] = append(groups[subject], msgs[i])
		if len(groups[subject]) == 1 {
			subjects = append(subjects, subject)
		}
	}

	return subjects, groups
}

func (r *jetstreamReceiver) handleDecodedMessages(ctx context.Context, msgs []decodedMessage) error {
	if len(msgs) == 0 {
		return nil
	}

	signal := signalForKind(r.kind)

	var (
		valid      []decodedMessage
		consumeErr error
	)

	switch r.kind {
	case payloadLogs:
		var req *plogotlp.ExportRequest
		req, valid = r.buildBatchLogsExportRequest(ctx, signal, msgs)
		if len(valid) == 0 {
			return nil
		}
		if r.nextLogs == nil {
			return fmt.Errorf("logs consumer is not configured")
		}
		ctx = contextWithJetStreamMessageSubjects(ctx, valid)
		consumeErr = r.nextLogs.ConsumeLogs(ctx, req.Logs())
	case payloadMetrics:
		var req *pmetricotlp.ExportRequest
		req, valid = r.buildBatchMetricsExportRequest(ctx, signal, msgs)
		if len(valid) == 0 {
			return nil
		}
		if r.nextMetrics == nil {
			return fmt.Errorf("metrics consumer is not configured")
		}
		ctx = contextWithJetStreamMessageSubjects(ctx, valid)
		consumeErr = r.nextMetrics.ConsumeMetrics(ctx, req.Metrics())
	case payloadTraces:
		var req *ptraceotlp.ExportRequest
		req, valid = r.buildBatchTracesExportRequest(ctx, signal, msgs)
		if len(valid) == 0 {
			return nil
		}
		if r.nextTraces == nil {
			return fmt.Errorf("traces consumer is not configured")
		}
		ctx = contextWithJetStreamMessageSubjects(ctx, valid)
		consumeErr = r.nextTraces.ConsumeTraces(ctx, req.Traces())
	case payloadProfiles:
		var req *pprofileotlp.ExportRequest
		req, valid = r.buildBatchProfilesExportRequest(ctx, signal, msgs)
		if len(valid) == 0 {
			return nil
		}
		if r.nextProfiles == nil {
			return fmt.Errorf("profiles consumer is not configured")
		}
		ctx = contextWithJetStreamMessageSubjects(ctx, valid)
		consumeErr = r.nextProfiles.ConsumeProfiles(ctx, req.Profiles())
	default:
		for i := range msgs {
			_ = msgs[i].TermWithReason(reasonUnsupportedPayloadKind)
		}
		return fmt.Errorf("unsupported payload kind")
	}

	if consumeErr != nil {
		return r.retryOrReturnErrorBatch(ctx, valid, signal, operationForKind(r.kind), consumeErr)
	}

	return r.ackDecodedMessages(ctx, signal, valid)
}

func (r *jetstreamReceiver) ackDecodedMessages(ctx context.Context, signal string, msgs []decodedMessage) error {
	var errs error
	for i := range msgs {
		if err := r.ackDecodedMessage(ctx, signal, msgs[i]); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func (r *jetstreamReceiver) retryOrReturnErrorBatch(ctx context.Context, msgs []decodedMessage, signal, operation string, err error) error {
	if consumererror.IsPermanent(err) {
		for i := range msgs {
			r.metrics.recordConsumeFailure(ctx, signal, failureStageConsumePermanent, msgs[i].Subject)
			_ = msgs[i].TermWithReason(reasonPermanentConsumerError)
		}
		return fmt.Errorf("%s: %w", operation, err)
	}

	var retryErr error
	for i := range msgs {
		msg := msgs[i]
		r.metrics.recordConsumeFailure(ctx, signal, failureStageConsumeRetryable, msg.Subject)
		msg.logRetryMessage(operation, err)
		if r.cfg.ConsumeRetryDelay > 0 {
			retryErr = errors.Join(retryErr, msg.NakWithDelay(r.cfg.ConsumeRetryDelay))
		} else {
			retryErr = errors.Join(retryErr, msg.Nak())
		}
	}
	return errors.Join(fmt.Errorf("%s: %w", operation, err), retryErr)
}

func (r *jetstreamReceiver) buildBatchLogsExportRequest(ctx context.Context, signal string, msgs []decodedMessage) (*plogotlp.ExportRequest, []decodedMessage) {
	req := plogotlp.NewExportRequest()
	valid := make([]decodedMessage, 0, len(msgs))

	for i := range msgs {
		format, err := payloadFormatFromHeaders(msgs[i].Headers)
		if r.rejectBatchMessageIfInvalid(ctx, signal, msgs[i], err, reasonUnsupportedContentType, failureStageParseContentType) {
			continue
		}
		part := plogotlp.NewExportRequest()
		err = unmarshalPayload(part, msgs[i].Payload, format)
		if r.rejectBatchMessageIfInvalid(ctx, signal, msgs[i], err, reasonInvalidLogsPayload, failureStageUnmarshal) {
			continue
		}
		part.Logs().ResourceLogs().MoveAndAppendTo(req.Logs().ResourceLogs())
		valid = append(valid, msgs[i])
	}

	return &req, valid
}

func (r *jetstreamReceiver) buildBatchMetricsExportRequest(ctx context.Context, signal string, msgs []decodedMessage) (*pmetricotlp.ExportRequest, []decodedMessage) {
	req := pmetricotlp.NewExportRequest()
	valid := make([]decodedMessage, 0, len(msgs))

	for i := range msgs {
		format, err := payloadFormatFromHeaders(msgs[i].Headers)
		if r.rejectBatchMessageIfInvalid(ctx, signal, msgs[i], err, reasonUnsupportedContentType, failureStageParseContentType) {
			continue
		}
		part := pmetricotlp.NewExportRequest()
		err = unmarshalPayload(part, msgs[i].Payload, format)
		if r.rejectBatchMessageIfInvalid(ctx, signal, msgs[i], err, reasonInvalidMetricsPayload, failureStageUnmarshal) {
			continue
		}
		part.Metrics().ResourceMetrics().MoveAndAppendTo(req.Metrics().ResourceMetrics())
		valid = append(valid, msgs[i])
	}

	return &req, valid
}

func (r *jetstreamReceiver) buildBatchTracesExportRequest(ctx context.Context, signal string, msgs []decodedMessage) (*ptraceotlp.ExportRequest, []decodedMessage) {
	req := ptraceotlp.NewExportRequest()
	valid := make([]decodedMessage, 0, len(msgs))

	for i := range msgs {
		format, err := payloadFormatFromHeaders(msgs[i].Headers)
		if r.rejectBatchMessageIfInvalid(ctx, signal, msgs[i], err, reasonUnsupportedContentType, failureStageParseContentType) {
			continue
		}
		part := ptraceotlp.NewExportRequest()
		err = unmarshalPayload(part, msgs[i].Payload, format)
		if r.rejectBatchMessageIfInvalid(ctx, signal, msgs[i], err, reasonInvalidTracesPayload, failureStageUnmarshal) {
			continue
		}
		part.Traces().ResourceSpans().MoveAndAppendTo(req.Traces().ResourceSpans())
		valid = append(valid, msgs[i])
	}

	return &req, valid
}

func (r *jetstreamReceiver) buildBatchProfilesExportRequest(ctx context.Context, signal string, msgs []decodedMessage) (*pprofileotlp.ExportRequest, []decodedMessage) {
	req := pprofileotlp.NewExportRequest()
	valid := make([]decodedMessage, 0, len(msgs))

	for i := range msgs {
		format, err := payloadFormatFromHeaders(msgs[i].Headers)
		if r.rejectBatchMessageIfInvalid(ctx, signal, msgs[i], err, reasonUnsupportedContentType, failureStageParseContentType) {
			continue
		}
		part := pprofileotlp.NewExportRequest()
		err = unmarshalPayload(part, msgs[i].Payload, format)
		if r.rejectBatchMessageIfInvalid(ctx, signal, msgs[i], err, reasonInvalidProfilesPayload, failureStageUnmarshal) {
			continue
		}
		part.Profiles().ResourceProfiles().MoveAndAppendTo(req.Profiles().ResourceProfiles())
		valid = append(valid, msgs[i])
	}

	return &req, valid
}

func (r *jetstreamReceiver) rejectBatchMessageIfInvalid(ctx context.Context, signal string, msg decodedMessage, unmarshalErr error, reason string, stage string) bool {
	if unmarshalErr == nil {
		return false
	}
	_ = msg.TermWithReason(reason)
	r.metrics.recordConsumeFailure(ctx, signal, stage, msg.Subject)
	r.metrics.recordBatchDropped(ctx, signal, 1)
	return true
}
