package jetstreamreceiver

import (
	"context"
	"strings"

	"go.opentelemetry.io/collector/client"
)

const (
	JetStreamSubjectMetadataKey          = "JetStream-Subject"
	JetStreamSubjectLastTokenMetadataKey = "JetStream-Subject-Last-Token"
)

func contextWithJetStreamSubjects(ctx context.Context, subjects ...string) context.Context {
	info := client.FromContext(ctx)
	metadata := make(map[string][]string)
	for key := range info.Metadata.Keys() {
		metadata[key] = info.Metadata.Get(key)
	}

	uniqueSubjects := uniqueStrings(subjects)
	lastTokens := make([]string, 0, len(uniqueSubjects))
	for _, subject := range uniqueSubjects {
		lastTokens = append(lastTokens, jetStreamSubjectLastToken(subject))
	}
	metadata[JetStreamSubjectMetadataKey] = uniqueSubjects
	metadata[JetStreamSubjectLastTokenMetadataKey] = lastTokens

	info.Metadata = client.NewMetadata(metadata)
	return client.NewContext(ctx, info)
}

func contextWithJetStreamMessageSubjects(ctx context.Context, msgs []decodedMessage) context.Context {
	subjects := make([]string, 0, len(msgs))
	for i := range msgs {
		subjects = append(subjects, msgs[i].Subject)
	}
	return contextWithJetStreamSubjects(ctx, subjects...)
}

func jetStreamSubjectLastToken(subject string) string {
	if index := strings.LastIndexByte(subject, '.'); index >= 0 {
		return subject[index+1:]
	}
	return subject
}

func uniqueStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
