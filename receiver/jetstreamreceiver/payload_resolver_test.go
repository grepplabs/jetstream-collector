package jetstreamreceiver

import (
	"context"
	"testing"

	sharedjetstream "github.com/grepplabs/jetstream-collector/pkg/jetstream"
	"github.com/grepplabs/jetstream-collector/pkg/payloadresolver"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
)

const testResolverMediaType = "application/vnd.test-envelope+json"

type testPayloadResolver struct {
	input  payloadresolver.Input
	result payloadresolver.ResolvedPayload
}

func (r *testPayloadResolver) Resolve(_ context.Context, input payloadresolver.Input) (payloadresolver.ResolvedPayload, error) {
	r.input = input
	return r.result, nil
}

type testResolverExtension struct {
	registrations []payloadresolver.Registration
}

func (e *testResolverExtension) Start(context.Context, component.Host) error { return nil }
func (e *testResolverExtension) Shutdown(context.Context) error              { return nil }
func (e *testResolverExtension) Resolvers() []payloadresolver.Registration   { return e.registrations }

type testResolverHost struct {
	component.Host
	extensions map[component.ID]component.Component
}

type testPlainExtension struct{}

func (testPlainExtension) Start(context.Context, component.Host) error { return nil }
func (testPlainExtension) Shutdown(context.Context) error              { return nil }

func (h *testResolverHost) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

func TestBuildResolverRegistryUsesOnlyConfiguredExtensions(t *testing.T) {
	firstID := component.MustNewIDWithName("testresolver", "first")
	secondID := component.MustNewIDWithName("testresolver", "second")
	firstResolver := &testPayloadResolver{}
	secondResolver := &testPayloadResolver{}
	host := &testResolverHost{extensions: map[component.ID]component.Component{
		firstID: &testResolverExtension{registrations: []payloadresolver.Registration{{
			Name: "first", MediaTypes: []string{testResolverMediaType}, Resolver: firstResolver,
		}}},
		secondID: &testResolverExtension{registrations: []payloadresolver.Registration{{
			Name: "second", MediaTypes: []string{"application/vnd.other-envelope+json"}, Resolver: secondResolver,
		}}},
	}}

	registry, err := buildResolverRegistry(host, []component.ID{firstID})
	require.NoError(t, err)
	require.Len(t, registry, 1)
	require.Same(t, firstResolver, registry[testResolverMediaType])
	_, exists := registry["application/vnd.other-envelope+json"]
	require.False(t, exists)
}

func TestBuildResolverRegistryRejectsInvalidSelection(t *testing.T) {
	testID := component.MustNewIDWithName("testresolver", "one")
	plainID := component.MustNewIDWithName("plain", "one")
	host := &testResolverHost{extensions: map[component.ID]component.Component{
		testID: &testResolverExtension{registrations: []payloadresolver.Registration{{
			Name: "one", MediaTypes: []string{testResolverMediaType}, Resolver: &testPayloadResolver{},
		}}},
		plainID: testPlainExtension{},
	}}

	_, err := buildResolverRegistry(host, []component.ID{component.MustNewIDWithName("missing", "one")})
	require.ErrorContains(t, err, "not available")

	_, err = buildResolverRegistry(host, []component.ID{plainID})
	require.ErrorContains(t, err, "does not provide payload resolvers")

	otherID := component.MustNewIDWithName("testresolver", "other")
	host.extensions[otherID] = &testResolverExtension{registrations: []payloadresolver.Registration{{
		Name: "other", MediaTypes: []string{testResolverMediaType}, Resolver: &testPayloadResolver{},
	}}}
	_, err = buildResolverRegistry(host, []component.ID{testID, otherID})
	require.ErrorContains(t, err, "registered by both")
}

func TestResolvePayloadUsesReceiverResolver(t *testing.T) {
	resolver := &testPayloadResolver{result: payloadresolver.ResolvedPayload{
		Data: []byte("resolved OTLP"), MediaType: sharedjetstream.MIMEContentTypeJSON,
	}}
	receiver := &jetstreamReceiver{resolvers: map[string]payloadresolver.Resolver{
		testResolverMediaType: resolver,
	}}
	message := decodedMessage{
		Payload: []byte("event envelope"),
		Headers: nats.Header{sharedjetstream.HeaderContentType: []string{testResolverMediaType}},
	}

	resolved, err := receiver.resolvePayload(context.Background(), message)
	require.NoError(t, err)
	require.Equal(t, []byte("event envelope"), resolver.input.Data)
	require.Equal(t, testResolverMediaType, resolver.input.MediaType)
	require.Equal(t, []byte("resolved OTLP"), resolved.Payload)
	require.Equal(t, sharedjetstream.MIMEContentTypeJSON, resolved.Headers.Get(sharedjetstream.HeaderContentType))
}

func TestConfigValidateResolverIDs(t *testing.T) {
	validConfig := func() *Config {
		return &Config{URL: "nats://localhost:4222", Stream: "events", Subject: "events.input", ConsumerName: "reader"}
	}

	goodID := component.MustNewIDWithName("s3eventresolver", "east")
	cfg := validConfig()
	cfg.Resolvers = []component.ID{goodID}
	require.NoError(t, cfg.Validate())

	cfg = validConfig()
	cfg.Resolvers = []component.ID{{}, {}}
	require.ErrorContains(t, cfg.Validate(), "must include an extension type")

	cfg = validConfig()
	cfg.Resolvers = []component.ID{goodID, goodID}
	require.ErrorContains(t, cfg.Validate(), "duplicate resolver extension")
}
