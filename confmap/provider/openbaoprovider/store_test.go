package openbaoprovider

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	openbao "github.com/openbao/openbao/api/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenBaoStoreGetValue(t *testing.T) {
	store := newTestOpenBaoStore(t, func(r *http.Request) *http.Response {
		assert.Equal(t, "/v1/secret/data/otel/collector", r.URL.Path)
		assert.Equal(t, "test-token", r.Header.Get("X-Vault-Token"))
		return jsonResponse(http.StatusOK, `{"data":{"data":{"config":"receivers:\n  otlp: {}\n"},"metadata":{"version":1}}}`)
	})

	got, err := store.GetValue(context.Background(), SecretRef{Mount: "secret", Path: "otel/collector"})
	require.NoError(t, err)
	assert.Equal(t, storeValue{Value: "receivers:\n  otlp: {}\n", Version: 1}, got)
}

func TestOpenBaoStoreGetSelectedField(t *testing.T) {
	store := newTestOpenBaoStore(t, func(*http.Request) *http.Response {
		return jsonResponse(http.StatusOK, `{"data":{"data":{"token":"Bearer example"},"metadata":{"version":1}}}`)
	})

	got, err := store.GetValue(context.Background(), SecretRef{Mount: "secret", Path: "credentials", Field: "token"})
	require.NoError(t, err)
	assert.Equal(t, storeValue{Value: "Bearer example", Version: 1}, got)
}

func TestOpenBaoStoreGetValueValidation(t *testing.T) {
	tests := []struct {
		name       string
		response   string
		ref        SecretRef
		wantErrMsg string
	}{
		{
			name:       "missing config field",
			response:   `{"data":{"data":{"other":"value"},"metadata":{"version":1}}}`,
			wantErrMsg: `does not contain "config" field`,
		},
		{
			name:       "missing selected field",
			response:   `{"data":{"data":{"other":"value"},"metadata":{"version":1}}}`,
			ref:        SecretRef{Mount: "secret", Path: "config", Field: "token"},
			wantErrMsg: `does not contain "token" field`,
		},
		{
			name:       "config is not string",
			response:   `{"data":{"data":{"config":{"secret":"must-not-leak"}},"metadata":{"version":1}}}`,
			wantErrMsg: `field "config" must be a string, got map[string]interface {}`,
		},
		{
			name:       "selected field is not string",
			response:   `{"data":{"data":{"token":1234},"metadata":{"version":1}}}`,
			ref:        SecretRef{Mount: "secret", Path: "config", Field: "token"},
			wantErrMsg: `field "token" must be a string, got json.Number`,
		},
		{
			name:       "deleted secret",
			response:   `{"data":{"data":null,"metadata":{"version":1,"deletion_time":"2026-01-01T00:00:00Z"}}}`,
			wantErrMsg: "secret has no data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestOpenBaoStore(t, func(*http.Request) *http.Response {
				return jsonResponse(http.StatusOK, tt.response)
			})
			ref := tt.ref
			if ref == (SecretRef{}) {
				ref = SecretRef{Mount: "secret", Path: "config"}
			}
			_, err := store.GetValue(context.Background(), ref)
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErrMsg)
			assert.NotContains(t, err.Error(), "must-not-leak")
		})
	}
}

func TestOpenBaoStoreBackendError(t *testing.T) {
	store := newTestOpenBaoStore(t, func(*http.Request) *http.Response {
		return jsonResponse(http.StatusForbidden, `{"errors":["permission denied"]}`)
	})
	_, err := store.GetValue(context.Background(), SecretRef{Mount: "secret", Path: "config"})
	assert.Error(t, err)
}

func TestStoreFromEnvironmentValidation(t *testing.T) {
	clearOpenBaoEnvironment(t)
	_, err := newOpenBaoStoreFromEnvironment()
	require.Error(t, err)
	assert.ErrorContains(t, err, "address is required")

	t.Setenv(envAddress, "http://127.0.0.1:8200")
	_, err = newOpenBaoStoreFromEnvironment()
	require.Error(t, err)
	assert.ErrorContains(t, err, "auth.token is required")

	t.Setenv(envToken, "test-token")
	t.Setenv(envAuthMethod, "kubernetes")
	_, err = newOpenBaoStoreFromEnvironment()
	require.Error(t, err)
	assert.ErrorContains(t, err, "auth.method")

	t.Setenv(envAuthMethod, "token")
	t.Setenv(envClientCert, "/cert.pem")
	_, err = newOpenBaoStoreFromEnvironment()
	require.Error(t, err)
	assert.ErrorContains(t, err, "must be set together")
}

func TestStoreFromEnvironment(t *testing.T) {
	clearOpenBaoEnvironment(t)
	t.Setenv(envAddress, "http://127.0.0.1:8200")
	t.Setenv(envToken, "test-token")
	t.Setenv(envNamespace, "team-a")

	store, err := newOpenBaoStoreFromEnvironment()
	require.NoError(t, err)
	assert.NotNil(t, store)
}

type roundTripFunc func(*http.Request) *http.Response

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	response := f(request)
	response.Request = request
	return response, nil
}

func newTestOpenBaoStore(t *testing.T, roundTrip roundTripFunc) *openBaoStore {
	t.Helper()
	config := openbao.NewConfig()
	config.Address = "http://openbao.test"
	config.HttpClient = &http.Client{Transport: roundTrip}
	client, err := openbao.NewClient(config)
	require.NoError(t, err)
	client.SetToken("test-token")
	return &openBaoStore{client: client}
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func clearOpenBaoEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		envConfig,
		envAddress,
		envToken,
		envAuthMethod,
		envNamespace,
		envCACert,
		envClientCert,
		envClientKey,
		envWatchEnabled,
		envWatchInterval,
	} {
		value, exists := os.LookupEnv(name)
		require.NoError(t, os.Unsetenv(name))
		t.Cleanup(func() {
			var err error
			if exists {
				err = os.Setenv(name, value)
			} else {
				err = os.Unsetenv(name)
			}
			assert.NoError(t, err)
		})
	}
}

func TestOpenBaoStoreCurrentVersion(t *testing.T) {
	store := newTestOpenBaoStore(t, func(r *http.Request) *http.Response {
		assert.Equal(t, "/v1/secret/metadata/otel/collector", r.URL.Path)
		return jsonResponse(http.StatusOK, `{"data":{"current_version":7,"versions":{"7":{"created_time":"2026-08-19T00:00:00Z","deletion_time":"","destroyed":false}}}}`)
	})

	got, err := store.CurrentVersion(context.Background(), SecretRef{Mount: "secret", Path: "otel/collector"})
	require.NoError(t, err)
	assert.Equal(t, 7, got)
}
