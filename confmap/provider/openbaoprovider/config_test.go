package openbaoprovider

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigFromEnvironmentFileWithLiteralToken(t *testing.T) {
	clearOpenBaoEnvironment(t)
	path := writeBootstrapConfig(t, `
address: https://openbao.internal:8200
namespace: team-a
auth:
  method: token
  token: literal-token
tls:
  ca_cert: /etc/openbao/ca.pem
`)
	t.Setenv(envConfig, path)

	cfg, err := ConfigFromEnvironment()
	require.NoError(t, err)
	assert.Equal(t, "https://openbao.internal:8200", cfg.Address)
	assert.Equal(t, "literal-token", cfg.Auth.Token)
	assert.Equal(t, "team-a", cfg.Namespace)
	assert.Equal(t, "/etc/openbao/ca.pem", cfg.TLS.CACert)
}

func TestConfigFromEnvironmentFileReferences(t *testing.T) {
	clearOpenBaoEnvironment(t)
	path := writeBootstrapConfig(t, `
address: ${env:OPENBAO_TEST_ADDR}
auth:
  token: ${env:OPENBAO_TEST_TOKEN}
`)
	t.Setenv(envConfig, path)
	t.Setenv("OPENBAO_TEST_ADDR", "https://openbao.internal:8200")
	t.Setenv("OPENBAO_TEST_TOKEN", "referenced-token")

	cfg, err := ConfigFromEnvironment()
	require.NoError(t, err)
	assert.Equal(t, authMethodToken, cfg.Auth.Method)
	assert.Equal(t, "referenced-token", cfg.Auth.Token)
}

func TestConfigFromStandardEnvironment(t *testing.T) {
	clearOpenBaoEnvironment(t)
	t.Setenv(envAddress, "https://from-env:8200")
	t.Setenv(envToken, "env-token")
	t.Setenv(envCACert, "/env/ca.pem")

	cfg, err := ConfigFromEnvironment()
	require.NoError(t, err)
	assert.Equal(t, "https://from-env:8200", cfg.Address)
	assert.Equal(t, authMethodToken, cfg.Auth.Method)
	assert.Equal(t, "env-token", cfg.Auth.Token)
	assert.Equal(t, "/env/ca.pem", cfg.TLS.CACert)
	assert.False(t, cfg.Watch.Enabled)
	assert.Equal(t, defaultWatchInterval, cfg.Watch.Interval)
}

func TestConfigFromEnvironmentWatchEnabled(t *testing.T) {
	clearOpenBaoEnvironment(t)
	t.Setenv(envAddress, "https://from-env:8200")
	t.Setenv(envToken, "env-token")
	t.Setenv(envWatchEnabled, "true")
	t.Setenv(envWatchInterval, "45s")

	cfg, err := ConfigFromEnvironment()
	require.NoError(t, err)
	assert.True(t, cfg.Watch.Enabled)
	assert.Equal(t, 45*time.Second, cfg.Watch.Interval)
}

func TestConfigWatchIntervalValidation(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.Address = "https://openbao:8200"
	cfg.Auth.Token = "token"
	cfg.Watch.Enabled = true
	cfg.Watch.Interval = 0

	err := cfg.Validate()
	require.Error(t, err)
	assert.ErrorContains(t, err, "watch.interval must be greater than zero")
}

func TestConfigFileIsAuthoritative(t *testing.T) {
	clearOpenBaoEnvironment(t)
	path := writeBootstrapConfig(t, `
address: https://from-file:8200
auth:
  token: file-token
tls:
  ca_cert: /file/ca.pem
`)
	t.Setenv(envConfig, path)
	t.Setenv(envAddress, "https://from-env:8200")
	t.Setenv(envToken, "env-token")
	t.Setenv(envCACert, "/env/ca.pem")

	cfg, err := ConfigFromEnvironment()
	require.NoError(t, err)
	assert.Equal(t, "https://from-file:8200", cfg.Address)
	assert.Equal(t, "file-token", cfg.Auth.Token)
	assert.Equal(t, "/file/ca.pem", cfg.TLS.CACert)
}

func TestConfigFromEnvironmentErrors(t *testing.T) {
	tests := []struct {
		name       string
		content    string
		wantErrMsg string
	}{
		{
			name:       "unknown field",
			content:    "address: https://openbao:8200\nunknown: value\n",
			wantErrMsg: "invalid keys: unknown",
		},
		{
			name:       "missing referenced environment variable",
			content:    "address: https://openbao:8200\nauth:\n  token: ${env:OPENBAO_UNSET_TOKEN}\n",
			wantErrMsg: "auth.token is required",
		},
		{
			name:       "invalid address",
			content:    "address: openbao.internal\nauth:\n  token: token\n",
			wantErrMsg: "address must be an absolute URL",
		},
		{
			name:       "incomplete mTLS",
			content:    "address: https://openbao:8200\nauth:\n  token: token\ntls:\n  client_cert: /cert.pem\n",
			wantErrMsg: "must be set together",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearOpenBaoEnvironment(t)
			path := writeBootstrapConfig(t, tt.content)
			t.Setenv(envConfig, path)
			_, err := ConfigFromEnvironment()
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErrMsg)
		})
	}
}

func TestConfigValidateDoesNotExposeToken(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.Auth.Token = "must-not-leak"
	err := cfg.Validate()
	require.Error(t, err)
	assert.NotContains(t, err.Error(), cfg.Auth.Token)
}

func writeBootstrapConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "openbao.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}
