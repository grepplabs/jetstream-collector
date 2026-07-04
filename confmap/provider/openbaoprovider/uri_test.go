package openbaoprovider

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		name       string
		uri        string
		want       SecretRef
		wantErrMsg string
	}{
		{
			name: "valid nested path",
			uri:  "openbao:secret/otel/collector",
			want: SecretRef{Mount: "secret", Path: "otel/collector"},
		},
		{
			name: "valid single segment path",
			uri:  "openbao:kv/config",
			want: SecretRef{Mount: "kv", Path: "config"},
		},
		{
			name: "valid field selector",
			uri:  "openbao:secret/otel/credentials#token",
			want: SecretRef{Mount: "secret", Path: "otel/credentials", Field: "token"},
		},
		{name: "empty URI", wantErrMsg: "missing scheme"},
		{name: "missing colon", uri: "openbao", wantErrMsg: "missing scheme"},
		{name: "wrong scheme", uri: "vault:secret/foo", wantErrMsg: "unsupported scheme"},
		{name: "empty reference", uri: "openbao:", wantErrMsg: "missing mount and secret path"},
		{name: "missing mount", uri: "openbao:/foo", wantErrMsg: "missing mount"},
		{name: "missing path separator", uri: "openbao:secret", wantErrMsg: "missing secret path"},
		{name: "empty path", uri: "openbao:secret/", wantErrMsg: "missing secret path"},
		{name: "URL style", uri: "openbao://secret/foo", wantErrMsg: "URL-style syntax"},
		{name: "repeated slash", uri: "openbao:secret//foo", wantErrMsg: "empty segment"},
		{name: "trailing slash", uri: "openbao:secret/foo/", wantErrMsg: "empty segment"},
		{name: "empty field selector", uri: "openbao:secret/foo#", wantErrMsg: "field selector is empty"},
		{name: "multiple fragments", uri: "openbao:secret/foo#token#other", wantErrMsg: "unexpected fragment delimiter"},
		{name: "query", uri: "openbao:secret/foo?version=1", wantErrMsg: "query parameters"},
		{name: "space in mount", uri: "openbao:sec ret/foo", wantErrMsg: "whitespace"},
		{name: "space in path", uri: "openbao:secret/foo bar", wantErrMsg: "whitespace"},
		{name: "newline in path", uri: "openbao:secret/foo\nbar", wantErrMsg: "whitespace"},
		{name: "space in field", uri: "openbao:secret/foo#access token", wantErrMsg: "whitespace"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseURI(tt.uri)
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErrMsg)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
