package s3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveS3Endpoint(t *testing.T) {
	tests := []struct {
		name, endpoint, region, wantEndpoint, wantRegion string
		secure, wantSecure                               bool
	}{
		{name: "default AWS", secure: true, wantEndpoint: "s3.amazonaws.com", wantRegion: DefaultRegion, wantSecure: true},
		{name: "regional AWS", region: "us-west-2", secure: true, wantEndpoint: "s3.us-west-2.amazonaws.com", wantRegion: "us-west-2", wantSecure: true},
		{name: "HTTP compatible endpoint", endpoint: "http://s3.example.test:9000", region: "test-1", secure: true, wantEndpoint: "s3.example.test:9000", wantRegion: "test-1", wantSecure: false},
		{name: "HTTPS compatible endpoint", endpoint: "https://s3.example.test:9000", region: "test-1", secure: false, wantEndpoint: "s3.example.test:9000", wantRegion: "test-1", wantSecure: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint, secure, region := ResolveEndpoint(tt.endpoint, tt.region, tt.secure)
			require.Equal(t, tt.wantEndpoint, endpoint)
			require.Equal(t, tt.wantSecure, secure)
			require.Equal(t, tt.wantRegion, region)
		})
	}
}

func TestNewS3ClientBuildsWithStaticCredentials(t *testing.T) {
	client, err := NewS3Client(context.Background(), nil, S3ClientConfig{
		Endpoint:    "localhost:9000",
		Region:      "test-1",
		Credentials: S3CredentialsConfig{AccessKeyID: "access", SecretAccessKey: "secret"},
	})
	require.NoError(t, err)
	require.NotNil(t, client)
}
