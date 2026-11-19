package s3

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/collector/component"
)

const DefaultRegion = "eu-central-1"

type S3CredentialsConfig struct {
	AccessKeyID     string       `mapstructure:"access_key_id"`
	SecretAccessKey string       `mapstructure:"secret_access_key"`
	SessionToken    string       `mapstructure:"session_token"`
	Provider        component.ID `mapstructure:"provider"`
}

type S3ClientConfig struct {
	Endpoint       string
	Region         string
	Secure         bool
	ForcePathStyle bool
	Credentials    S3CredentialsConfig
}

// NewS3ClientWithClose builds an S3 client and returns a function that closes
// idle HTTP connections owned by the client.
func NewS3ClientWithClose(ctx context.Context, host component.Host, cfg S3ClientConfig) (*minio.Client, func(), error) {
	creds, err := resolveS3Credentials(ctx, host, cfg.Credentials)
	if err != nil {
		return nil, nil, err
	}
	endpoint, secure, region := ResolveEndpoint(cfg.Endpoint, cfg.Region, cfg.Secure)
	transport, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, nil, fmt.Errorf("create S3 HTTP transport: %w", err)
	}
	opts := &minio.Options{
		Creds:        creds,
		Secure:       secure,
		Region:       region,
		BucketLookup: minio.BucketLookupAuto,
		Transport:    transport,
	}
	if cfg.ForcePathStyle {
		opts.BucketLookup = minio.BucketLookupPath
	}
	client, err := minio.New(endpoint, opts)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, nil, fmt.Errorf("create S3 client: %w", err)
	}
	return client, transport.CloseIdleConnections, nil
}

func resolveS3Credentials(ctx context.Context, host component.Host, cfg S3CredentialsConfig) (*credentials.Credentials, error) {
	if cfg.AccessKeyID != "" {
		return credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken), nil
	}
	if cfg.Provider == (component.ID{}) || host == nil {
		return nil, fmt.Errorf("S3 credentials provider %q unavailable", cfg.Provider)
	}
	ext, ok := host.GetExtensions()[cfg.Provider]
	if !ok {
		return nil, fmt.Errorf("unknown S3 credentials provider extension %q", cfg.Provider)
	}
	provider, ok := extCredentialsProvider(ext)
	if !ok {
		return nil, fmt.Errorf("extension %q does not provide retrievable credentials", cfg.Provider)
	}
	creds, err := provider.Retrieve(ctx)
	if err != nil {
		return nil, err
	}
	return credentials.NewStaticV4(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken), nil
}

func extCredentialsProvider(ext component.Component) (aws.CredentialsProvider, bool) {
	provider, ok := ext.(interface {
		GetCredentialsProvider() aws.CredentialsProvider
	})
	if !ok {
		return nil, false
	}
	return provider.GetCredentialsProvider(), true
}

func ResolveEndpoint(endpoint, region string, secure bool) (string, bool, string) {
	region = strings.TrimSpace(region)
	if region == "" {
		region = DefaultRegion
	}
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		if region == DefaultRegion {
			endpoint = "s3.amazonaws.com"
		} else {
			endpoint = "s3." + region + ".amazonaws.com"
		}
		return endpoint, secure, region
	}
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
		switch parsed.Scheme {
		case "http":
			secure = false
		case "https":
			secure = true
		}
		endpoint = parsed.Host
	}
	return endpoint, secure, region
}
