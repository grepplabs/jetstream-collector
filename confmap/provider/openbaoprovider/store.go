package openbaoprovider

import (
	"context"
	"errors"
	"fmt"

	openbao "github.com/openbao/openbao/api/v2"
)

const (
	configField = "config"

	envAddress       = "BAO_ADDR"
	envToken         = "BAO_TOKEN"
	envAuthMethod    = "BAO_AUTH_METHOD"
	envNamespace     = "BAO_NAMESPACE"
	envCACert        = "BAO_CACERT"
	envClientCert    = "BAO_CLIENT_CERT"
	envClientKey     = "BAO_CLIENT_KEY"
	envWatchEnabled  = "BAO_WATCH_ENABLED"
	envWatchInterval = "BAO_WATCH_INTERVAL"
)

type openBaoStore struct {
	client *openbao.Client
}

func newOpenBaoStoreFromEnvironment() (store, error) {
	cfg, err := ConfigFromEnvironment()
	if err != nil {
		return nil, err
	}
	return newOpenBaoStore(cfg)
}

func newOpenBaoStore(cfg Config) (store, error) {
	if cfg.Auth.Method == "" {
		cfg.Auth.Method = NewDefaultConfig().Auth.Method
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	config := openbao.NewConfig()
	config.Address = cfg.Address
	if err := config.ConfigureTLS(&openbao.TLSConfig{
		CACert:     cfg.TLS.CACert,
		ClientCert: cfg.TLS.ClientCert,
		ClientKey:  cfg.TLS.ClientKey,
	}); err != nil {
		return nil, fmt.Errorf("configure TLS: %w", err)
	}

	client, err := openbao.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("create OpenBao client: %w", err)
	}
	client.SetToken(cfg.Auth.Token)
	if cfg.Namespace != "" {
		client.SetNamespace(cfg.Namespace)
	}

	return &openBaoStore{client: client}, nil
}

func (s *openBaoStore) GetValue(ctx context.Context, ref SecretRef) (storeValue, error) {
	secret, err := s.client.KVv2(ref.Mount).Get(ctx, ref.Path)
	if err != nil {
		return storeValue{}, err
	}
	if secret == nil || secret.Data == nil {
		return storeValue{}, errors.New("secret has no data")
	}

	field := ref.Field
	if field == "" {
		field = configField
	}
	value, ok := secret.Data[field]
	if !ok {
		return storeValue{}, fmt.Errorf("secret does not contain %q field", field)
	}
	config, ok := value.(string)
	if !ok {
		return storeValue{}, fmt.Errorf("secret field %q must be a string, got %T", field, value)
	}
	if secret.VersionMetadata == nil {
		return storeValue{}, errors.New("secret has no version metadata")
	}
	return storeValue{Value: config, Version: secret.VersionMetadata.Version}, nil
}

func (s *openBaoStore) CurrentVersion(ctx context.Context, ref SecretRef) (int, error) {
	metadata, err := s.client.KVv2(ref.Mount).GetMetadata(ctx, ref.Path)
	if err != nil {
		return 0, err
	}
	if metadata == nil {
		return 0, errors.New("secret has no metadata")
	}
	return metadata.CurrentVersion, nil
}
