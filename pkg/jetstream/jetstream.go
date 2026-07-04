package jetstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.uber.org/zap"
)

const (
	DefaultURL      = "nats://127.0.0.1:4222"
	CompressionNone = "none"
	CompressionGzip = "gzip"

	ContentTypeProto = "proto"
	ContentTypeJSON  = "json"

	MIMEContentTypeProto = "application/x-protobuf"
	MIMEContentTypeJSON  = "application/json"

	HeaderContentType     = "Content-Type"
	HeaderContentEncoding = "Content-Encoding"
)

// TLSConfig controls how the client connects to NATS over TLS.
type TLSConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
	ServerName         string `mapstructure:"server_name"`
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
}

// AuthConfig controls how the client authenticates to NATS.
type AuthConfig struct {
	Username        string `mapstructure:"username"`
	Password        string `mapstructure:"password"`
	Token           string `mapstructure:"token"`
	CredentialsFile string `mapstructure:"credentials_file"`
}

// BootstrapStreamConfig describes a JetStream stream that can be provisioned at startup.
type BootstrapStreamConfig struct {
	Name                   string            `mapstructure:"name"`
	Subjects               []string          `mapstructure:"subjects"`
	Description            string            `mapstructure:"description"`
	Retention              string            `mapstructure:"retention"`
	MaxConsumers           int               `mapstructure:"max_consumers"`
	MaxMsgs                int64             `mapstructure:"max_msgs"`
	MaxBytes               int64             `mapstructure:"max_bytes"`
	Discard                string            `mapstructure:"discard"`
	DiscardNewPerSubject   bool              `mapstructure:"discard_new_per_subject"`
	MaxAge                 time.Duration     `mapstructure:"max_age"`
	MaxMsgsPerSubject      int64             `mapstructure:"max_msgs_per_subject"`
	MaxMsgSize             int32             `mapstructure:"max_msg_size"`
	Storage                string            `mapstructure:"storage"`
	Replicas               int               `mapstructure:"num_replicas"`
	NoAck                  bool              `mapstructure:"no_ack"`
	Duplicates             time.Duration     `mapstructure:"duplicate_window"`
	Sealed                 bool              `mapstructure:"sealed"`
	DenyDelete             bool              `mapstructure:"deny_delete"`
	DenyPurge              bool              `mapstructure:"deny_purge"`
	AllowRollup            bool              `mapstructure:"allow_rollup_hdrs"`
	Compression            string            `mapstructure:"compression"`
	FirstSeq               uint64            `mapstructure:"first_seq"`
	AllowDirect            bool              `mapstructure:"allow_direct"`
	MirrorDirect           bool              `mapstructure:"mirror_direct"`
	AllowMsgTTL            bool              `mapstructure:"allow_msg_ttl"`
	SubjectDeleteMarkerTTL time.Duration     `mapstructure:"subject_delete_marker_ttl"`
	Metadata               map[string]string `mapstructure:"metadata"`
}

// BootstrapConsumerConfig describes a JetStream pull consumer that can be provisioned at startup.
type BootstrapConsumerConfig struct {
	Name          string        `mapstructure:"name"`
	FilterSubject string        `mapstructure:"filter_subject"`
	Description   string        `mapstructure:"description"`
	AckWait       time.Duration `mapstructure:"ack_wait"`
	DeliverPolicy string        `mapstructure:"deliver_policy"`
	MaxDeliver    int           `mapstructure:"max_deliver"`
	MaxAckPending int           `mapstructure:"max_ack_pending"`
}

// BootstrapConfig controls optional JetStream stream and consumer provisioning.
type BootstrapConfig struct {
	Stream   configoptional.Optional[BootstrapStreamConfig]   `mapstructure:"stream"`
	Consumer configoptional.Optional[BootstrapConsumerConfig] `mapstructure:"consumer"`
}

func (cfg *BootstrapConfig) ApplyReceiverDefaults(subject, consumerName string) {
	if cfg == nil || !cfg.Consumer.HasValue() {
		return
	}
	consumer := cfg.Consumer.Get()
	if consumer == nil {
		return
	}
	if strings.TrimSpace(consumer.Name) == "" {
		consumer.Name = consumerName
	}
	if strings.TrimSpace(consumer.FilterSubject) == "" {
		consumer.FilterSubject = subject
	}
}

func (cfg BootstrapConfig) Validate() error {
	if cfg.Stream.HasValue() {
		stream := cfg.Stream.Get()
		if stream == nil {
			return fmt.Errorf("stream is nil")
		}
		if strings.TrimSpace(stream.Name) == "" {
			return fmt.Errorf("stream.name is required")
		}
		if len(stream.Subjects) == 0 {
			return fmt.Errorf("stream.subjects are required")
		}
		for i, subject := range stream.Subjects {
			if strings.TrimSpace(subject) == "" {
				return fmt.Errorf("stream.subjects[%d] is empty", i)
			}
		}
		if _, err := normalizeRetentionPolicy(stream.Retention); err != nil {
			return fmt.Errorf("stream.retention: %w", err)
		}
		if _, err := normalizeDiscardPolicy(stream.Discard); err != nil {
			return fmt.Errorf("stream.discard: %w", err)
		}
		if _, err := normalizeStorageType(stream.Storage); err != nil {
			return fmt.Errorf("stream.storage: %w", err)
		}
		if _, err := normalizeCompression(stream.Compression); err != nil {
			return fmt.Errorf("stream.compression: %w", err)
		}
	}
	if cfg.Consumer.HasValue() {
		consumer := cfg.Consumer.Get()
		if consumer == nil {
			return fmt.Errorf("consumer is nil")
		}
		if strings.TrimSpace(consumer.Name) == "" {
			return fmt.Errorf("consumer.name is required")
		}
		if strings.TrimSpace(consumer.FilterSubject) == "" {
			return fmt.Errorf("consumer.filter_subject is required")
		}
		if _, err := normalizeDeliverPolicy(consumer.DeliverPolicy); err != nil {
			return fmt.Errorf("consumer.deliver_policy: %w", err)
		}
	}

	return nil
}

func EnsureBootstrapStreams(ctx context.Context, js streamProvisioner, cfg BootstrapConfig) error {
	if !cfg.Stream.HasValue() {
		return nil
	}
	streamCfg, err := buildStreamConfig(cfg.Stream)
	if err != nil {
		stream := cfg.Stream.Get()
		name := ""
		if stream != nil {
			name = stream.Name
		}
		return fmt.Errorf("build JetStream stream %q: %w", name, err)
	}
	_, err = js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		stream := cfg.Stream.Get()
		name := ""
		if stream != nil {
			name = stream.Name
		}
		return fmt.Errorf("create JetStream stream %q: %w", name, err)
	}
	return nil
}

func EnsureBootstrapConsumers(ctx context.Context, stream consumerProvisioner, cfg BootstrapConfig) error {
	if !cfg.Consumer.HasValue() {
		return nil
	}
	consumerCfg, err := buildConsumerConfig(cfg.Consumer)
	if err != nil {
		consumer := cfg.Consumer.Get()
		name := ""
		if consumer != nil {
			name = consumer.Name
		}
		return fmt.Errorf("build JetStream consumer %q: %w", name, err)
	}
	_, err = stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		consumer := cfg.Consumer.Get()
		name := ""
		if consumer != nil {
			name = consumer.Name
		}
		return fmt.Errorf("create JetStream consumer %q: %w", name, err)
	}
	return nil
}

type streamProvisioner interface {
	CreateOrUpdateStream(context.Context, natsjetstream.StreamConfig) (natsjetstream.Stream, error)
}

type consumerProvisioner interface {
	CreateOrUpdateConsumer(context.Context, natsjetstream.ConsumerConfig) (natsjetstream.Consumer, error)
}

func buildStreamConfig(stream configoptional.Optional[BootstrapStreamConfig]) (natsjetstream.StreamConfig, error) {
	if !stream.HasValue() {
		return natsjetstream.StreamConfig{}, fmt.Errorf("stream is nil")
	}
	streamCfg := stream.Get()
	if streamCfg == nil {
		return natsjetstream.StreamConfig{}, fmt.Errorf("stream is nil")
	}
	cfg := natsjetstream.StreamConfig{Name: streamCfg.Name}
	if len(streamCfg.Subjects) > 0 {
		cfg.Subjects = append([]string(nil), streamCfg.Subjects...)
	}
	cfg.Description = streamCfg.Description
	if streamCfg.Retention != "" {
		rp, err := normalizeRetentionPolicy(streamCfg.Retention)
		if err != nil {
			return natsjetstream.StreamConfig{}, err
		}
		cfg.Retention = rp
	}
	cfg.MaxConsumers = streamCfg.MaxConsumers
	cfg.MaxMsgs = streamCfg.MaxMsgs
	cfg.MaxBytes = streamCfg.MaxBytes
	if streamCfg.Discard != "" {
		dp, err := normalizeDiscardPolicy(streamCfg.Discard)
		if err != nil {
			return natsjetstream.StreamConfig{}, err
		}
		cfg.Discard = dp
	}
	cfg.MaxAge = streamCfg.MaxAge
	cfg.MaxMsgsPerSubject = streamCfg.MaxMsgsPerSubject
	cfg.MaxMsgSize = streamCfg.MaxMsgSize
	if streamCfg.Storage != "" {
		st, err := normalizeStorageType(streamCfg.Storage)
		if err != nil {
			return natsjetstream.StreamConfig{}, err
		}
		cfg.Storage = st
	}
	cfg.Replicas = streamCfg.Replicas
	cfg.DiscardNewPerSubject = streamCfg.DiscardNewPerSubject
	cfg.NoAck = streamCfg.NoAck
	cfg.Sealed = streamCfg.Sealed
	cfg.DenyDelete = streamCfg.DenyDelete
	cfg.DenyPurge = streamCfg.DenyPurge
	cfg.AllowRollup = streamCfg.AllowRollup
	cfg.Duplicates = streamCfg.Duplicates
	if streamCfg.Compression != "" {
		compression, err := normalizeCompression(streamCfg.Compression)
		if err != nil {
			return natsjetstream.StreamConfig{}, err
		}
		cfg.Compression = compression
	}
	cfg.FirstSeq = streamCfg.FirstSeq
	cfg.AllowDirect = streamCfg.AllowDirect
	cfg.MirrorDirect = streamCfg.MirrorDirect
	cfg.AllowMsgTTL = streamCfg.AllowMsgTTL
	cfg.SubjectDeleteMarkerTTL = streamCfg.SubjectDeleteMarkerTTL
	if len(streamCfg.Metadata) > 0 {
		cfg.Metadata = streamCfg.Metadata
	}
	return cfg, nil
}

func buildConsumerConfig(consumer configoptional.Optional[BootstrapConsumerConfig]) (natsjetstream.ConsumerConfig, error) {
	if !consumer.HasValue() {
		return natsjetstream.ConsumerConfig{}, fmt.Errorf("consumer is nil")
	}
	consumerCfg := consumer.Get()
	if consumerCfg == nil {
		return natsjetstream.ConsumerConfig{}, fmt.Errorf("consumer is nil")
	}
	deliverPolicy, err := normalizeDeliverPolicy(consumerCfg.DeliverPolicy)
	if err != nil {
		return natsjetstream.ConsumerConfig{}, err
	}

	name := strings.TrimSpace(consumerCfg.Name)
	filterSubject := strings.TrimSpace(consumerCfg.FilterSubject)
	if name == "" {
		return natsjetstream.ConsumerConfig{}, fmt.Errorf("consumer name is required")
	}
	if filterSubject == "" {
		return natsjetstream.ConsumerConfig{}, fmt.Errorf("consumer filter_subject is required")
	}

	cfg := natsjetstream.ConsumerConfig{
		Name:          name,
		Durable:       name,
		FilterSubject: filterSubject,
		Description:   consumerCfg.Description,
		AckPolicy:     natsjetstream.AckExplicitPolicy,
		AckWait:       consumerCfg.AckWait,
		DeliverPolicy: deliverPolicy,
		ReplayPolicy:  natsjetstream.ReplayInstantPolicy,
		MaxDeliver:    consumerCfg.MaxDeliver,
		MaxAckPending: consumerCfg.MaxAckPending,
	}
	return cfg, nil
}

func normalizeRetentionPolicy(value string) (natsjetstream.RetentionPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "limits":
		return natsjetstream.LimitsPolicy, nil
	case "interest":
		return natsjetstream.InterestPolicy, nil
	case "workqueue", "work_queue", "work-queue":
		return natsjetstream.WorkQueuePolicy, nil
	default:
		return 0, fmt.Errorf("unsupported retention policy %q", value)
	}
}

func normalizeDiscardPolicy(value string) (natsjetstream.DiscardPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "old":
		return natsjetstream.DiscardOld, nil
	case "new":
		return natsjetstream.DiscardNew, nil
	default:
		return 0, fmt.Errorf("unsupported discard policy %q", value)
	}
}

func normalizeStorageType(value string) (natsjetstream.StorageType, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "file":
		return natsjetstream.FileStorage, nil
	case "memory":
		return natsjetstream.MemoryStorage, nil
	default:
		return 0, fmt.Errorf("unsupported storage type %q", value)
	}
}

func normalizeCompression(value string) (natsjetstream.StoreCompression, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "none", "identity":
		return natsjetstream.NoCompression, nil
	case "s2":
		return natsjetstream.S2Compression, nil
	default:
		return 0, fmt.Errorf("unsupported compression %q", value)
	}
}

func normalizeDeliverPolicy(value string) (natsjetstream.DeliverPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "all":
		return natsjetstream.DeliverAllPolicy, nil
	case "new":
		return natsjetstream.DeliverNewPolicy, nil
	case "last":
		return natsjetstream.DeliverLastPolicy, nil
	case "last_per_subject", "last-per-subject":
		return natsjetstream.DeliverLastPerSubjectPolicy, nil
	case "by_start_sequence", "by-start-sequence":
		return natsjetstream.DeliverByStartSequencePolicy, nil
	case "by_start_time", "by-start-time":
		return natsjetstream.DeliverByStartTimePolicy, nil
	default:
		return 0, fmt.Errorf("unsupported deliver policy %q", value)
	}
}

func Connect(url string, tlsCfg TLSConfig, authCfg AuthConfig, logger *zap.Logger) (natsjetstream.JetStream, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	opts := []nats.Option{
		nats.Name("jetstream"),
	}
	if tlsCfg.Enabled {
		tlsConfig, err := buildTLSConfig(tlsCfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}
	if strings.TrimSpace(authCfg.Username) != "" {
		opts = append(opts, nats.UserInfo(authCfg.Username, authCfg.Password))
	}
	if strings.TrimSpace(authCfg.Token) != "" {
		opts = append(opts, nats.Token(authCfg.Token))
	}
	if strings.TrimSpace(authCfg.CredentialsFile) != "" {
		opts = append(opts, nats.UserCredentials(authCfg.CredentialsFile))
	}

	conn, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	js, err := natsjetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return js, nil
}

func NormalizeCompression(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", CompressionNone, "identity":
		return CompressionNone, nil
	case CompressionGzip:
		return CompressionGzip, nil
	default:
		return "", fmt.Errorf("unsupported compression %q", value)
	}
}

func NormalizeContentType(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", ContentTypeProto, "protobuf", "application/x-protobuf", "application/protobuf", "application/octet-stream", "application/grpc+proto":
		return ContentTypeProto, nil
	case ContentTypeJSON, "application/json", "application/x-json", "text/json":
		return ContentTypeJSON, nil
	default:
		if strings.HasSuffix(strings.ToLower(strings.TrimSpace(value)), "+json") {
			return ContentTypeJSON, nil
		}
		if strings.HasSuffix(strings.ToLower(strings.TrimSpace(value)), "+proto") {
			return ContentTypeProto, nil
		}
		return "", fmt.Errorf("unsupported content type %q", value)
	}
}

func buildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify, ServerName: cfg.ServerName}
	if strings.TrimSpace(cfg.CAFile) != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM(caPEM); !ok {
			return nil, fmt.Errorf("parse ca file %q", cfg.CAFile)
		}
		tlsConfig.RootCAs = pool
	}
	if strings.TrimSpace(cfg.CertFile) != "" || strings.TrimSpace(cfg.KeyFile) != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}
