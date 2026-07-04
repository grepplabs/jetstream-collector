package jetstream

import (
	"context"
	"testing"
	"time"

	natsjetstream "github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configoptional"
)

func TestNormalizeStreamEnums(t *testing.T) {
	tests := []struct {
		name string
		got  any
		want any
		err  error
	}{
		{name: "retention default", got: mustRetention(t, ""), want: natsjetstream.LimitsPolicy},
		{name: "retention limits", got: mustRetention(t, "limits"), want: natsjetstream.LimitsPolicy},
		{name: "retention interest", got: mustRetention(t, "interest"), want: natsjetstream.InterestPolicy},
		{name: "retention workqueue", got: mustRetention(t, "workqueue"), want: natsjetstream.WorkQueuePolicy},
		{name: "retention work_queue", got: mustRetention(t, "work_queue"), want: natsjetstream.WorkQueuePolicy},
		{name: "retention work-queue", got: mustRetention(t, "work-queue"), want: natsjetstream.WorkQueuePolicy},
		{name: "discard default", got: mustDiscard(t, ""), want: natsjetstream.DiscardOld},
		{name: "discard old", got: mustDiscard(t, "old"), want: natsjetstream.DiscardOld},
		{name: "discard new", got: mustDiscard(t, "new"), want: natsjetstream.DiscardNew},
		{name: "storage default", got: mustStorage(t, ""), want: natsjetstream.FileStorage},
		{name: "storage file", got: mustStorage(t, "file"), want: natsjetstream.FileStorage},
		{name: "storage memory", got: mustStorage(t, "memory"), want: natsjetstream.MemoryStorage},
		{name: "compression default", got: mustCompression(t, ""), want: natsjetstream.NoCompression},
		{name: "compression none", got: mustCompression(t, "none"), want: natsjetstream.NoCompression},
		{name: "compression identity", got: mustCompression(t, "identity"), want: natsjetstream.NoCompression},
		{name: "compression s2", got: mustCompression(t, "s2"), want: natsjetstream.S2Compression},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			switch want := tt.want.(type) {
			case natsjetstream.RetentionPolicy:
				got := tt.got.(natsjetstream.RetentionPolicy)
				require.Equal(t, want, got)
			case natsjetstream.DiscardPolicy:
				got := tt.got.(natsjetstream.DiscardPolicy)
				require.Equal(t, want, got)
			case natsjetstream.StorageType:
				got := tt.got.(natsjetstream.StorageType)
				require.Equal(t, want, got)
			case natsjetstream.StoreCompression:
				got := tt.got.(natsjetstream.StoreCompression)
				require.Equal(t, want, got)
			default:
				require.Fail(t, "unsupported test case")
			}
		})
	}

	require.Error(t, func() error {
		_, err := normalizeRetentionPolicy("bad")
		return err
	}())
	require.Error(t, func() error {
		_, err := normalizeDiscardPolicy("interst")
		return err
	}())
	require.Error(t, func() error {
		_, err := normalizeStorageType("disk")
		return err
	}())
	require.Error(t, func() error {
		_, err := normalizeCompression("gzip")
		return err
	}())
}

func TestBuildStreamConfigAppliesOptionalFields(t *testing.T) {
	cfg, err := buildStreamConfig(configoptional.Some(BootstrapStreamConfig{
		Name:                   "otel_logs",
		Subjects:               []string{"otel.logs"},
		Description:            "demo",
		Retention:              "interest",
		MaxConsumers:           7,
		MaxMsgs:                11,
		MaxBytes:               2048,
		Discard:                "new",
		DiscardNewPerSubject:   true,
		MaxAge:                 5 * time.Minute,
		MaxMsgsPerSubject:      3,
		MaxMsgSize:             1024,
		Storage:                "memory",
		Replicas:               2,
		NoAck:                  true,
		Duplicates:             30 * time.Second,
		Sealed:                 true,
		DenyDelete:             true,
		DenyPurge:              true,
		AllowRollup:            true,
		Compression:            "s2",
		FirstSeq:               42,
		AllowDirect:            true,
		MirrorDirect:           true,
		AllowMsgTTL:            true,
		SubjectDeleteMarkerTTL: 2 * time.Minute,
		Metadata:               map[string]string{"team": "dev"},
	}))
	require.NoError(t, err)
	require.Equal(t, "otel_logs", cfg.Name)
	require.Equal(t, []string{"otel.logs"}, cfg.Subjects)
	require.Equal(t, "demo", cfg.Description)
	require.Equal(t, natsjetstream.InterestPolicy, cfg.Retention)
	require.Equal(t, 7, cfg.MaxConsumers)
	require.Equal(t, int64(11), cfg.MaxMsgs)
	require.Equal(t, int64(2048), cfg.MaxBytes)
	require.Equal(t, natsjetstream.DiscardNew, cfg.Discard)
	require.True(t, cfg.DiscardNewPerSubject)
	require.Equal(t, 5*time.Minute, cfg.MaxAge)
	require.Equal(t, int64(3), cfg.MaxMsgsPerSubject)
	require.Equal(t, int32(1024), cfg.MaxMsgSize)
	require.Equal(t, natsjetstream.MemoryStorage, cfg.Storage)
	require.Equal(t, 2, cfg.Replicas)
	require.True(t, cfg.NoAck)
	require.Equal(t, 30*time.Second, cfg.Duplicates)
	require.True(t, cfg.Sealed)
	require.True(t, cfg.DenyDelete)
	require.True(t, cfg.DenyPurge)
	require.True(t, cfg.AllowRollup)
	require.Equal(t, natsjetstream.S2Compression, cfg.Compression)
	require.Equal(t, uint64(42), cfg.FirstSeq)
	require.True(t, cfg.AllowDirect)
	require.True(t, cfg.MirrorDirect)
	require.True(t, cfg.AllowMsgTTL)
	require.Equal(t, 2*time.Minute, cfg.SubjectDeleteMarkerTTL)
	require.Equal(t, map[string]string{"team": "dev"}, cfg.Metadata)
}

func TestBuildConsumerConfigAppliesOptionalFields(t *testing.T) {
	cfg, err := buildConsumerConfig(configoptional.Some(BootstrapConsumerConfig{
		Name:          "otel_logs_in",
		FilterSubject: "otel.logs",
		Description:   "demo",
		AckWait:       5 * time.Second,
		DeliverPolicy: "last",
		MaxDeliver:    7,
		MaxAckPending: 11,
	}))
	require.NoError(t, err)
	require.Equal(t, "otel_logs_in", cfg.Name)
	require.Equal(t, "otel_logs_in", cfg.Durable)
	require.Equal(t, "otel.logs", cfg.FilterSubject)
	require.Equal(t, "demo", cfg.Description)
	require.Equal(t, natsjetstream.AckExplicitPolicy, cfg.AckPolicy)
	require.Equal(t, 5*time.Second, cfg.AckWait)
	require.Equal(t, natsjetstream.DeliverLastPolicy, cfg.DeliverPolicy)
	require.Equal(t, natsjetstream.ReplayInstantPolicy, cfg.ReplayPolicy)
	require.Equal(t, 7, cfg.MaxDeliver)
	require.Equal(t, 11, cfg.MaxAckPending)
}

type bootstrapStreamProvisioner struct {
	configs []natsjetstream.StreamConfig
}

func (p *bootstrapStreamProvisioner) CreateOrUpdateStream(_ context.Context, cfg natsjetstream.StreamConfig) (natsjetstream.Stream, error) {
	p.configs = append(p.configs, cfg)
	return nil, nil
}

func TestEnsureBootstrapStreams(t *testing.T) {
	prov := &bootstrapStreamProvisioner{}
	err := EnsureBootstrapStreams(context.Background(), prov, BootstrapConfig{
		Stream: configoptional.Some(BootstrapStreamConfig{
			Name:     "otel_logs",
			Subjects: []string{"otel.logs"},
		}),
	})
	require.NoError(t, err)
	require.Len(t, prov.configs, 1)
	require.Equal(t, "otel_logs", prov.configs[0].Name)
	require.Equal(t, []string{"otel.logs"}, prov.configs[0].Subjects)
}

type bootstrapConsumerProvisioner struct {
	configs []natsjetstream.ConsumerConfig
}

func (p *bootstrapConsumerProvisioner) CreateOrUpdateConsumer(_ context.Context, cfg natsjetstream.ConsumerConfig) (natsjetstream.Consumer, error) {
	p.configs = append(p.configs, cfg)
	return nil, nil
}

func TestEnsureBootstrapConsumers(t *testing.T) {
	prov := &bootstrapConsumerProvisioner{}
	err := EnsureBootstrapConsumers(context.Background(), prov, BootstrapConfig{
		Consumer: configoptional.Some(BootstrapConsumerConfig{
			Name:          "otel_logs_in",
			FilterSubject: "otel.logs",
			AckWait:       time.Second,
			DeliverPolicy: "all",
			MaxAckPending: 1,
		}),
	})
	require.NoError(t, err)
	require.Len(t, prov.configs, 1)
	require.Equal(t, "otel_logs_in", prov.configs[0].Name)
	require.Equal(t, "otel.logs", prov.configs[0].FilterSubject)
}

func TestBootstrapConfigApplyReceiverDefaults(t *testing.T) {
	cfg := BootstrapConfig{Consumer: configoptional.Some(BootstrapConsumerConfig{})}
	cfg.ApplyReceiverDefaults("otel.logs", "jetstream-in-1")
	require.NotNil(t, cfg.Consumer.Get())
	require.Equal(t, "jetstream-in-1", cfg.Consumer.Get().Name)
	require.Equal(t, "otel.logs", cfg.Consumer.Get().FilterSubject)
}

func TestBootstrapConfigValidate(t *testing.T) {
	require.NoError(t, BootstrapConfig{}.Validate())
	require.NoError(t, BootstrapConfig{Stream: configoptional.Some(BootstrapStreamConfig{Name: "x", Subjects: []string{"otel.logs"}, Retention: ""})}.Validate())
	require.NoError(t, BootstrapConfig{Consumer: configoptional.Some(BootstrapConsumerConfig{Name: "c", FilterSubject: "otel.logs", DeliverPolicy: "all", MaxDeliver: 0})}.Validate())
	require.Error(t, BootstrapConfig{Stream: configoptional.Some(BootstrapStreamConfig{Name: "x", Subjects: []string{"otel.logs"}, Retention: "bad"})}.Validate())
	require.Error(t, BootstrapConfig{Stream: configoptional.Some(BootstrapStreamConfig{Name: "x", Subjects: []string{"otel.logs"}, Discard: "interst"})}.Validate())
	require.Error(t, BootstrapConfig{Consumer: configoptional.Some(BootstrapConsumerConfig{Name: "c", FilterSubject: "", DeliverPolicy: "all", MaxDeliver: 0})}.Validate())
}

func mustRetention(t *testing.T, value string) natsjetstream.RetentionPolicy {
	t.Helper()
	got, err := normalizeRetentionPolicy(value)
	require.NoError(t, err)
	return got
}

func mustDiscard(t *testing.T, value string) natsjetstream.DiscardPolicy {
	t.Helper()
	got, err := normalizeDiscardPolicy(value)
	require.NoError(t, err)
	return got
}

func mustStorage(t *testing.T, value string) natsjetstream.StorageType {
	t.Helper()
	got, err := normalizeStorageType(value)
	require.NoError(t, err)
	return got
}

func mustCompression(t *testing.T, value string) natsjetstream.StoreCompression {
	t.Helper()
	got, err := normalizeCompression(value)
	require.NoError(t, err)
	return got
}
