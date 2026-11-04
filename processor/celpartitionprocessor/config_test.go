package celpartitionprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr string
	}{
		{name: "defaults error action", cfg: NewDefaultConfig()},
		{name: "rejects empty expression", cfg: &Config{PartitionBy: PartitionByConfig{Logs: []string{" "}}}, wantErr: "cannot be empty"},
		{name: "rejects invalid error action", cfg: &Config{OnError: "ignore"}, wantErr: "invalid on_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, OnErrorDrop, tt.cfg.OnError)
			}
		})
	}
}

func TestCompileConfigRejectsInvalidCEL(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.PartitionBy.Logs = []string{"record.attributes["}
	_, err := compileConfig(cfg)
	require.ErrorContains(t, err, "partition_by.logs[0]")
}

func TestCELRejectsNonStringResult(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.PartitionBy.Logs = []string{"1"}
	compiled, err := compileConfig(cfg)
	require.NoError(t, err)
	_, err = compiled.key(compiled.logs, pcommon.NewMap().AsRaw(), pcommon.NewMap().AsRaw())
	require.ErrorContains(t, err, "must return strings")
}
