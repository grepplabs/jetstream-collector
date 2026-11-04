package celpartitionprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
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
