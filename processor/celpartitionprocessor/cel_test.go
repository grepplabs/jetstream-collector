package celpartitionprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

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

func TestCELGetAttr(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.PartitionBy.Logs = []string{"record.attributes.get_attr('tenant')"}
	compiled, err := compileConfig(cfg)
	require.NoError(t, err)

	key, err := compiled.key(compiled.logs, map[string]any{"tenant": "tenant-a"}, nil)
	require.NoError(t, err)
	require.Equal(t, `"tenant-a"`, key)

	_, err = compiled.key(compiled.logs, map[string]any{}, nil)
	require.ErrorContains(t, err, "missing attribute")
}

func TestCELConvertsIntegerAndBooleanAttributesToString(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.PartitionBy.Logs = []string{
		"string(record.attributes.get_attr('attempt'))",
		"string(record.attributes.get_attr('enabled'))",
	}
	compiled, err := compileConfig(cfg)
	require.NoError(t, err)

	key, err := compiled.key(compiled.logs, map[string]any{"attempt": int64(42), "enabled": true}, nil)
	require.NoError(t, err)
	require.Equal(t, `"42"|"true"`, key)
}
