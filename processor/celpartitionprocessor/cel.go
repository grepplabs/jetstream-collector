package celpartitionprocessor

import (
	"fmt"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
)

type compiledExpression struct {
	source   string
	program  cel.Program
	variable string
}
type compiledConfig struct {
	cfg                   *Config
	logs, metrics, traces []compiledExpression
}

func compileExpressions(cfg *Config) (*compiledConfig, error) {
	out := &compiledConfig{cfg: cfg}
	for _, spec := range []struct {
		name, variable string
		sources        []string
		target         *[]compiledExpression
	}{
		{"logs", "record", cfg.PartitionBy.Logs, &out.logs},
		{"metrics", "datapoint", cfg.PartitionBy.Metrics, &out.metrics},
		{"traces", "span", cfg.PartitionBy.Traces, &out.traces},
	} {
		env, err := cel.NewEnv(cel.Variable(spec.variable, cel.DynType))
		if err != nil {
			return nil, err
		}
		for i, source := range spec.sources {
			ast, issues := env.Compile(source)
			if issues != nil && issues.Err() != nil {
				return nil, fmt.Errorf("partition_by.%s[%d]: %w", spec.name, i, issues.Err())
			}
			program, err := env.Program(ast)
			if err != nil {
				return nil, fmt.Errorf("partition_by.%s[%d]: %w", spec.name, i, err)
			}
			*spec.target = append(*spec.target, compiledExpression{source: source, program: program, variable: spec.variable})
		}
	}
	return out, nil
}

func (c *compiledConfig) key(expressions []compiledExpression, attrs, resourceAttrs map[string]any) (string, error) {
	parts := make([]string, 0, len(expressions))
	for _, expression := range expressions {
		value, _, err := expression.program.Eval(map[string]any{expression.variable: map[string]any{
			"attributes": attrs,
			"resource":   map[string]any{"attributes": resourceAttrs},
		}})
		if err != nil {
			return "", fmt.Errorf("evaluate CEL %q: %w", expression.source, err)
		}
		str, ok := value.(types.String)
		if !ok {
			return "", fmt.Errorf("CEL expression %q returned %s; partition key expressions must return strings", expression.source, value.Type())
		}
		parts = append(parts, strconvQuote(string(str)))
	}
	return strings.Join(parts, "|"), nil
}

func strconvQuote(value string) string { return fmt.Sprintf("%q", value) }
