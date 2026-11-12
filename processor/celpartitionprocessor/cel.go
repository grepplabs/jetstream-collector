package celpartitionprocessor

import (
	"fmt"
	"strconv"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
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
		env, err := cel.NewEnv(
			cel.Variable(spec.variable, cel.DynType),
			cel.Function("get_attr", cel.MemberOverload(
				"map_get_attr_string",
				[]*cel.Type{cel.MapType(cel.StringType, cel.DynType), cel.StringType},
				cel.DynType,
				cel.BinaryBinding(getAttr),
			)),
		)
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

func getAttr(mapValue, keyValue ref.Val) ref.Val {
	mapper, ok := mapValue.(traits.Mapper)
	if !ok {
		return types.NewErr("get_attr receiver must be a map, got %s", mapValue.Type())
	}
	key, ok := keyValue.(types.String)
	if !ok {
		return types.NewErr("get_attr key must be a string, got %s", keyValue.Type())
	}
	value, found := mapper.Find(key)
	if !found {
		return types.NewErr("missing attribute %q", string(key))
	}
	return value
}

func (c *compiledConfig) key(expressions []compiledExpression, attrs, resourceAttrs map[string]any) (string, error) {
	if len(expressions) == 0 {
		return "", nil
	}
	activation := map[string]any{expressions[0].variable: map[string]any{
		"attributes": attrs,
		"resource":   map[string]any{"attributes": resourceAttrs},
	}}
	var key strings.Builder
	for i, expression := range expressions {
		value, _, err := expression.program.Eval(activation)
		if err != nil {
			return "", fmt.Errorf("evaluate CEL %q: %w", expression.source, err)
		}
		str, ok := value.(types.String)
		if !ok {
			return "", fmt.Errorf("CEL expression %q returned %s; partition key expressions must return strings", expression.source, value.Type())
		}
		if i > 0 {
			key.WriteByte('|')
		}
		key.WriteString(strconv.Quote(string(str)))
	}
	return key.String(), nil
}
