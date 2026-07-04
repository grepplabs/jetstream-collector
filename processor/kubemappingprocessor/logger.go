package kubemappingprocessor

import (
	"fmt"
	"strings"

	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
	zlog "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func configureControllerRuntimeLogger(cfg LoggerConfig) error {
	opts, err := controllerRuntimeLoggerOptions(cfg)
	if err != nil {
		return err
	}
	ctrl.SetLogger(zlog.New(opts...))
	return nil
}

func controllerRuntimeLoggerOptions(cfg LoggerConfig) ([]zlog.Opts, error) {
	opts := make([]zlog.Opts, 0, 4)
	if cfg.Development {
		opts = append(opts, zlog.UseDevMode(true))
	}

	if encoder, ok := normalizeLoggerValue(cfg.Encoder); ok {
		switch encoder {
		case "json":
			opts = append(opts, zlog.JSONEncoder())
		case "console":
			opts = append(opts, zlog.ConsoleEncoder())
		default:
			return nil, fmt.Errorf(`logger.encoder must be %q or %q`, "json", "console")
		}
	}

	if level, ok := normalizeLoggerValue(cfg.Level); ok {
		parsed, err := parseZapLevel(level)
		if err != nil {
			return nil, fmt.Errorf("logger.level: %w", err)
		}
		opts = append(opts, zlog.Level(parsed))
	}

	if stacktrace, ok := normalizeLoggerValue(cfg.StacktraceLevel); ok {
		parsed, err := parseZapLevel(stacktrace)
		if err != nil {
			return nil, fmt.Errorf("logger.stacktrace_level: %w", err)
		}
		opts = append(opts, zlog.StacktraceLevel(parsed))
	}

	return opts, nil
}

func normalizeLoggerValue(v string) (string, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	return strings.ToLower(v), true
}

func parseZapLevel(v string) (zapcore.Level, error) {
	switch v {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	default:
		return 0, fmt.Errorf(`must be one of %q, %q, %q, %q`, "debug", "info", "warn", "error")
	}
}
