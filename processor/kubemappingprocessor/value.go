package kubemappingprocessor

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func extractValue(obj unstructured.Unstructured, cfg ValueConfig) (string, bool, error) {
	if cfg.Label != "" {
		value, ok := obj.GetLabels()[cfg.Label]
		return value, ok, nil
	}
	value, ok, err := unstructured.NestedFieldNoCopy(obj.Object, strings.Split(cfg.Field, ".")...)
	if err != nil || !ok || value == nil {
		return "", ok, err
	}
	text, err := scalarString(value, cfg.Field)
	if err != nil {
		return "", false, err
	}
	return text, true, nil
}

func scalarString(value any, field string) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	default:
		return "", fmt.Errorf("field %q has unsupported type %T", field, value)
	}
}
