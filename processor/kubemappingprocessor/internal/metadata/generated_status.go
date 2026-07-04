package metadata

import "go.opentelemetry.io/collector/component"

var Type = component.MustNewType("kubemapping")

const LogsStability = component.StabilityLevelAlpha
const MetricsStability = component.StabilityLevelAlpha
const TracesStability = component.StabilityLevelAlpha
