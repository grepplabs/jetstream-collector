//go:generate mdatagen metadata.yaml

// Package partitionbyattrs provides an OpenTelemetry logs processor that splits
// incoming batches into multiple downstream batches based on configured resource
// and telemetry attributes.
package partitionbyattrsprocessor
