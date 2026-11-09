//go:generate mdatagen metadata.yaml

// Package clientmetadataprocessor provides OpenTelemetry logs, metrics, and traces processors that
// extract configured resource and telemetry values and store them in the
// collector client metadata context.
package clientmetadataprocessor
