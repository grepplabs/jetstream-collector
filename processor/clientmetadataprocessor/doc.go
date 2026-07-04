//go:generate mdatagen metadata.yaml

// Package clientmetadataprocessor provides an OpenTelemetry logs processor that
// extracts configured resource and telemetry values and stores them in the
// collector client metadata context.
package clientmetadataprocessor
