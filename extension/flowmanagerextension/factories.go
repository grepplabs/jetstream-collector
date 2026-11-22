package flowmanagerextension

import (
	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/extension/badgerextension"
	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/extension/pebbleextension"
	"github.com/elastic/opentelemetry-collector-components/extension/awscredentialsproviderextension"
	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"
	"github.com/grepplabs/jetstream-collector/exporter/jetstreamexporter"
	"github.com/grepplabs/jetstream-collector/exporter/s3exporter"
	"github.com/grepplabs/jetstream-collector/extension/s3eventresolverextension"
	"github.com/grepplabs/jetstream-collector/extension/s3jetstreameventextension"
	"github.com/grepplabs/jetstream-collector/processor/celpartitionprocessor"
	"github.com/grepplabs/jetstream-collector/processor/clientmetadataprocessor"
	"github.com/grepplabs/jetstream-collector/processor/clientmetadataselector"
	"github.com/grepplabs/jetstream-collector/processor/kubemappingprocessor"
	"github.com/grepplabs/jetstream-collector/processor/partitionbyattrsprocessor"
	"github.com/grepplabs/jetstream-collector/receiver/jetstreamreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/basicauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/bearertokenauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/oidcauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/opampextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/pprofextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/exporter/nopexporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/memorylimiterextension"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/processor/memorylimiterprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/nopreceiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
)

// factories is the explicit allowlist used by dynamic flows. Keep it aligned
// with the factories exposed in the Collector distribution.
type factories struct {
	receivers  map[component.Type]receiver.Factory
	processors map[component.Type]processor.Factory
	exporters  map[component.Type]exporter.Factory
	extensions map[component.Type]extension.Factory
}

func defaultFactories() factories {
	return factories{
		receivers: map[component.Type]receiver.Factory{
			component.MustNewType("otlp"):      otlpreceiver.NewFactory(),
			component.MustNewType("jetstream"): jetstreamreceiver.NewFactory(),
			component.MustNewType("nop"):       nopreceiver.NewFactory(),
			component.MustNewType("loadgen"):   loadgenreceiver.NewFactory(),
			component.MustNewType("kafka"):     kafkareceiver.NewFactory(),
		},
		processors: map[component.Type]processor.Factory{
			component.MustNewType("attributes"):             attributesprocessor.NewFactory(),
			component.MustNewType("batch"):                  batchprocessor.NewFactory(),
			component.MustNewType("filter"):                 filterprocessor.NewFactory(),
			component.MustNewType("memory_limiter"):         memorylimiterprocessor.NewFactory(),
			component.MustNewType("transform"):              transformprocessor.NewFactory(),
			component.MustNewType("celpartition"):           celpartitionprocessor.NewFactory(),
			component.MustNewType("partitionbyattrs"):       partitionbyattrsprocessor.NewFactory(),
			component.MustNewType("clientmetadata"):         clientmetadataprocessor.NewFactory(),
			component.MustNewType("clientmetadataselector"): clientmetadataselector.NewFactory(),
			component.MustNewType("kubemapping"):            kubemappingprocessor.NewFactory(),
		},
		exporters: map[component.Type]exporter.Factory{
			component.MustNewType("debug"):         debugexporter.NewFactory(),
			component.MustNewType("otlp"):          otlpexporter.NewFactory(),
			component.MustNewType("otlphttp"):      otlphttpexporter.NewFactory(),
			component.MustNewType("jetstream"):     jetstreamexporter.NewFactory(),
			component.MustNewType("s3"):            s3exporter.NewFactory(),
			component.MustNewType("nop"):           nopexporter.NewFactory(),
			component.MustNewType("kafka"):         kafkaexporter.NewFactory(),
			component.MustNewType("loadbalancing"): loadbalancingexporter.NewFactory(),
		},
		extensions: map[component.Type]extension.Factory{
			component.MustNewType("awscredentialsprovider"): awscredentialsproviderextension.NewFactory(),
			component.MustNewType("badger"):                 badgerextension.NewFactory(),
			component.MustNewType("basicauth"):              basicauthextension.NewFactory(),
			component.MustNewType("bearertokenauth"):        bearertokenauthextension.NewFactory(),
			component.MustNewType("file_storage"):           filestorage.NewFactory(),
			component.MustNewType("health_check"):           healthcheckextension.NewFactory(),
			component.MustNewType("jetstreams3event"):       s3jetstreameventextension.NewFactory(),
			component.MustNewType("memory_limiter"):         memorylimiterextension.NewFactory(),
			component.MustNewType("oidcauth"):               oidcauthextension.NewFactory(),
			component.MustNewType("opamp"):                  opampextension.NewFactory(),
			component.MustNewType("pebble"):                 pebbleextension.NewFactory(),
			component.MustNewType("pprof"):                  pprofextension.NewFactory(),
			component.MustNewType("s3eventresolver"):        s3eventresolverextension.NewFactory(),
		},
	}
}
