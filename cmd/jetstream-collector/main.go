package main

import (
	"os"

	"github.com/elastic/opentelemetry-collector-components/extension/awscredentialsproviderextension"
	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"
	"github.com/grepplabs/jetstream-collector/confmap/provider/openbaoprovider"
	"github.com/grepplabs/jetstream-collector/exporter/jetstreamexporter"
	"github.com/grepplabs/jetstream-collector/exporter/s3exporter"
	"github.com/grepplabs/jetstream-collector/processor/clientmetadataprocessor"
	"github.com/grepplabs/jetstream-collector/processor/kubemappingprocessor"
	"github.com/grepplabs/jetstream-collector/processor/partitionbyattrsprocessor"
	"github.com/grepplabs/jetstream-collector/receiver/jetstreamreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/basicauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/bearertokenauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/oidcauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/opampextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/pprofextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/httpprovider"
	"go.opentelemetry.io/collector/confmap/provider/httpsprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/exporter/nopexporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/memorylimiterextension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/processor/memorylimiterprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/nopreceiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
)

func main() {
	set := otelcol.CollectorSettings{
		Factories: func() (otelcol.Factories, error) {
			return otelcol.Factories{
				Receivers: map[component.Type]receiver.Factory{
					component.MustNewType("jetstream"): jetstreamreceiver.NewFactory(), // owned
					component.MustNewType("otlp"):      otlpreceiver.NewFactory(),
					component.MustNewType("nop"):       nopreceiver.NewFactory(),
					component.MustNewType("loadgen"):   loadgenreceiver.NewFactory(),
				},
				Processors: map[component.Type]processor.Factory{
					component.MustNewType("attributes"):       attributesprocessor.NewFactory(),
					component.MustNewType("batch"):            batchprocessor.NewFactory(),
					component.MustNewType("filter"):           filterprocessor.NewFactory(),
					component.MustNewType("memory_limiter"):   memorylimiterprocessor.NewFactory(),
					component.MustNewType("partitionbyattrs"): partitionbyattrsprocessor.NewFactory(),
					component.MustNewType("clientmetadata"):   clientmetadataprocessor.NewFactory(),
					component.MustNewType("kubemapping"):      kubemappingprocessor.NewFactory(),
					component.MustNewType("transform"):        transformprocessor.NewFactory(),
				},
				Exporters: map[component.Type]exporter.Factory{
					component.MustNewType("jetstream"): jetstreamexporter.NewFactory(), // owned
					component.MustNewType("s3"):        s3exporter.NewFactory(),
					component.MustNewType("debug"):     debugexporter.NewFactory(),
					component.MustNewType("otlp"):      otlpexporter.NewFactory(),
					component.MustNewType("otlphttp"):  otlphttpexporter.NewFactory(),
					component.MustNewType("nop"):       nopexporter.NewFactory(),
				},
				Connectors: map[component.Type]connector.Factory{
					component.MustNewType("routing"): routingconnector.NewFactory(),
				},
				Extensions: map[component.Type]extension.Factory{
					component.MustNewType("basicauth"):              basicauthextension.NewFactory(),
					component.MustNewType("bearertokenauth"):        bearertokenauthextension.NewFactory(),
					component.MustNewType("opamp"):                  opampextension.NewFactory(),
					component.MustNewType("oidcauth"):               oidcauthextension.NewFactory(),
					component.MustNewType("memory_limiter"):         memorylimiterextension.NewFactory(),
					component.MustNewType("pprof"):                  pprofextension.NewFactory(),
					component.MustNewType("file_storage"):           filestorage.NewFactory(),
					component.MustNewType("awscredentialsprovider"): awscredentialsproviderextension.NewFactory(),
				},
				Telemetry: otelconftelemetry.NewFactory(),
			}, nil
		},
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				ProviderFactories: []confmap.ProviderFactory{
					openbaoprovider.NewFactory(),
					fileprovider.NewFactory(),
					envprovider.NewFactory(),
					httpprovider.NewFactory(),
					httpsprovider.NewFactory(),
					yamlprovider.NewFactory(),
				},
				DefaultScheme: "file",
			},
		},
	}

	cmd := otelcol.NewCommand(set)
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
