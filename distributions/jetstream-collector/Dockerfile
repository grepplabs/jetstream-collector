FROM alpine:3.24 AS certs
RUN apk --update add ca-certificates


FROM --platform=$BUILDPLATFORM golang:1.26.7 AS build-stage

ARG TARGETOS
ARG TARGETARCH

WORKDIR /build

COPY distributions distributions
COPY exporter exporter
COPY internal internal
COPY pkg pkg
COPY receiver receiver
COPY processor processor
COPY confmap confmap

WORKDIR /build/distributions/jetstream-collector

RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build GO111MODULE=on go install go.opentelemetry.io/collector/cmd/builder@v0.159.0
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 builder --config builder.yaml

FROM scratch
ARG USER_UID=10001
USER ${USER_UID}

COPY distributions/jetstream-collector/config.yaml /etc/jetstream-collector/config.yaml
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --chmod=755 --from=build-stage /build/distributions/jetstream-collector/dist/jetstream-collector /jetstream-collector

ENTRYPOINT ["/jetstream-collector"]
CMD ["--config", "/etc/jetstream-collector/config.yaml"]
EXPOSE 4317 4318
