# JetStream Collector OCB Build

This directory contains the OCB manifest for the `jetstream-collector` Collector distribution.

## Build

```bash
make -C cmd/jetstream-collector-ocb build
```

The generated Collector is written to `cmd/jetstream-collector-ocb/dist/`.

## Docker build

```bash
make -C cmd/jetstream-collector-ocb build-docker
```

This uses the official Collector Builder container and mounts the current directory at `/build`.
