# modelsrv-otel-exporter

A self-contained certificate monitoring service for EmELand. It probes HTTPS endpoints for TLS certificate expiry and exposes the results as EmELand Findings via the standard modelsrv HTTP API.

Built as a minimal OpenTelemetry Collector distribution containing the **httpcheck receiver** and a custom **emeland exporter**. Runs standalone with no existing OTel infrastructure required.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  modelsrv-otel-exporter                                         │
│                                                                 │
│  ┌──────────────────┐              ┌──────────────────────────┐ │
│  │ httpcheck        │── metrics ──>│ emeland exporter         │ │
│  │ receiver         │              │                          │ │
│  │ (probes HTTPS    │              │ endpoint_mapping:        │ │
│  │  endpoints)      │              │   URL -> ApiInstance UUID│ │
│  └──────────────────┘              │                          │ │
│                                    │ ┌──────────────────────┐ │ │
│                                    │ │ in-memory modelsrv   │ │ │
│                                    │ │ (findings)           │ │ │
│                                    │ └──────────┬───────────┘ │ │
│                                    └────────────│─────────────┘ │
│                                                 │               │
│                              modelsrv HTTP API  │  :24200       │
└─────────────────────────────────────────────────│───────────────┘
                                                  │
                           subscribes  ┌──────────▼──────────────┐
                           via         │  modelsrv               │
                           /events/    │  (aggregator, UI...)    │
                           register    └─────────────────────────┘
```

### Data flow

1. The **httpcheck receiver** probes configured HTTPS endpoints and produces `httpcheck.tls.cert_remaining` (seconds until expiry) and `httpcheck.error` (probe failures).
2. The **emeland exporter** looks up each `http.url` in its `endpoint_mapping` to find the corresponding ApiInstance UUID, then applies the same decision table as native certprobe: probe errors → `CertificateProbeFailed`; remaining time → `CertificateExpiringSoon` / `CertificateExpired` / clear.
3. Findings live in an in-memory modelsrv model exposed via the standard HTTP API.
4. Downstream modelsrv instances subscribe and get finding events pushed in real time.

### Code structure

```
cmd/modelsrv-otel-exporter/     Collector binary (httpcheck + emeland exporter)
internal/
  sensor/                       Embedded modelsrv (HTTP API, event replication)
  emelandexporter/
    mapping.go                  Pure logic: duration -> Finding events
    exporter.go                 ConsumeMetrics: resolve URL, call mapper, emit
    config.go                   endpoint_mapping, listen_addr, threshold
    factory.go                  OTel component registration
```

## Usage

**1. Generate the collector config** from a running modelsrv:

```sh
modelsrv certprobe \
  --otel-config-out collector.yaml \
  --server http://modelsrv:8080/api/ \
  --subscriber http://modelsrv:8080
```

This queries ApiInstances for their endpoint annotations and produces a ready-to-run config with the httpcheck targets and the URL-to-UUID mapping.

**2. Run the exporter:**

```sh
modelsrv-otel-exporter --config collector.yaml
```

That's it. Certificates get probed, findings get created, and modelsrv receives them via subscription.

### Generated config example

```yaml
receivers:
  httpcheck:
    collection_interval: 5m
    metrics:
      httpcheck.tls.cert_remaining:
        enabled: true
    targets:
      - method: GET
        endpoint: https://api.example.com:443/health

exporters:
  emeland:
    listen_addr: 0.0.0.0:24200
    expiry_threshold: 720h
    subscribers:
      - http://modelsrv:8080
    endpoint_mapping:
      https://api.example.com:443/health: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee

service:
  pipelines:
    metrics:
      receivers: [httpcheck]
      exporters: [emeland]
```

## Configuration reference

| Field | Default | Description |
|-------|---------|-------------|
| `listen_addr` | `localhost:24200` | Address for the modelsrv HTTP API |
| `expiry_threshold` | `720h` (30 days) | How far before expiry to raise CertificateExpiringSoon |
| `subscribers` | `[]` | Downstream modelsrv URLs for event replication |
| `endpoint_mapping` | `{}` | URL -> ApiInstance UUID map |

## Building

```sh
make ci          # lint, build, test
make run         # run with config/otel-collector.yaml
```

## Design note: why this exists

The modelsrv server already has built-in certificate probing (`--enable-certprobe`). It runs a background scheduler that probes ApiInstance endpoints and creates findings in the same process. That works fine for simple deployments.

This binary exists for cases where you want to decouple the probing:

- Probing from a different network segment (e.g. outside a cluster)
- Scaling probing independently of the main modelsrv
- Running as a sidecar or CronJob in Kubernetes
- Keeping the main modelsrv lightweight

| Approach | When to use |
|----------|-------------|
| `modelsrv server --enable-certprobe` | Small setups, modelsrv can reach all endpoints |
| `modelsrv-otel-exporter` (this binary) | Separate network, independent scaling, dedicated probe workload |

Both produce identical Finding IDs (deterministic SHA1 from ApiInstance UUID + FindingKind), so you can switch between them or even run both without creating duplicates.
