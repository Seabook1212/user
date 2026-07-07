# User Service

This repository contains the enhanced Sock Shop `user` service used by the
EviRCA benchmark:

> EviRCA: An Evidence-Aware Skill-Based LLM Agent and a Telemetry-Rich
> Multi-Modal Benchmark for Microservice Root Cause Analysis

The service stores and serves Sock Shop customer accounts, addresses, and
payment cards. It is a Go 1.22 modernization of the original Sock Shop user
service, with additional observability and robustness changes for reproducible
microservice root cause analysis (RCA) experiments.

## Role in the EviRCA Benchmark

EviRCA builds a telemetry-rich benchmark on an enhanced Sock Shop deployment.
This service is one of the Go services in that system, alongside services such
as `catalogue` and `payment`. In the benchmark, `user` contributes telemetry for
account-related request paths and MongoDB-backed dependency behavior.

The benchmark uses synchronized metrics, logs, traces, service topology, Chaos
Mesh fault-injection artifacts, upgraded service implementations, and
fine-grained labels to evaluate RCA at service, pod, service-fault, and
pod-fault granularities.

## What This Service Does

- Provides REST endpoints for customer, address, card, login, register, delete,
  health, and Prometheus metrics operations.
- Persists customer, address, and card data in MongoDB.
- Emits Prometheus HTTP metrics, including request latency, request count,
  in-flight requests, request size, and response size.
- Propagates and emits OpenTracing/Zipkin-compatible spans for HTTP requests
  and MongoDB operations.
- Adds Kubernetes metadata to trace spans when `CONTAINER_NAME`, `POD_NAME`,
  `POD_NAMESPACE`, and `NODE_NAME` are available.
- Emits trace-aware structured logs with operation names, trace IDs, span IDs,
  HTTP status codes, latency, result summaries, and classified errors.
- Includes dependency initialization retry behavior, HTTP timeouts, structured
  error responses, and panic recovery instrumentation.

## API

The service exposes the original Sock Shop user API surface:

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Health check |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/login` | Login with HTTP Basic Auth |
| `POST` | `/register` | Register a customer |
| `GET` | `/customers` or `/customers/{id}` | List or fetch customers |
| `POST` | `/customers` | Create a customer |
| `GET` | `/addresses` or `/addresses/{id}` | List or fetch addresses |
| `POST` | `/addresses` | Create an address |
| `GET` | `/cards` or `/cards/{id}` | List or fetch cards |
| `POST` | `/cards` | Create a card |
| `DELETE` | `/{entity}/{id}` | Delete a customer, address, or card |

The OpenAPI specification is in `apispec/user.json`.

## Configuration

Common runtime options and environment variables:

| Name | Description | Default |
| --- | --- | --- |
| `-port` | HTTP listen port | `8084` for native runs, `80` or `8080` in container images |
| `-mongo-host` / `MONGO_HOST` | MongoDB host | empty natively, `user-db` in Docker images |
| `-mongo-user` / `MONGO_USER` | MongoDB username | empty |
| `-mongo-password` / `MONGO_PASS` | MongoDB password | empty |
| `-zipkin` / `ZIPKIN` | Zipkin collector URL | resolved from tracing environment |
| `ZIPKIN_BASE_URL` | Zipkin base URL before `/api/v2/spans` is appended | empty |
| `ZIPKIN_HOST` / `ZIPKIN_PORT` | Zipkin host and port | `jaeger-collector.observability.svc.cluster.local:9411` |
| `JAEGER_COLLECTOR_URL` / `JAEGER_ENDPOINT` | Jaeger/Zipkin-compatible collector URL | empty |

The tracing URL resolver normalizes collector URLs to the Zipkin v2 endpoint
ending in `/api/v2/spans`.

## Build

### Native Go

```bash
go mod download
go build -o bin/user main.go
```

### Docker

```bash
make dockerlocal
```

## Test

Run the full test suite in the test container:

```bash
make test
```

Run native coverage tests:

```bash
make cover
```

## Run

### Docker Compose

```bash
docker-compose up --build
```

The compose file starts both `user` and `user-db`. The service is published on
`http://localhost:8080`.

### Native

Start MongoDB first, then run the service:

```bash
docker-compose up -d user-db
go run . -port=8084 -mongo-host=localhost:27017
```

Check the service:

```bash
curl http://localhost:8084/health
curl http://localhost:8084/metrics
```

## Example Requests

```bash
curl http://localhost:8084/customers
curl http://localhost:8084/addresses
curl http://localhost:8084/cards
curl -u user:password http://localhost:8084/login
```

Seed test users and example credentials are defined in
`docker/user-db/scripts/customer-insert.js`.

## Zipkin / Trace Collection

For a local Zipkin-style trace collection test:

```bash
docker-compose -f docker-compose-zipkin.yml build
docker-compose -f docker-compose-zipkin.yml up
```

After the service and seed data are ready, open:

```text
http://localhost:9411/
```

Then run a few requests against the user API and search for traces in Zipkin.
Stop the stack with:

```bash
docker-compose -f docker-compose-zipkin.yml down
```
