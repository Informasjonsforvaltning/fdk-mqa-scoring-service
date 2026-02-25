# FDK MQA Scoring Service – Project Context (Claude)

Project-specific context for working in this codebase.

## Overview

**fdk-mqa-scoring-service** is a Rust service in the FDK (Felles Datakatalog) Metadata Quality Assessment (MQA) pipeline. It reads MQA assessment events from Kafka, computes quality scores from RDF assessment graphs, and pushes those scores to an external scoring API.

## Architecture

```
Kafka (mqa-events, Avro) → Consumer workers (4) → Decode → Load RDF graph
                                                           → calculate_score (score_graph + assessment_graph)
                                                           → POST to SCORING_API_URL
HTTP server :8080 → /ping, /ready (Kafka connected?), /metrics (Prometheus)
```

- **Binary:** `src/bin/fdk-mqa-scoring-service.rs` – starts actix-web on `0.0.0.0:8080` and 4 Kafka consumer tasks via `run_async_processor`.
- **Library:** Core logic lives under `src/`; binary only wires HTTP and Kafka.

## Data Flow

1. **Kafka message** – Avro payload decoded with schema registry; parsed into `InputEvent` (e.g. `MqaEvent`) in `schemas.rs`.
2. **Event types** – `MqaEventType`: `PropertiesChecked`, `UrlsChecked`, `DcatComplienceChecked`; each carries `fdkId`, `graph` (RDF string), `timestamp`.
3. **Assessment graph** – `graph` is loaded into `AssessmentGraph` (oxigraph store). Vocab: DCAT, DQV, dcatno-mqa (see `vocab.rs`).
4. **Score definitions** – `ScoreGraph` loads dimension/metric definitions from RDF; used by `score::calculate_score`.
5. **Scoring** – `calculate_score(assessment_graph, score_definitions)` returns dataset and distribution scores. Results are converted via `json_conversion::convert_scores` and sent as `UpdateRequest` to the scoring API.

## Key Types and Files

| Concept           | Types / location                                                                                                                                            |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Kafka / config    | `kafka.rs`: `BROKERS`, `SCHEMA_REGISTRY`, `INPUT_TOPIC`, `SCORING_API_URL`, `API_KEY` (env); `create_consumer`, `create_sr_settings`, `run_async_processor` |
| Events            | `schemas.rs`: `InputEvent`, `MqaEvent`, `MqaEventType`                                                                                                      |
| RDF assessment    | `assessment_graph.rs`: `AssessmentGraph`, `AssessmentNode`; methods: `load`, `dataset`, `distributions`, `quality_measurements`, `insert_scores`, etc.      |
| Score definitions | `score_graph.rs`: `ScoreGraph`, `ScoreDefinitions`, `ScoreDimension`, `ScoreMetric`                                                                         |
| Score calculation | `score.rs`: `Score`, `DimensionScore`, `MetricScore`; `calculate_score`, `best_score` (internal)                                                            |
| Scoring API       | `json_conversion.rs`: `UpdateRequest`, `Scores`, `convert_scores`                                                                                           |
| Errors            | `error.rs`: `Error` enum (thiserror, wraps io, oxigraph, kafka, avro, reqwest, etc.)                                                                        |

## Environment and Deployment

- **HTTP:** Bind `0.0.0.0:8080`; endpoints: `GET /ping` → "pong", `GET /ready` → 200 if Kafka is ready, `GET /metrics` → Prometheus.
- **Kafka:** Defaults: `BROKERS=localhost:9092`, `SCHEMA_REGISTRY=http://localhost:8081`, `INPUT_TOPIC=mqa-events`. Readiness is set when consumer successfully fetches metadata.
- **Scoring API:** `SCORING_API_URL` (default `http://localhost:8082`), `API_KEY` for auth.
- **Local run:** `docker compose up -d` (Kafka + schema-registry), then run the release binary. Schema registration is done by `kafka/create_schemas.sh` (see `docker-compose.yaml`).

## Tests

- **Integration:** `tests/integration_test.rs` – uses `kafka_utils` (producer, consumer helpers) and `httptest` to mock the scoring API; asserts end-to-end transformation from RDF input to expected scores/API calls.
- **Fixtures:** `tests/data/` – Turtle assessments and expected JSON scores.
- **Running:** `cargo test ./tests`; full flow (with Docker) via `make test` (brings up compose, runs tests, tears down).

## Conventions

- **Errors:** Use `crate::error::Error`; prefer `?` and `thiserror` derives.
- **Logging:** `tracing` with JSON and env filter; use spans and fields in hot paths (e.g. message handling in `kafka.rs`).
- **RDF:** All assessment and score-definition IRIs and namespaces in `vocab.rs`; oxigraph for store and SPARQL; Sophia for Turtle/JSON-LD parse/serialize where used.
- **New features:** Prefer extending existing modules (e.g. new event types in `schemas.rs`, new score logic in `score.rs`/`score_graph.rs`) and reusing `AssessmentGraph`/`ScoreGraph` patterns.

## Useful Commands

```bash
cargo build --release
cargo test ./tests
./target/release/fdk-mqa-scoring-service --help
docker compose up -d   # before running the service
```

For more on the broader system and MQA subsystem, see the [architecture documentation](https://github.com/Informasjonsforvaltning/architecture-documentation) wiki.
