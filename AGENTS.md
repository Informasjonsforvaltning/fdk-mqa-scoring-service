# FDK MQA Scoring Service – Agent Guide

This file helps AI agents work effectively in this repository.

## What This Project Does

Rust service that:

- Consumes **Kafka** messages (Avro, schema registry) with MQA (Metadata Quality Assessment) events
- Parses RDF assessment graphs (Turtle/JSON-LD), computes **scores** from quality measurements
- Posts score updates to an external **scoring API**
- Exposes HTTP **health** and **metrics** (Prometheus) on port 8080

## Tech Stack

- **Rust** (edition 2021), **Cargo**
- **actix-web** – HTTP server (ping, ready, metrics)
- **rdkafka** + **schema_registry_converter** – Kafka consumer, Avro decode
- **oxigraph** – RDF store, SPARQL
- **sophia\_\*** – Turtle/JSON-LD parsing and serialization
- **thiserror** – error types
- **tracing** – logging

## Layout

| Path                                 | Purpose                                                         |
| ------------------------------------ | --------------------------------------------------------------- |
| `src/lib.rs`                         | Library root; modules and visibility                            |
| `src/bin/fdk-mqa-scoring-service.rs` | Binary: HTTP server + Kafka workers                             |
| `src/kafka.rs`                       | Kafka consumer, message handling, scoring API calls             |
| `src/assessment_graph.rs`            | RDF assessment graph (oxigraph), scores, Turtle/JSON-LD         |
| `src/score_graph.rs`                 | Score definitions (dimensions/metrics) from RDF                 |
| `src/score.rs`                       | Score calculation from measurements (internal)                  |
| `src/schemas.rs`                     | Kafka event types (MqaEvent, MqaEventType)                      |
| `src/json_conversion.rs`             | DTOs and conversion for scoring API                             |
| `src/error.rs`                       | `Error` enum (thiserror)                                        |
| `src/vocab.rs`                       | RDF vocabulary IRIs (DCAT, DQV, dcatno-mqa)                     |
| `src/helpers.rs`                     | SPARQL/oxigraph helpers                                         |
| `src/metrics.rs`                     | Prometheus registration and exposure                            |
| `tests/`                             | Integration tests (Kafka + HTTP mock); `tests/data/` – fixtures |

## Running and Testing

- **Build:** `cargo build` (dev) or `cargo build --release`
- **Run:** Start Kafka + schema registry with `docker compose up -d`, then run the binary (e.g. `./target/release/fdk-mqa-scoring-service`)
- **Tests:** `cargo test ./tests` (integration tests may expect Kafka; see `Makefile` for full test flow with Docker)
- **Help:** `./target/release/fdk-mqa-scoring-service --help`

## Conventions for Agents

1. **Errors** – Use `crate::error::Error` and `thiserror`; propagate with `?` where appropriate.
2. **Logging** – Use `tracing` (e.g. `tracing::info!`, `tracing::error!`) with structured fields.
3. **RDF** – Assessment and score definitions live in oxigraph stores; vocab IRIs are in `src/vocab.rs`.
4. **Kafka** – Config via env: `BROKERS`, `SCHEMA_REGISTRY`, `INPUT_TOPIC`; readiness is tracked and exposed on `/ready`.
5. **API** – Scoring API base URL and key: `SCORING_API_URL`, `API_KEY`; see `src/json_conversion.rs` and `src/kafka.rs` for request shape.
6. **Rust** – Prefer existing patterns (e.g. `AssessmentGraph`, `ScoreGraph`, `calculate_score`) and avoid unnecessary dependencies.

## External Context

- Broader system: [architecture documentation](https://github.com/Informasjonsforvaltning/architecture-documentation) (Metadata Quality subsystem).
- README in repo root: prerequisites, run instructions, test commands.
