# FDK MQA Scoring Service

This application provides an API for retrieving scores calculated based on assessments provided by MQA components
consumed by kafka messages.

For a broader understanding of the system’s context, refer to
the [architecture documentation](https://github.com/Informasjonsforvaltning/architecture-documentation) wiki. For more
specific context on this application, see the **Metadata Quality** subsystem section.

## Getting Started
These instructions will give you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Ensure you have the following installed:
- [Rust](https://www.rust-lang.org/tools/install)
- [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Running locally

Clone the repository:

```sh
git clone https://github.com/Informasjonsforvaltning/fdk-mqa-scoring-service.git
cd fdk-mqa-scoring-service
```

Build for development:

```sh
cargo build --verbose
```

Build release:

```sh
cargo build --release
```

Start Kafka (Docker Compose) and the application

```sh
docker compose up -d
./target/release/fdk-mqa-scoring-service
```

Show help:

```sh
./target/release/fdk-mqa-scoring-service --help
```

### Running tests

```sh
cargo test ./tests
```
