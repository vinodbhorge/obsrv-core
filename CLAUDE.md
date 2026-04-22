# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Full build with tests
mvn clean install

# Build without tests (faster)
mvn clean install -DskipTests

# Build a specific module (e.g., pipeline)
mvn clean install -DskipTests -f pipeline/pom.xml

# Package a specific submodule
mvn clean package -DskipTests -f pipeline/extractor/pom.xml
```

## Test Commands

```bash
# Run all tests
mvn clean install

# Run tests for a specific module
mvn scalatest:test -f pipeline/preprocessor/pom.xml

# Generate code coverage report
mvn scoverage:report
```

## Architecture Overview

**obsrv-core** is a Scala/Flink-based data streaming framework for data ingestion, validation, transformation, and routing. It is a multi-module Maven project.

### Top-Level Modules

| Module | Purpose |
|--------|---------|
| `framework/` | Core Flink streaming base classes, Kafka connector, utilities |
| `dataset-registry/` | Runtime dataset configuration management (PostgreSQL-backed) |
| `transformation-sdk/` | Spark-based library for applying data transformations |
| `pipeline/` | Flink streaming jobs (see submodules below) |
| `data-products/` | Spark-based analytics and data products |

### Pipeline Submodules (`pipeline/`)

| Submodule | Purpose |
|-----------|---------|
| `extractor` | Splits batch event payloads into individual events |
| `preprocessor` | Schema validation and deduplication |
| `denormalizer` | Window-based enrichment using Redis/PostgreSQL lookups |
| `transformer` | Applies custom transformations via `transformation-sdk` |
| `dataset-router` | Routes processed events to Kafka topics or downstream sinks |
| `unified-pipeline` | Combines all pipeline stages into a single Flink job |
| `cache-indexer` | Indexes dataset cache for quick lookups |
| `hudi-connector` | Lakehouse (Apache Hudi) storage connector |

### Data Flow

```
Kafka (raw) → Extractor → Preprocessor → Denormalizer → Transformer → Dataset Router → Kafka (processed) / Druid / PostgreSQL
```

### Key Base Classes (in `framework/`)

- `BaseStreamTask` — All Flink streaming jobs extend this; sets up the Flink environment, Kafka sources/sinks, and the processing function chain.
- `BaseDatasetProcessFunction` — Base `ProcessFunction` for per-dataset processing logic; manages routing of system events and metrics.
- `FlinkKafkaConnector` — Wraps Flink's Kafka connector with project-specific serialization and topic configuration.
- `DatasetRegistry` — Loads and caches dataset schema/config from PostgreSQL at runtime.

### Technology Stack

- **Language**: Scala 2.12, Java 11
- **Streaming**: Apache Flink 1.20.0
- **Messaging**: Apache Kafka 3.7.1
- **State/Lookup**: Redis (Jedis), PostgreSQL
- **Analytics Storage**: Apache Druid
- **Data Lake**: Apache Hudi 1.0.2
- **Config**: TypeSafe Config (HOCON, `.conf` files)
- **Observability**: OpenTelemetry 1.42.1
- **Testing**: ScalaTest 3.0.6, ScalaMock, embedded-kafka, embedded-postgres, embedded-redis

### Configuration

Jobs are configured via HOCON `.conf` files (TypeSafe Config). Key settings include Kafka broker/topic names, Redis hosts/databases, PostgreSQL connection details, Flink parallelism, checkpointing intervals, and window durations. Each pipeline job has its own config file under `pipeline/<job>/src/main/resources/`.

### CI/CD

- PRs trigger `mvn clean install` via `.github/workflows/pull_request.yaml`.
- Merges to main build Docker images (base image: `sanketikahub/flink:1.20-scala_2.12-java11`) and deploy via Terragrunt (AWS/Azure) using `.github/workflows/build_and_deploy.yaml`.
