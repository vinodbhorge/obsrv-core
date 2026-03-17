# Obsrv Core – Copilot Instructions

## Build & Test

```bash
# Build all modules (skip tests)
mvn clean install -DskipTests

# Build and run all tests
mvn clean install

# Build a single module
mvn clean install -pl framework -DskipTests
mvn clean install -pl pipeline/extractor -DskipTests

# Run tests for a single module
mvn test -pl framework
mvn test -pl dataset-registry

# Coverage report (scoverage)
mvn scoverage:report
```

Tests use ScalaTest (FlatSpec) with embedded Redis (port 6340) and embedded Postgres (port 5432) — no external services required.

## Architecture

This is a **Maven multi-module Scala/Flink project** (Scala 2.12, Flink 1.20, Java 11):

```
pom.xml (root aggregator)
├── framework/          – Core Flink abstractions, utilities, model enums
├── dataset-registry/   – PostgreSQL-backed dataset config; BaseDatasetProcessFunction
├── transformation-sdk/ – Expression-based transformation engine
└── pipeline/
    ├── extractor/          – Batch event extraction
    ├── preprocessor/       – Dedup + schema validation
    ├── denormalizer/       – Redis-based denormalization
    ├── transformer/        – Field transformation
    ├── dataset-router/     – Routes events to Druid topics
    ├── cache-indexer/      – Indexes master data into Redis
    └── unified-pipeline/   – Single-job wrapper for the full pipeline
```

**Data flow (Kafka topics):**
`ingest` → extractor → `raw` → preprocessor → `unique` → denormalizer → `denorm` → transformer → `transform` → dataset-router → `stats`

Failed events at any stage are routed to `failed`/`invalid`/`duplicate` topics. System telemetry events go to `system.events`.

## Key Conventions

### Event Envelope
Every event carries an `obsrv_meta` map that tracks pipeline state. Always read/write through `BaseFunction` helpers:
- `markSuccess(event, producer)` / `markFailed(event, error, producer)` / `markSkipped` / `markPartial`
- `markComplete(event, dataVersion)` – called at end of pipeline, stamps timing stats
- The `flags` sub-map records each producer's status; `timespans` records per-stage latency

### Creating a New Pipeline Job
1. Extend `BaseJobConfig[mutable.Map[String, AnyRef]]` for the job config (reads a `.conf` file via typesafe config, always includes `baseconfig.conf`)
2. Extend `BaseDatasetProcessFunction` for the core logic – it handles dataset lookup, missing-dataset errors, and missing-event errors before calling `processElement(dataset, event, ctx, metrics)`
3. Extend `BaseStreamTask[mutable.Map[String, AnyRef]]` for the job wiring; override `processStream()` to connect operators and add sinks
4. Use `addDefaultSinks(stream, config, kafkaConnector)` to wire the system-events and failed-events side outputs automatically

For windowed functions extend `BaseDatasetWindowProcessFunction` / `WindowBaseProcessFunction` instead.

### Configuration Pattern
Each job has `src/main/resources/<job>.conf` that starts with `include "baseconfig.conf"` then overrides job-specific keys. Config is loaded via `ConfigFactory.parseFile` (if `--config.file.path` arg provided) or `ConfigFactory.load("<job>.conf")`.

### Side Outputs
Side outputs are the primary mechanism for routing events to different Kafka topics. Each config class defines `OutputTag` fields (e.g., `successTag()`, `failedEventsOutputTag()`, `duplicateEventOutputTag`). Do not write directly to topics from inside process functions.

### Metrics
Every process function declares `getMetricsList(): MetricsList` returning dataset IDs and counter names. Increment with `metrics.incCounter(datasetId, metricName)`. Metrics are exposed as Flink gauges that reset on read.

### DatasetRegistry
`DatasetRegistry` is a singleton that lazy-loads all datasets from Postgres on first access and caches them in-memory. It falls back to a DB lookup on cache miss. Tests must bootstrap Postgres with the dataset schema before using any registry-dependent code.

### Coverage Exclusions
Code paths that can only run inside a real Flink cluster are wrapped with:
```scala
// $COVERAGE-OFF$
...
// $COVERAGE-ON$
```
This is a project-wide convention for `main()` methods and `process()` entry points.

### Error Codes
All error codes are defined in `ErrorConstants` (e.g., `ERR_EXT_1001`). Use existing constants before adding new ones; follow the prefix convention (`ERR_EXT_`, `ERR_PP_`, `ERR_DENORM_`, `ERR_TRANSFORM_`, etc.).

### OTel Integration
`OTelMetricsGenerator.generateOTelSystemEventMetric(systemEvent)` is called inside `generateSystemEvent()`. OTel is disabled by default (`otel.enable=false` in `baseconfig.conf`); enable via config for local metrics export.
