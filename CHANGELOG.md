# Changelog

All notable changes to StatGuard are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
StatGuard uses [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Added

**Lakehouse table format support**
- `DeltaReader` — reads Apache Delta Lake tables directly from the transaction log
  - `read(path)` — current snapshot
  - `read_version(path, version)` — time-travel by version number
  - `read_as_of_timestamp(path, ms)` — time-travel by Unix timestamp
  - `read_two_versions(path, ref, cur)` — convenience pair for drift analysis
  - Auto-detected by `DataReader::read_file()` when `_delta_log/` directory is present
- `IcebergReader` — reads Apache Iceberg v1/v2 tables from the metadata directory
  - `read(path)` — current snapshot
  - `read_snapshot(path, snapshot_id)` — time-travel by snapshot ID
  - `read_as_of_timestamp(path, ms)` — time-travel by Unix timestamp
  - `read_ref(path, ref_name)` — read a named branch or tag (e.g. `"main"`)
  - `read_two_snapshots(path, ref_id, cur_id)` — convenience pair for drift analysis
  - `list_snapshots(path)` — enumerate all snapshots with timestamps and operations
  - Auto-detected by `DataReader::read_file()` when `metadata/` directory is present

**Additional file formats**
- `DataReader::read_avro(path)` — Apache Avro (uses Polars `avro` feature)
- `DataReader::read_orc(path)` — Apache ORC (opt-in via `--features orc`)
- Auto-detection extended: `.avro` and `.orc` extensions handled by `read_file()`

**Python API additions**
- `execute_delta(contract, table_path, version, reference_path, reference_version)`
- `compare_delta_versions(contract, table_path, reference_version, current_version)`
- `execute_iceberg(contract, table_path, snapshot_id, reference_snapshot)`
- `list_iceberg_snapshots(table_path)` → list of snapshot dicts

**Examples**
- `examples/lakehouse_pipeline.py` — end-to-end Delta + Iceberg + drift detection example

---

## [0.1.0] — 2026-06-15

### Added

**Core**
- PEG grammar DSL (`grammar.pest`) supporting `dataset`, `schema`, `quality`,
  `stats`, `anomalies`, and `stream` sections
- Severity prefixes: `@info`, `@warning`, `@error` (default), `@blocking`
- Full AST (`ast.rs`): `DataContract`, `FieldDef`, `QualityRule`, `StatsRule`,
  `AnomalyRule`, `StreamConfig`
- 3-pass compiler optimizer: deduplication → null-check fusion → cost-sort
- Compiled `ExecutionDag` with column grouping for parallel execution

**Schema validation**
- Type checking: `int`, `float`, `string`, `bool`, `date`, `datetime`, `bytes`
- Constraints: `not_null`, `unique`, `primary_key`, `positive`, `negative`,
  `coerce`, `regex=`, `between()`, `min=`, `max=`, `len()`, `enum=[]`

**Quality rules**
- Metrics: `completeness`, `uniqueness`, `validity`, `consistency`, `non_null_rate`
- Comparison operators: `>`, `<`, `>=`, `<=`, `==`, `!=`

**Drift detection**
- Population Stability Index (PSI)
- Kolmogorov–Smirnov (KS) statistic
- Stat functions: `mean`, `std`, `median`, `min`, `max`, `p05`, `p95`, `p99`, `p999`

**Anomaly detection**
- `detect_outliers(method="iqr"|"zscore")`
- `detect_duplicates`
- `detect_nulls`
- `detect_cardinality_explosion`
- `detect_pattern_breaks`

**Profiling**
- Per-column: null rate, distinct count (HyperLogLog precision=14), min/max/mean/std,
  percentiles (p05/p25/p50/p75/p95/p99), 10-bucket histogram
- Profiling runs on every execution at no extra cost

**Output**
- `ValidationReport` (JSON, pretty JSON, Prometheus text format)
- `DatasetHealthScore` with letter grade (A/B/C/D/F)
- `ExecutionSummary` one-liner for CI / logging

**IO**
- Auto-detecting file reader: Parquet, CSV, JSON/NDJSON, Arrow IPC
- `StreamingBatcher` for large files (batch-size slicing)
- `RowBuffer` for micro-batch streaming pipelines

**Python API**
- `DataContract.from_dsl(str)` / `DataContract.from_file(path)`
- `execute(contract, df, reference=None)` → `ValidationReport`
- `execute_file(contract, path, reference_path=None)` → `ValidationReport`
- `execute_streaming(contract, path, batch_size=10000)` → `List[ValidationReport]`
- `validate_dsl(str)` — syntax check without execution
- `statguard validate / check` CLI with JSON, summary, and Prometheus output formats
- `pip install statguard` / `uv add statguard`

**Tests**
- 25 unit + integration tests across all crates
- All tests pass on the first release

[Unreleased]: https://github.com/Mullassery/StatGuard/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Mullassery/StatGuard/releases/tag/v0.1.0
