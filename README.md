# Statguardian

**A Python library for data quality, validation, and statistical drift monitoring in production data pipelines — built in Rust.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![PyPI: statguardian](https://img.shields.io/badge/PyPI-statguardian-blue)](https://pypi.org/project/statguardian/)
[![Version](https://img.shields.io/badge/version-0.1.0-blue)](https://github.com/Mullassery/statguardian/releases)
[![Rust](https://img.shields.io/badge/built%20with-Rust-orange.svg)](https://www.rust-lang.org)

Statguardian is a Python library for validating your datasets against a declarative contract — schema, quality rules, statistical drift, and anomaly detection — across every major file format and lakehouse table format. You write one contract file; Statguardian compiles it into an optimised columnar execution plan and runs it at Rust speed.

**Python is the frontend. Rust is the engine.**

## Install

Pick one:

```bash
pip install statguardian
```

OR

```bash
uv add statguardian
```

OR

```bash
curl -sSfL https://raw.githubusercontent.com/Mullassery/statguardian/main/install.sh | sh
```

See [INSTALL.md](INSTALL.md) for source builds and verification steps.

---

## Quick start

### 1. Write a contract

```
# orders.sg
dataset orders {
    schema {
        order_id:   string, not_null, unique, primary_key
        customer_id: string, not_null
        amount:     float,  positive, max=100000.0
        currency:   string, not_null, enum=["USD","EUR","GBP","JPY"]
        status:     string, not_null, enum=["pending","paid","cancelled","refunded"]
    }

    quality {
        @blocking: completeness(order_id) > 0.9999
        @warning:  uniqueness(order_id)   == 1.0
    }

    stats {
        amount.mean drift < 0.15
        amount.p95  drift < 0.25
    }

    anomalies {
        detect_outliers(amount, method="iqr")
        @blocking: detect_duplicates(order_id)
    }
}
```

### 2. Validate — any format

```python
import polars as pl
import statguard

contract = statguard.DataContract.from_file("orders.sg")

# Auto-detected from extension: Parquet, CSV, JSON, Avro, Arrow IPC, Delta, Iceberg
report = statguard.execute_file(contract, "orders.parquet")
report = statguard.execute_file(contract, "orders.csv")
report = statguard.execute_file(contract, "orders.avro")

# Delta Lake
report = statguard.execute_delta(contract, "/data/orders_delta/")
report = statguard.execute_delta(contract, "/data/orders_delta/", version=5)  # time travel

# Apache Iceberg
report = statguard.execute_iceberg(contract, "/data/orders_iceberg/")
report = statguard.execute_iceberg(contract, "/data/orders_iceberg/", snapshot_id=9876543)

# Polars DataFrame (in-memory).
# NOTE: passing a Polars DataFrame across the Rust boundary requires a Polars
# build compatible with StatGuard's (pyo3-polars 0.18 / Polars 0.44). With a
# newer Polars you may hit a `compat_level` error — prefer execute_file(), which
# reads the data on the Rust side and has no such coupling.
df = pl.read_parquet("orders.parquet")
report = statguard.execute(contract, df)

print(report.summary())
# [StatGuard] PASS ✓ | dataset=orders | score=0.97 (A) | rows=500000 | violations=2 | 3ms
```

### 3. Drift detection

```python
# Compare today vs yesterday
report = statguard.execute_delta(
    contract, "/data/orders_delta/",
    version=10,
    reference_path="/data/orders_delta/",
    reference_version=9,
)

# Iceberg snapshot comparison
snapshots = statguard.list_iceberg_snapshots("/data/orders_iceberg/")
report = statguard.execute_iceberg(
    contract, "/data/orders_iceberg/",
    snapshot_id=snapshots[-1]["snapshot_id"],
    reference_snapshot=snapshots[-2]["snapshot_id"],
)

for d in report.drift_results():
    print(f"{d['column']}.{d['stat']}: drift={d['drift']:.4f}  PSI={d['psi']:.4f}  KS={d['ks_stat']:.4f}")
```

### 4. CLI

```bash
# Validate any format — auto-detected
statguard validate --contract orders.sg --file orders.parquet
statguard validate --contract orders.sg --file /data/orders_delta/
statguard validate --contract orders.sg --file /data/orders_iceberg/

# Drift: compare two datasets
statguard validate --contract orders.sg --file today.parquet --reference yesterday.parquet

# Output formats
statguard validate --contract orders.sg --file data.parquet --format json
statguard validate --contract orders.sg --file data.parquet --format prometheus

# Fail CI on any violation
statguard validate --contract orders.sg --file data.parquet --fail-on-warning

# DSL syntax check
statguard check --contract orders.sg
```

→ Full CLI reference: [docs/CLI.md](docs/CLI.md)

### 5. Streaming

```python
reports = statguard.execute_streaming(contract, "huge.parquet", batch_size=50_000)
for i, r in enumerate(reports):
    if not r.passed:
        print(f"Batch {i} FAILED: {r.summary()}")
        break
```

### 6. Cloud storage (S3, GCS, Azure)

```python
report = statguard.execute_cloud(contract, "s3://bucket/events/2026/06/*.parquet")
report = statguard.execute_cloud(contract, "gs://bucket/events.csv")
report = statguard.execute_cloud(contract, "az://container/data/")

# Drift across two cloud datasets
report = statguard.execute_cloud(
    contract,
    uri="s3://bucket/events/today/",
    reference_uri="s3://bucket/events/yesterday/",
)
```

### 7. SQL databases and warehouses

```python
# PostgreSQL, MySQL, SQLite (pure Rust)
report = statguard.execute_sql(
    contract,
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    query="SELECT * FROM orders WHERE created_date >= '2026-01-01'",
)

# BigQuery, Snowflake, Redshift, Databricks, ClickHouse, DuckDB (Python layer)
report = statguard.execute_sql(
    contract,
    connection_string="bigquery://project/dataset",
    query="SELECT * FROM events LIMIT 1000000",
)

# Drift between two SQL queries
report = statguard.execute_sql(
    contract,
    connection_string="postgresql://localhost/db",
    query="SELECT * FROM events WHERE date = CURRENT_DATE",
    reference_query="SELECT * FROM events WHERE date = CURRENT_DATE - 1",
)
```

### 8. Apache Spark

```python
from pyspark.sql import SparkSession
import statguard

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

contract = statguard.DataContract.from_file("events.sg")
spark_df = spark.read.parquet("s3a://bucket/events/")

report = statguard.execute_spark(contract, spark_df)

# Drift between Spark DataFrames
today = spark.read.parquet("s3a://bucket/today/")
yesterday = spark.read.parquet("s3a://bucket/yesterday/")
report = statguard.execute_spark(contract, today, reference_spark_df=yesterday)
```

Works on: local, YARN, Kubernetes, Databricks, AWS EMR, Google Dataproc, Azure HDInsight.

---

## PII detection

Scan any Polars DataFrame for columns that appear to contain personally identifiable information, using two complementary methods: column-name heuristics (instant, zero data access) and regex pattern matching on a sample of string values.

```python
import polars as pl
import statguard

df = pl.read_parquet("customers.parquet")
findings = statguard.scan_pii(df)

print(statguard.pii_report(findings))
# PII scan — 3 finding(s):
#
#   [HIGH]   'email_address' — email (pattern: 1823/2000 values matched)
#   [MEDIUM] 'phone'         — phone (name: column name suggests PII)
#   [HIGH]   'ssn'           — ssn (pattern: 998/2000 values matched)
```

**Detected PII types:** email · phone · SSN · credit card · IP address · date of birth · passport · IBAN · name · address · date of birth · gender · nationality

```python
# Detailed findings
for f in findings:
    print(f.column, f.pii_type, f.risk, f.detection_method)

# Gate a pipeline — fail if high-risk PII found in unexpected columns
high_risk = [f for f in findings if f.risk == "high" and f.column not in ALLOWED_PII_COLS]
if high_risk:
    raise ValueError(f"Unexpected PII: {[f.column for f in high_risk]}")
```

```python
# Control sensitivity
findings = statguard.scan_pii(
    df,
    sample_rows=5_000,       # rows to scan for pattern matching (default: 2000)
    pattern_threshold=0.10,  # fraction that must match to flag (default: 0.05)
)
```

---

## Schema evolution detection

Compare two DataFrames and surface structural changes — added columns, removed columns, type changes — before they silently break a downstream pipeline.

```python
import statguard

yesterday = pl.read_parquet("events_yesterday.parquet")
today     = pl.read_parquet("events_today.parquet")

changes = statguard.detect_schema_changes(today, yesterday)
print(statguard.schema_evolution_report(changes))
# Schema evolution — 2 change(s):
#
#   [ERROR]   Column removed: 'legacy_id' (was Int64)
#   [WARNING] Column retyped: 'amount' Float32 → Float64
```

**Use as a pipeline gate:**

```python
# Raises ValueError listing all removed or retyped columns
statguard.assert_no_breaking_changes(today_df, yesterday_df)
```

**Customise severity:**

```python
changes = statguard.detect_schema_changes(
    today, yesterday,
    added_severity="warning",   # new columns are warnings (default: info)
    removed_severity="error",   # removed columns are errors (default)
    retyped_severity="error",   # type changes are errors (default: warning)
)
```

**Pass raw schema dicts instead of DataFrames:**

```python
changes = statguard.detect_schema_changes(
    {"id": "Int64", "amount": "Float64"},
    {"id": "Int64", "amount": "Float32", "legacy_id": "String"},
)
```

---

## HTML report

Generate a self-contained, dependency-free HTML report from any `ValidationReport`. Safe to email, commit as a CI artefact, or open offline.

```python
report = statguard.execute(contract, df)

with open("report.html", "w") as f:
    f.write(statguard.to_html(report))
```

The report includes: status badge, health score and grade, violations table (column · check · severity · message), drift results table (reference vs current values, PSI, KS), and column profiles (mean, std, p95, null rate, distinct count).

---

## Cross-column conditional assertions

Write conditional rules directly in the contract DSL — no Python required. A cross-column rule checks an assertion only on rows where a condition holds.

```
dataset orders {
    quality {
        completeness(order_id) > 0.999

        @blocking: assert amount > 0.0 when status == "paid"
        @warning:  assert discount >= 0.0 when status == "paid"
        assert amount > 0.0 when status == "refunded"
    }
}
```

**Syntax:** `[@severity:] assert <column> <op> <value> when <column> <op> <value>`

Rows where the `when` condition is false are skipped — only matching rows are checked. Violations include the row indices that failed.

| Value type | Example |
|---|---|
| Number | `assert amount > 0.0 when status == "paid"` |
| String | `assert status != "cancelled" when amount > 0.0` |
| Boolean | `assert is_verified == true when is_premium == true` |

String comparisons support `==` and `!=`. Numeric comparisons support all six operators (`>` `<` `>=` `<=` `==` `!=`).

---

## Custom Python validators

Register arbitrary Python functions as validators that run alongside the Rust engine — for checks that can't be expressed in the DSL.

```python
import statguard

@statguard.validator("email", severity="warning")
def no_example_domains(values):
    failing = [i for i, v in enumerate(values) if v and "example.com" in v]
    return (failing, f"{len(failing)} example.com address(es)") if failing else None

# Use "*" to run against every string column
@statguard.validator("*", severity="error")
def no_empty_strings(values):
    failing = [i for i, v in enumerate(values) if v == ""]
    return (failing, f"{len(failing)} empty string(s)") if failing else None

# Run all registered validators
extra = statguard.run_custom_validators(df)

# Manage the registry
print(statguard.list_validators())  # → {"email": ["no_example_domains"], ...}
statguard.clear_validators("email") # remove validators for one column
statguard.clear_validators()        # remove all
```

The function receives a plain Python list of column values and must return `(failing_row_indices, message)` or `None` (check passed).

---

## Parallel multi-file validation

Validate a glob of files concurrently against one contract. The GIL is released during the Rust validation call, so CPU-bound checks run in true parallel.

```python
contract = statguard.DataContract.from_file("orders.sg")

results = statguard.execute_files(contract, "data/orders_*.parquet", workers=8)

passed = [r for r in results if r.passed]
failed = [r for r in results if r.failed]
print(f"{len(passed)}/{len(results)} files passed")
```

**Stream results as they arrive** — useful for fail-fast pipelines:

```python
for result in statguard.execute_files_stream(contract, "data/**/*.parquet"):
    if not result.passed:
        print(f"FAIL {result.path}: {result.report.summary()}")
        break
```

`FileResult` fields: `.path` · `.report` · `.error` · `.passed` · `.failed`

---

## GPU acceleration (cuDF)

Validate RAPIDS cuDF DataFrames directly. StatGuard converts to Polars via the Arrow C Stream interface (zero-copy where CUDA unified memory is available) before passing to the Rust engine.

```python
import cudf, statguard

contract = statguard.DataContract.from_file("events.sg")
gdf = cudf.read_parquet("s3://bucket/events.parquet")

report = statguard.execute_cudf(contract, gdf)
print(report.summary())

# Drift detection with GPU DataFrames
report = statguard.execute_cudf(contract, gdf, reference_cudf_df=yesterday_gdf)

# Guard for environments without RAPIDS
if statguard.is_cudf_available():
    report = statguard.execute_cudf(contract, gdf)
else:
    report = statguard.execute(contract, polars_df)
```

Requires RAPIDS cuDF ≥ 23.08. Falls back to pandas host-memory conversion on older versions.

---

## Referential integrity

Check that foreign-key values in one DataFrame exist in the primary-key column of another — catching orphaned records before they break downstream joins.

```python
import polars as pl, statguard

orders    = pl.read_parquet("orders.parquet")
customers = pl.read_parquet("customers.parquet")

violations = statguard.check_referential_integrity(
    orders, customers,
    foreign_key="customer_id",
    primary_key="id",
    foreign_table="orders",
    primary_table="customers",
)

print(statguard.integrity_report(violations))
# Referential integrity — 1 violation(s):
#   [ERROR] orders.customer_id → customers.id: 142 orphaned value(s): ['C_99999', ...]
```

**Check multiple keys at once:**

```python
violations = statguard.check_all_foreign_keys(
    orders, dims,
    key_pairs=[("customer_id", "id"), ("product_id", "sku")],
)
```

**Gate a pipeline:**

```python
violations = statguard.check_referential_integrity(orders, customers, "customer_id", "id")
if violations:
    raise ValueError(statguard.integrity_report(violations))
```

---

## Why not just use pandera or Great Expectations?

You can — until the dataset is large, or you need drift detection, or you want one tool that covers files, Delta Lake, Iceberg, cloud storage, and SQL without gluing libraries together.

**100,000 rows × 4 columns, 5 checks — Apple M-series:**

| Tool | Best time | vs StatGuard |
|---|---|---|
| **StatGuard 0.1** | **2.0 ms** | baseline |
| Pure Python loops | 11.5 ms | 5.8× slower |
| pandera 0.31 (pandas) | 26.5 ms | 13× slower |
| Pydantic v2 (TypeAdapter bulk) | 43.5 ms | 22× slower |
| Pydantic v2 (row-by-row) | 46.2 ms | 23× slower |
| Great Expectations 1.18 | 50.4 ms | 25× slower |

> Pydantic allocates one Python object per row regardless of batch size. StatGuard never touches individual rows — it operates on entire Arrow columns.

See [BENCHMARKS.md](BENCHMARKS.md) for full methodology, scaling table, and reproduce steps.

**Feature comparison:**

| | Pydantic v2 | pandera | Great Expectations | WhyLogs | **StatGuard** |
|---|---|---|---|---|---|
| Performance | Row-by-row Python | Python/pandas | Python-heavy | Python | **Rust — 13–25× faster** |
| Schema / type validation | ✓ | ✓ | ✓ | ✗ | ✓ |
| Tabular quality rules | ✗ | ✓ | ✓ | ✗ | ✓ |
| Drift detection (PSI + KS) | ✗ | ✗ | ✗ | ✓ | ✓ |
| Anomaly detection | ✗ | ✗ | partial | partial | ✓ |
| Delta Lake (no Spark) | ✗ | ✗ | ✗ | ✗ | ✓ |
| Apache Iceberg (no Spark) | ✗ | ✗ | ✗ | ✗ | ✓ |
| Avro | ✗ | ✗ | partial | ✗ | ✓ |
| Streaming support | ✗ | ✗ | ✗ | partial | ✓ |
| PII detection | ✗ | ✗ | ✗ | ✗ | ✓ |
| Schema evolution detection | ✗ | ✗ | partial | ✗ | ✓ |
| Cross-column assertions in DSL | ✗ | partial | ✗ | ✗ | ✓ |
| Custom Python validators | ✗ | ✓ | ✓ | ✗ | ✓ |
| Parallel multi-file validation | ✗ | ✗ | ✗ | ✗ | ✓ |
| GPU / cuDF support | ✗ | ✗ | ✗ | ✗ | ✓ |
| Referential integrity checks | ✗ | ✗ | ✗ | ✗ | ✓ |
| HTML report | ✗ | ✗ | ✓ | ✗ | ✓ |
| Single contract DSL | ✗ | ✗ | ✗ | ✗ | ✓ |
| pip / uv install | ✓ | ✓ | ✓ | ✓ | ✓ |

---

## Format and connector compatibility

| | pandera | Great Expectations | Pydantic v2 | **StatGuard** |
|---|---|---|---|---|
| **Files** (Parquet, CSV, JSON, Avro, Arrow IPC) | ✓ via pandas | ✓ via pandas | ✗ load first | ✓ native |
| **Delta Lake** (no Spark) | ✗ | ✗ | ✗ | ✓ |
| **Apache Iceberg** (no Spark) | ✗ | ✗ | ✗ | ✓ |
| **Cloud** (S3, GCS, Azure) | via extras | ✓ native | ✗ | ✓ |
| **Spark DataFrames** | ✓ | ✓ native | ✗ | ✓ Arrow bridge |
| **SQL / warehouses** | via SQLAlchemy | 12 connectors | ✗ | 13 OSS connectors |

→ Full matrix: [docs/FORMAT_COMPATIBILITY.md](docs/FORMAT_COMPATIBILITY.md)

---

## DSL reference

```
dataset <name> {
    schema {
        <field>: <type>[, <constraint>]*
    }
    quality {
        [@<severity>:] <metric>(<field>) <op> <value>
    }
    stats {
        [@<severity>:] <field>.<stat> drift <op> <value>
    }
    anomalies {
        [@<severity>:] <fn>(<field>[, <arg>=<value>]*)
    }
    stream {              // optional — streaming window config
        window    = "5m"
        watermark = "30s"
        emit      = "on_window_close"
    }
}
```

### Types
`int` · `float` · `string` · `bool` · `date` · `datetime` · `bytes`

### Constraints

| Constraint | Example |
|---|---|
| `not_null` | `id: int, not_null` |
| `unique` | `email: string, unique` |
| `primary_key` | `id: int, primary_key` |
| `positive` / `negative` | `amount: float, positive` |
| `coerce` | `age: int, coerce` (type mismatch → warning, not blocking) |
| `regex=` | `email: string, regex="^[^@]+@[^@]+\.[^@]+$"` |
| `between(lo, hi)` | `age: int, between(0, 120)` |
| `min=` / `max=` | `score: float, min=0.0, max=1.0` |
| `len(min, max)` | `code: string, len(3, 10)` |
| `enum=[...]` | `status: string, enum=["A","B","C"]` |

### Cross-column conditional assertions

Validate a column conditionally based on the value of another column — directly in the contract DSL:

```
quality {
    @blocking: assert amount > 0.0 when status == "paid"
    @warning:  assert discount >= 0.0 when status == "paid"
    assert refund_amount <= amount when status == "refunded"
}
```

Syntax: `[@severity:] assert <column> <op> <literal> when <column> <op> <literal>`

Supported literal types: numbers (`0.0`, `-100`), strings (`"paid"`), booleans (`true`, `false`).
String comparisons support `==` and `!=`; numeric comparisons support all six operators.
Rows where the `when` condition is false are skipped — only rows meeting the condition are checked.

### Quality metrics
`completeness` · `uniqueness` · `validity` · `consistency` · `non_null_rate`

### Drift stat functions
`mean` · `std` · `median` · `min` · `max` · `p05` · `p95` · `p99` · `p999`

PSI and KS statistic are always computed alongside every drift rule — no extra config needed.

### Anomaly functions

| Function | Description |
|---|---|
| `detect_outliers(col, method="iqr")` | IQR 1.5× rule or z-score > 3σ |
| `detect_duplicates(col)` | Exact duplicate detection |
| `detect_nulls(col)` | Null-value anomalies |
| `detect_cardinality_explosion(col)` | Sudden cardinality spike |
| `detect_pattern_breaks(col, pattern=...)` | Regex pattern consistency |

### Severity levels
`@blocking` · `@error` (default) · `@warning` · `@info`

`@blocking` violations abort further column checks and set `report.passed = False`.

---

## Python API

```python
import statguard

# ── Contract ─────────────────────────────────────────────────────────────────
contract = statguard.DataContract.from_dsl("...")
contract = statguard.DataContract.from_file("orders.sg")
statguard.validate_dsl(dsl_string)   # syntax check only

# ── Core execution ────────────────────────────────────────────────────────────
statguard.execute(contract, polars_df, reference=None)
statguard.execute_file(contract, path, reference_path=None)
statguard.execute_streaming(contract, path, batch_size=10_000)

# ── Lakehouse ─────────────────────────────────────────────────────────────────
statguard.execute_delta(contract, table_path, version=None,
                        reference_path=None, reference_version=None)
statguard.compare_delta_versions(contract, table_path, ref_v, cur_v=None)
statguard.execute_iceberg(contract, table_path, snapshot_id=None,
                          reference_snapshot=None)
statguard.list_iceberg_snapshots(table_path)

# ── Cloud, SQL, Spark ─────────────────────────────────────────────────────────
statguard.execute_cloud(contract, uri, reference_uri=None)
statguard.execute_sql(contract, connection_string, query, reference_query=None)
statguard.execute_spark(contract, spark_df, reference_spark_df=None)

# ── PII detection ─────────────────────────────────────────────────────────────
findings = statguard.scan_pii(df, sample_rows=2_000, pattern_threshold=0.05)
print(statguard.pii_report(findings))   # human-readable summary

# ── Schema evolution ──────────────────────────────────────────────────────────
changes = statguard.detect_schema_changes(current_df, reference_df,
              added_severity="info", removed_severity="error",
              retyped_severity="warning")
print(statguard.schema_evolution_report(changes))
statguard.assert_no_breaking_changes(current_df, reference_df)  # raises on errors

# ── Report output ─────────────────────────────────────────────────────────────
report.passed            # bool
report.health_score      # float [0, 1]
report.grade             # "A" / "B" / "C" / "D" / "F"
report.row_count         # int
report.violation_count   # int
report.duration_ms       # int
report.violations()      # list[dict]
report.drift_results()   # list[dict]
report.column_profiles() # list[dict]
report.to_json()
report.to_json_pretty()
report.to_prometheus()
report.summary()         # one-line string

statguard.to_html(report)   # → self-contained HTML string

# ── Custom Python validators ──────────────────────────────────────────────────
@statguard.validator("amount", severity="warning")
def no_suspiciously_round_numbers(values):
    failing = [i for i, v in enumerate(values) if v and v == int(v) and v > 10_000]
    return (failing, f"{len(failing)} suspiciously round value(s)") if failing else None

violations = statguard.run_custom_validators(df)
print(statguard.list_validators())  # → {"amount": ["no_suspiciously_round_numbers"]}
statguard.clear_validators()        # remove all registered validators

# ── Parallel multi-file validation ────────────────────────────────────────────
results = statguard.execute_files(contract, "data/orders_*.parquet", workers=8)
failed  = [r for r in results if r.failed]

# streaming — react to each result as it completes
for result in statguard.execute_files_stream(contract, "data/**/*.parquet"):
    if not result.passed:
        print(f"FAIL {result.path}: {result.report.summary()}")

# ── GPU / cuDF ────────────────────────────────────────────────────────────────
import cudf
gdf    = cudf.read_parquet("s3://bucket/events.parquet")
report = statguard.execute_cudf(contract, gdf)

# ── Referential integrity ─────────────────────────────────────────────────────
violations = statguard.check_referential_integrity(
    orders, customers,
    foreign_key="customer_id", primary_key="id",
)
print(statguard.integrity_report(violations))

# check multiple FK pairs at once
violations = statguard.check_all_foreign_keys(
    orders, dims,
    key_pairs=[("customer_id", "id"), ("product_id", "sku")],
)
```

---

## Report output

```json
{
  "id": "a1b2c3d4-...",
  "dataset": "orders",
  "executed_at": "2026-06-15T10:00:00Z",
  "duration_ms": 2,
  "row_count": 500000,
  "passed": true,
  "health": {
    "score": 0.972,
    "grade": "A",
    "schema_score": 0.980,
    "drift_score": 0.950
  },
  "violations": [
    {
      "column": "amount",
      "check": "outlier_detection",
      "severity": "Error",
      "message": "14 outlier(s) in 'amount' (method=iqr)",
      "row_indices": [142, 891, 3204]
    }
  ],
  "drift_results": [
    {
      "column": "amount",
      "stat": "mean",
      "reference_value": 84.20,
      "current_value": 91.50,
      "drift": 0.087,
      "threshold": 0.15,
      "psi": 0.012,
      "ks_stat": 0.041,
      "passed": true
    }
  ],
  "column_profiles": [
    {
      "name": "amount",
      "mean": 91.5,
      "std": 142.3,
      "p95": 310.0,
      "null_rate": 0.0,
      "distinct_count": 184291
    }
  ]
}
```

---

## Use cases

| Use case | How |
|---|---|
| **dbt / Airflow pipeline gate** | `statguard validate --fail-on-warning` in task |
| **ML feature drift monitor** | `stats { feature.mean drift < 0.05 }` + reference dataset |
| **Lakehouse quality layer** | `execute_delta()` / `execute_iceberg()` on every write |
| **Kafka / streaming quality** | `execute_streaming()` with micro-batch window |
| **Prometheus scraping** | `--format prometheus` or `report.to_prometheus()` |
| **CI data contract tests** | `statguard check` for DSL lint, `validate` for data |
| **PII audit** | `scan_pii(df)` before writing to a data warehouse or sharing a dataset |
| **Schema change gate** | `assert_no_breaking_changes(today, yesterday)` in pipeline DAG |
| **Stakeholder report** | `to_html(report)` → email or attach to CI build artefacts |
| **Business logic validation** | `@blocking: assert amount > 0 when status == "paid"` in DSL |
| **Custom domain checks** | `@statguard.validator()` for business-specific rules or ML-based scoring |
| **Orphaned data detection** | `check_referential_integrity(orders, customers, "cust_id", "id")` |
| **GPU-accelerated QA** | `execute_cudf(contract, gdf)` for 100M+ row validation |
| **Batch validation at scale** | `execute_files(contract, "data/**/*.parquet", workers=16)` for parallel processing |

---

## Architecture

```
statguard/
├── crates/
│   ├── statguard-core/       DSL (pest PEG grammar) → AST → compiler → ExecutionDag
│   ├── statguard-engine/     Rayon parallel executor — batch + streaming
│   ├── statguard-validators/ Type, null, regex, range, enum, uniqueness checks
│   ├── statguard-stats/      PSI, KS test, HyperLogLog profiler, percentile stats
│   ├── statguard-io/         Universal reader — auto-detects all formats
│   │                         • Parquet, CSV, JSON, IPC, Avro (local + cloud)
│   │                         • Delta Lake (pure Rust transaction log replay)
│   │                         • Apache Iceberg (v1/v2 metadata parsing, no Spark)
│   │                         • S3, GCS, Azure (Polars lazy, opt-in features)
│   │                         • SQL: PostgreSQL, MySQL, SQLite (pure Rust via sqlx)
│   │                         • StreamingBatcher, RowBuffer
│   ├── statguard-metrics/    ValidationReport, health scores, Prometheus output
│   └── statguard-py/         PyO3 bindings — Rust layer public API
└── python/
    ├── statguard/
    │   ├── __init__.py           Re-exports from Rust + Python layers
    │   ├── _connectors.py        Cloud (S3/GCS/Azure), SQL (13 connectors), Spark
    │   ├── _cli.py               CLI: validate, check commands
    │   ├── _pii.py               PII detection (name heuristics + regex patterns)
    │   ├── _evolution.py         Schema evolution detection and gating
    │   ├── _html.py              Self-contained HTML report generation
    │   ├── _validators.py        Custom Python validator registry + runner
    │   ├── _parallel.py          Parallel multi-file validation with ThreadPoolExecutor
    │   ├── _gpu.py               RAPIDS cuDF adapter (Arrow C Stream interface)
    │   └── _integrity.py         Referential integrity checks (foreign-key validation)
    └── docs/
        └── FORMAT_COMPATIBILITY.md
```

### Execution pipeline

```
DSL text  →  pest parser  →  DataContract AST
                                   │
                              Compiler::compile()
                                   │
                           raw DagNode list
                                   │
                    Optimizer: dedup → fuse null checks → cost-sort
                                   │
                           ExecutionDag (column-grouped)
                                   │
              ┌────────────────────┼──────────────────────────┐
     SchemaValidator         Rayon parallel            DriftEngine
     RuleEngine              per-column nodes          (PSI + KS)
                                   │
                           Profiler (always-on)
                                   │
                         ValidationReport
```

### Why it's fast

- **Columnar execution** — every check operates on an entire Arrow column, never row by row
- **Compiled DAG** — validation logic is a fixed execution plan, not interpreted rules at runtime
- **Cost-ordered checks** — `null` (cost 1) runs before `regex` (cost 4) before `uniqueness` (cost 5); cheap failures abort expensive work early
- **Rayon parallelism** — columns execute concurrently, scaling with core count
- **Zero-copy IO** — Arrow IPC and Parquet data never leaves the Arrow memory model
- **HyperLogLog** — O(1) memory, ~0.8% error rate for cardinality estimation on every column

---

## Dependencies and licensing

Statguardian is MIT licensed. All core dependencies use MIT, Apache-2.0, or BSD licenses.

**Note on PostgreSQL support:** Using `execute_sql()` with PostgreSQL requires `psycopg2` (LGPL-2.1 with exceptions), which adds an LGPL component to your application. See [LICENSES.md](LICENSES.md) for full compliance details and impact on binary distributions.

All optional features use OSI-approved open-source licenses only. Proprietary drivers (Oracle, SQL Server ODBC) are intentionally excluded.

→ Full license matrix: [LICENSES.md](LICENSES.md)

---

## Roadmap

**Connectors**
- [ ] Kafka — streaming validation with micro-batch windows and watermarks
- [ ] Apache Flink — native DataStream and Table API integration
- [ ] Airflow operator — `StatGuardOperator` for pipeline-gate tasks
- [ ] dbt test macro — run StatGuard contracts as dbt tests after model runs
- [ ] GitHub Actions — `statguard-action` for contract validation in CI

**DSL and rules**
- [ ] Referential integrity in DSL — `foreign_key(customers.id)` enforced at execution time (Python-level `check_referential_integrity()` available now)
- [ ] Cross-dataset joins — validate consistency between two contracts in one run

**Output and observability**
- [ ] OpenTelemetry traces — emit spans per check for distributed tracing
- [ ] DataHub / OpenLineage lineage events on each validation run
- [ ] Webhook callbacks — POST violations to external systems (Slack, PagerDuty, …)

**Performance**
- [ ] Zero-copy GPU columnar path — avoid Arrow conversion for in-GPU validation
- [ ] Distributed validation — shard large datasets across workers with result merging
- [ ] Index-accelerated uniqueness — use Polars expr-level dedup for O(1) lookups

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md).

```bash
cargo test --workspace --exclude statguard
cargo clippy --workspace
cargo fmt --all
```

---

## License

MIT © 2026 Georgi Mammen Mullassery




