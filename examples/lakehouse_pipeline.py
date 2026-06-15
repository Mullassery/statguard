"""
StatGuard — Apache Iceberg + Delta Lake example
================================================

Demonstrates reading directly from lakehouse table formats,
comparing snapshots / versions for drift detection, and
validating data quality across the full lakehouse pipeline.

Prerequisites:
    pip install statguard polars deltalake pyiceberg
    # Build StatGuard from source:
    maturin develop --release
"""

import statguard

CONTRACT_DSL = """
dataset events {
    schema {
        event_id:   string, not_null, unique
        user_id:    string, not_null
        event_type: string, not_null, enum=["click","view","purchase","refund"]
        amount:     float,  min=0.0, max=1000000.0
        created_at: datetime, not_null
    }
    quality {
        @blocking: completeness(event_id)   > 0.9999
        completeness(user_id)               > 0.999
        @warning: uniqueness(event_id)      == 1.0
    }
    stats {
        amount.mean  drift < 0.15
        amount.p95   drift < 0.25
        amount.std   drift < 0.30
    }
    anomalies {
        detect_outliers(amount, method="iqr")
        @blocking: detect_duplicates(event_id)
    }
}
"""

contract = statguard.DataContract.from_dsl(CONTRACT_DSL)

# ── Delta Lake ────────────────────────────────────────────────────────────────

print("=== Delta Lake ===")

# Validate the latest Delta snapshot
report = statguard.execute_delta(contract, "/data/events_delta/")
print(report.summary())

# Time-travel: validate a specific version
report_v5 = statguard.execute_delta(contract, "/data/events_delta/", version=5)
print(f"Version 5 health: {report_v5.health_score:.3f}")

# Compare version 4 (reference) → version 10 (current) for drift
drift_report = statguard.compare_delta_versions(
    contract,
    table_path="/data/events_delta/",
    reference_version=4,
    current_version=10,
)
print(f"\nDelta drift analysis (v4 → v10):")
for d in drift_report.drift_results():
    status = "✓" if d["passed"] else "⚠ DRIFT"
    print(f"  {status}  {d['column']}.{d['stat']}: "
          f"drift={d['drift']:.4f}  PSI={d.get('psi',0):.4f}  KS={d.get('ks_stat',0):.4f}")

# ── Apache Iceberg ────────────────────────────────────────────────────────────

print("\n=== Apache Iceberg ===")

# List snapshots
snapshots = statguard.list_iceberg_snapshots("/data/events_iceberg/")
print(f"Found {len(snapshots)} snapshots:")
for s in snapshots[:3]:
    from datetime import datetime, timezone
    ts = datetime.fromtimestamp(s["timestamp_ms"] / 1000, tz=timezone.utc)
    print(f"  snapshot_id={s['snapshot_id']}  ts={ts.isoformat()}  op={s['operation']}")

# Validate the current Iceberg snapshot
report_ice = statguard.execute_iceberg(contract, "/data/events_iceberg/")
print(f"\nCurrent snapshot: {report_ice.summary()}")

# Time-travel: compare two snapshots for drift
if len(snapshots) >= 2:
    ref_id = snapshots[-2]["snapshot_id"]  # second-to-latest
    cur_id = snapshots[-1]["snapshot_id"]  # latest

    drift_ice = statguard.execute_iceberg(
        contract,
        "/data/events_iceberg/",
        snapshot_id=cur_id,
        reference_snapshot=ref_id,
    )
    print(f"\nIceberg drift ({ref_id} → {cur_id}):")
    for d in drift_ice.drift_results():
        status = "✓" if d["passed"] else "⚠ DRIFT"
        print(f"  {status}  {d['column']}.{d['stat']}: drift={d['drift']:.4f}")

# ── Avro / ORC files ──────────────────────────────────────────────────────────

print("\n=== Avro / ORC ===")

# These use the same API — format is auto-detected from the extension
avro_report = statguard.execute_file(contract, "/data/events.avro")
print(f"Avro: {avro_report.summary()}")

# ORC requires `--features orc` when building StatGuard
orc_report = statguard.execute_file(contract, "/data/events.orc")
print(f"ORC:  {orc_report.summary()}")

# ── CI/CD integration ─────────────────────────────────────────────────────────

import sys

# Exit with code 1 if any blocking violations found (safe for CI pipelines)
if not report.passed:
    print("\n❌ Blocking violations found — failing pipeline", file=sys.stderr)
    sys.exit(1)

print("\n✅ All checks passed")
