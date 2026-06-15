"""
StatGuard — High-performance Data Quality & Drift Monitoring Engine
===================================================================

A Rust-native engine with a Python-first API for:
- Schema validation (Pandera-like)
- Data expectations & rules (Great Expectations-like)
- Statistical drift detection (Evidently AI / WhyLogs-like)
- Anomaly detection

Quick start::

    import polars as pl
    import statguard

    contract = statguard.DataContract.from_dsl(\"\"\"
    dataset users {
        schema {
            id: int, not_null, unique
            email: string, regex="^[^@]+@[^@]+\\\\.[^@]+$"
            age: int, between(0, 120)
        }
        quality {
            completeness(id) > 0.99
        }
        stats {
            age.mean drift < 0.1
        }
    }
    \"\"\")

    df = pl.read_csv("users.csv")
    report = statguard.execute(contract, df)
    print(report.summary())
    print(f"Health score: {report.health_score:.2f} ({report.grade})")
"""

from ._statguard import (
    DataContract,
    ValidationReport,
    # Core execution
    execute,
    execute_file,
    execute_streaming,
    # Delta Lake
    execute_delta,
    compare_delta_versions,
    # Apache Iceberg
    execute_iceberg,
    list_iceberg_snapshots,
    # Utilities
    validate_dsl,
    __version__,
)

__all__ = [
    "DataContract",
    "ValidationReport",
    "execute",
    "execute_file",
    "execute_streaming",
    "execute_delta",
    "compare_delta_versions",
    "execute_iceberg",
    "list_iceberg_snapshots",
    "validate_dsl",
    "__version__",
]
