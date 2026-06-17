"""
RAPIDS cuDF adapter for StatGuard.

Validates GPU DataFrames by converting them to Polars via the Arrow
interchange protocol (zero-copy on same-device memory) before passing
to the Rust validation engine.

Requirements: cudf (NVIDIA RAPIDS ≥ 23.08) and polars.
"""

from __future__ import annotations

from typing import Optional


def execute_cudf(contract, cudf_df, reference_cudf_df=None):
    """
    Validate a RAPIDS cuDF DataFrame using StatGuard.

    The DataFrame is converted to Polars via the Arrow C Data Interface
    (zero-copy where CUDA unified memory or host-accessible memory is
    available) and then validated by the Rust engine.

    Args:
        contract:           DataContract
        cudf_df:            cuDF DataFrame to validate.
        reference_cudf_df:  Optional cuDF reference DataFrame for drift
                            detection.

    Returns:
        ValidationReport — identical to what ``statguard.execute()`` returns.

    Example::

        import cudf
        import statguard

        contract = statguard.DataContract.from_file("events.sg")
        gdf = cudf.read_parquet("s3://bucket/events.parquet")

        report = statguard.execute_cudf(contract, gdf)
        print(report.summary())
    """
    from . import execute

    df  = _cudf_to_polars(cudf_df)
    ref = _cudf_to_polars(reference_cudf_df) if reference_cudf_df is not None else None
    return execute(contract, df, reference=ref)


def _cudf_to_polars(cudf_df):
    """
    Convert a cuDF DataFrame to Polars.

    Tries the Arrow C Stream interface first (lowest overhead),
    falls back to host pandas conversion if unavailable.
    """
    import polars as pl

    # Preferred: Arrow C Stream interface — available in RAPIDS ≥ 23.08
    try:
        return pl.from_arrow(cudf_df.to_arrow())
    except AttributeError:
        pass

    # Fallback: go via pandas (copies data to host)
    try:
        return pl.from_pandas(cudf_df.to_pandas())
    except Exception as exc:
        raise RuntimeError(
            f"Could not convert cuDF DataFrame to Polars: {exc}. "
            "Ensure RAPIDS cuDF ≥ 23.08 is installed."
        ) from exc


def is_cudf_available() -> bool:
    """Return True if RAPIDS cuDF is installed and importable."""
    try:
        import cudf  # noqa: F401
        return True
    except ImportError:
        return False
