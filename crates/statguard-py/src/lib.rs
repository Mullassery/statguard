use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_polars::PyDataFrame;

use statguard_core::{parse_and_compile, DataContract};
use statguard_core::compiler::dag::ExecutionDag;
use statguard_engine::Engine;
use statguard_metrics::report::ValidationReport;

// ── PyDataContract ─────────────────────────────────────────────────────────────

/// A compiled StatGuard data contract ready for execution.
///
/// Create with `DataContract.from_dsl(dsl_string)` or `DataContract.from_file(path)`.
#[pyclass(name = "DataContract", module = "statguard")]
pub struct PyDataContract {
    inner: DataContract,
    dag: ExecutionDag,
}

#[pymethods]
impl PyDataContract {
    /// Parse and compile a contract from DSL source text.
    #[staticmethod]
    fn from_dsl(dsl: &str) -> PyResult<Self> {
        let pairs = parse_and_compile(dsl)
            .map_err(|e| PyValueError::new_err(format!("DSL parse error: {e}")))?;
        let (contract, dag) = pairs
            .into_iter()
            .next()
            .ok_or_else(|| PyValueError::new_err("no datasets defined in DSL"))?;
        Ok(Self { inner: contract, dag })
    }

    /// Load contract DSL from a file path.
    #[staticmethod]
    fn from_file(path: &str) -> PyResult<Self> {
        let dsl = std::fs::read_to_string(path)
            .map_err(|e| PyRuntimeError::new_err(format!("cannot read file '{path}': {e}")))?;
        Self::from_dsl(&dsl)
    }

    /// The dataset name from the contract.
    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }

    /// Number of schema fields.
    #[getter]
    fn field_count(&self) -> usize {
        self.inner.schema.len()
    }

    /// Number of quality rules.
    #[getter]
    fn quality_rule_count(&self) -> usize {
        self.inner.quality_rules.len()
    }

    /// Number of compiled DAG nodes.
    #[getter]
    fn dag_node_count(&self) -> usize {
        self.dag.node_count()
    }

    fn __repr__(&self) -> String {
        format!(
            "DataContract(name='{}', fields={}, quality_rules={}, dag_nodes={})",
            self.inner.name,
            self.inner.schema.len(),
            self.inner.quality_rules.len(),
            self.dag.node_count(),
        )
    }
}

// ── PyValidationReport ─────────────────────────────────────────────────────────

/// Structured result of executing a contract against a dataset.
#[pyclass(name = "ValidationReport", module = "statguard")]
pub struct PyValidationReport {
    inner: ValidationReport,
}

#[pymethods]
impl PyValidationReport {
    /// Unique report ID (UUID4).
    #[getter]
    fn id(&self) -> &str {
        &self.inner.id
    }

    /// Dataset name.
    #[getter]
    fn dataset(&self) -> &str {
        &self.inner.dataset
    }

    /// Whether all blocking checks passed.
    #[getter]
    fn passed(&self) -> bool {
        self.inner.passed
    }

    /// Health score in [0, 1].
    #[getter]
    fn health_score(&self) -> f64 {
        self.inner.health.score
    }

    /// Letter grade: A/B/C/D/F.
    #[getter]
    fn grade(&self) -> &str {
        &self.inner.health.grade
    }

    /// Number of rows processed.
    #[getter]
    fn row_count(&self) -> usize {
        self.inner.row_count
    }

    /// Total violation count.
    #[getter]
    fn violation_count(&self) -> usize {
        self.inner.violations.len()
    }

    /// Execution time in milliseconds.
    #[getter]
    fn duration_ms(&self) -> u64 {
        self.inner.duration_ms
    }

    /// Full report as a JSON string.
    fn to_json(&self) -> String {
        self.inner.to_json()
    }

    /// Full report as a pretty-printed JSON string.
    fn to_json_pretty(&self) -> String {
        self.inner.to_json_pretty()
    }

    /// Prometheus text exposition format.
    fn to_prometheus(&self) -> String {
        self.inner.to_prometheus()
    }

    /// Violations as a list of dicts (column, check, message, severity).
    fn violations(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        self.inner
            .violations
            .iter()
            .map(|v| {
                let d = PyDict::new(py);
                d.set_item("column", &v.column)?;
                d.set_item("check", &v.check)?;
                d.set_item("message", &v.message)?;
                d.set_item("severity", format!("{:?}", v.severity))?;
                d.set_item("observed", v.observed)?;
                d.set_item("expected", v.expected)?;
                Ok(d.into())
            })
            .collect()
    }

    /// Drift results as a list of dicts.
    fn drift_results(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        self.inner
            .drift_results
            .iter()
            .map(|r| {
                let d = PyDict::new(py);
                d.set_item("column", &r.column)?;
                d.set_item("stat", &r.stat)?;
                d.set_item("reference_value", r.reference_value)?;
                d.set_item("current_value", r.current_value)?;
                d.set_item("drift", r.drift)?;
                d.set_item("threshold", r.threshold)?;
                d.set_item("passed", r.passed)?;
                d.set_item("psi", r.psi)?;
                d.set_item("ks_stat", r.ks_stat)?;
                Ok(d.into())
            })
            .collect()
    }

    /// Column profiles as a list of dicts.
    fn column_profiles(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        self.inner
            .column_profiles
            .iter()
            .map(|p| {
                let d = PyDict::new(py);
                d.set_item("name", &p.name)?;
                d.set_item("dtype", &p.dtype)?;
                d.set_item("row_count", p.row_count)?;
                d.set_item("null_count", p.null_count)?;
                d.set_item("null_rate", p.null_rate)?;
                d.set_item("distinct_count", p.distinct_count)?;
                d.set_item("min", p.min)?;
                d.set_item("max", p.max)?;
                d.set_item("mean", p.mean)?;
                d.set_item("std", p.std)?;
                d.set_item("median", p.median)?;
                d.set_item("p05", p.p05)?;
                d.set_item("p95", p.p95)?;
                d.set_item("p99", p.p99)?;
                Ok(d.into())
            })
            .collect()
    }

    fn summary(&self) -> String {
        self.inner.summary().to_string()
    }

    fn __repr__(&self) -> String {
        format!(
            "ValidationReport(passed={}, score={:.3}, grade='{}', violations={}, rows={})",
            self.inner.passed,
            self.inner.health.score,
            self.inner.health.grade,
            self.inner.violations.len(),
            self.inner.row_count,
        )
    }
}

// ── Module-level functions ────────────────────────────────────────────────────

/// Execute a contract against a Polars DataFrame.
///
/// Args:
///     contract: A compiled `DataContract` object.
///     df:       A Polars `DataFrame` to validate.
///     reference: Optional Polars `DataFrame` to use as drift reference.
///
/// Returns:
///     A `ValidationReport` object.
#[pyfunction]
#[pyo3(signature = (contract, df, reference=None))]
fn execute(
    contract: &PyDataContract,
    df: PyDataFrame,
    reference: Option<PyDataFrame>,
) -> PyResult<PyValidationReport> {
    let frame: polars::prelude::DataFrame = df.into();
    let ref_frame: Option<polars::prelude::DataFrame> = reference.map(|r| r.into());

    let engine = Engine::new(contract.inner.clone(), contract.dag.clone());
    let report = engine.execute(&frame, ref_frame.as_ref());

    Ok(PyValidationReport { inner: report })
}

/// Execute a contract against a file (auto-detects format: parquet, csv, json, ipc).
///
/// Args:
///     contract:        A compiled `DataContract`.
///     path:            Path to the data file.
///     reference_path:  Optional path to a reference data file for drift detection.
#[pyfunction]
#[pyo3(signature = (contract, path, reference_path=None))]
fn execute_file(
    contract: &PyDataContract,
    path: &str,
    reference_path: Option<&str>,
) -> PyResult<PyValidationReport> {
    let engine = Engine::new(contract.inner.clone(), contract.dag.clone());
    let report = engine
        .execute_file(path, reference_path)
        .map_err(|e| PyRuntimeError::new_err(format!("IO error: {e}")))?;
    Ok(PyValidationReport { inner: report })
}

/// Execute a contract in streaming mode, processing the file in chunks.
///
/// Args:
///     contract:   A compiled `DataContract`.
///     path:       Path to the data file.
///     batch_size: Number of rows per batch (default: 10000).
///
/// Returns:
///     A list of `ValidationReport` objects, one per batch.
#[pyfunction]
#[pyo3(signature = (contract, path, batch_size=10000))]
fn execute_streaming(
    contract: &PyDataContract,
    path: &str,
    batch_size: usize,
) -> PyResult<Vec<PyValidationReport>> {
    let engine = Engine::new(contract.inner.clone(), contract.dag.clone());
    let reports = engine
        .execute_streaming(path, batch_size)
        .map_err(|e| PyRuntimeError::new_err(format!("streaming error: {e}")))?;
    Ok(reports.into_iter().map(|r| PyValidationReport { inner: r }).collect())
}

// ── Delta Lake ────────────────────────────────────────────────────────────────

/// Execute a contract against a Delta Lake table directory.
///
/// Args:
///     contract:         A compiled `DataContract`.
///     table_path:       Path to the Delta table root directory (contains `_delta_log/`).
///     version:          Optional Delta version for time-travel (default: latest).
///     reference_path:   Optional Delta table path to use as drift reference.
///     reference_version: Optional version of the reference table.
#[pyfunction]
#[pyo3(signature = (contract, table_path, version=None, reference_path=None, reference_version=None))]
fn execute_delta(
    contract: &PyDataContract,
    table_path: &str,
    version: Option<u64>,
    reference_path: Option<&str>,
    reference_version: Option<u64>,
) -> PyResult<PyValidationReport> {
    let df = statguard_io::DeltaReader::read_version(table_path, version)
        .map_err(|e| PyRuntimeError::new_err(format!("Delta read error: {e}")))?;

    let ref_df = match (reference_path, reference_version) {
        (Some(rp), rv) => Some(
            statguard_io::DeltaReader::read_version(rp, rv)
                .map_err(|e| PyRuntimeError::new_err(format!("Delta reference read error: {e}")))?
        ),
        _ => None,
    };

    let engine = Engine::new(contract.inner.clone(), contract.dag.clone());
    let report = engine.execute(&df, ref_df.as_ref());
    Ok(PyValidationReport { inner: report })
}

/// Compare two Delta Lake versions for drift analysis.
///
/// Convenience wrapper around `execute_delta` that takes explicit version numbers
/// and sets up drift detection automatically.
///
/// Args:
///     contract:          A compiled `DataContract` (must have `stats` rules).
///     table_path:        Path to the Delta table root.
///     current_version:   The version to validate (default: latest).
///     reference_version: The version to compare against.
#[pyfunction]
#[pyo3(signature = (contract, table_path, reference_version, current_version=None))]
fn compare_delta_versions(
    contract: &PyDataContract,
    table_path: &str,
    reference_version: u64,
    current_version: Option<u64>,
) -> PyResult<PyValidationReport> {
    let (reference, current) =
        statguard_io::DeltaReader::read_two_versions(table_path, reference_version, current_version.unwrap_or(u64::MAX))
            .map_err(|e| PyRuntimeError::new_err(format!("Delta compare error: {e}")))?;

    let engine = Engine::new(contract.inner.clone(), contract.dag.clone());
    let report = engine.execute(&current, Some(&reference));
    Ok(PyValidationReport { inner: report })
}

// ── Apache Iceberg ────────────────────────────────────────────────────────────

/// Execute a contract against an Apache Iceberg table directory.
///
/// Args:
///     contract:            A compiled `DataContract`.
///     table_path:          Path to the Iceberg table root (contains `metadata/`).
///     snapshot_id:         Optional snapshot ID for time-travel.
///     reference_snapshot:  Optional snapshot ID to use as drift reference.
#[pyfunction]
#[pyo3(signature = (contract, table_path, snapshot_id=None, reference_snapshot=None))]
fn execute_iceberg(
    contract: &PyDataContract,
    table_path: &str,
    snapshot_id: Option<i64>,
    reference_snapshot: Option<i64>,
) -> PyResult<PyValidationReport> {
    let df = statguard_io::IcebergReader::read_snapshot(table_path, snapshot_id)
        .map_err(|e| PyRuntimeError::new_err(format!("Iceberg read error: {e}")))?;

    let ref_df = match reference_snapshot {
        Some(ref_id) => Some(
            statguard_io::IcebergReader::read_snapshot(table_path, Some(ref_id))
                .map_err(|e| PyRuntimeError::new_err(format!("Iceberg reference read error: {e}")))?
        ),
        None => None,
    };

    let engine = Engine::new(contract.inner.clone(), contract.dag.clone());
    let report = engine.execute(&df, ref_df.as_ref());
    Ok(PyValidationReport { inner: report })
}

/// List all snapshots of an Iceberg table.
///
/// Returns a list of dicts with keys: snapshot_id, timestamp_ms,
/// parent_snapshot_id, operation.
#[pyfunction]
fn list_iceberg_snapshots(table_path: &str, py: Python<'_>) -> PyResult<Vec<PyObject>> {
    let snapshots = statguard_io::IcebergReader::list_snapshots(table_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Iceberg error: {e}")))?;

    snapshots.iter().map(|s| {
        let d = PyDict::new(py);
        d.set_item("snapshot_id", s.snapshot_id)?;
        d.set_item("timestamp_ms", s.timestamp_ms)?;
        d.set_item("parent_snapshot_id", s.parent_snapshot_id)?;
        d.set_item("operation", s.operation.as_deref())?;
        Ok(d.into())
    }).collect()
}

/// Parse and validate DSL syntax without executing.
/// Returns the contract name if valid, raises ValueError on parse error.
#[pyfunction]
fn validate_dsl(dsl: &str) -> PyResult<String> {
    let pairs = parse_and_compile(dsl)
        .map_err(|e| PyValueError::new_err(format!("DSL error: {e}")))?;
    let name = pairs
        .first()
        .map(|(c, _)| c.name.clone())
        .unwrap_or_default();
    Ok(name)
}

// ── Module definition ─────────────────────────────────────────────────────────

#[pymodule]
fn statguard(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDataContract>()?;
    m.add_class::<PyValidationReport>()?;

    // Core execution
    m.add_function(wrap_pyfunction!(execute, m)?)?;
    m.add_function(wrap_pyfunction!(execute_file, m)?)?;
    m.add_function(wrap_pyfunction!(execute_streaming, m)?)?;

    // Delta Lake
    m.add_function(wrap_pyfunction!(execute_delta, m)?)?;
    m.add_function(wrap_pyfunction!(compare_delta_versions, m)?)?;

    // Apache Iceberg
    m.add_function(wrap_pyfunction!(execute_iceberg, m)?)?;
    m.add_function(wrap_pyfunction!(list_iceberg_snapshots, m)?)?;

    // Utilities
    m.add_function(wrap_pyfunction!(validate_dsl, m)?)?;

    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
