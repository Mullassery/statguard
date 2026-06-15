/// Delta Lake table format reader.
///
/// Reads the Delta transaction log to determine the current set of active
/// Parquet data files, then streams them through Polars for zero-copy
/// columnar processing.
///
/// # Time-travel
/// Pass `version = Some(n)` to read a specific Delta version.
/// Pass `as_of_timestamp = Some(ms)` to restore the table as of a Unix-epoch
/// timestamp in milliseconds.

use polars::prelude::*;
use serde::Deserialize;
use std::path::Path;
use crate::{DataReader, IoError, IoResult};

// ── Delta transaction log types ───────────────────────────────────────────────

/// An `add` action from a Delta commit JSON file.
#[derive(Debug, Deserialize)]
struct DeltaAdd {
    path: String,
    #[allow(dead_code)]
    #[serde(rename = "modificationTime")]
    modification_time: Option<i64>,
}

/// A `remove` action from a Delta commit JSON file.
#[derive(Debug, Deserialize)]
struct DeltaRemove {
    path: String,
}

/// One line of a Delta commit JSON file — each line is a JSON object with
/// at most one of these variants populated.
#[derive(Debug, Deserialize, Default)]
struct DeltaAction {
    add:    Option<DeltaAdd>,
    remove: Option<DeltaRemove>,
}

// ── DeltaReader ───────────────────────────────────────────────────────────────

/// Reads Apache Delta Lake tables into Polars DataFrames.
pub struct DeltaReader;

impl DeltaReader {
    /// Read the latest snapshot of a Delta table at `table_path`.
    pub fn read(table_path: &str) -> IoResult<DataFrame> {
        Self::read_version(table_path, None)
    }

    /// Read a specific Delta version (time-travel).
    pub fn read_version(table_path: &str, version: Option<u64>) -> IoResult<DataFrame> {
        let log_dir = Path::new(table_path).join("_delta_log");
        if !log_dir.exists() {
            return Err(IoError::ReadError {
                path: table_path.to_string(),
                msg: "no _delta_log directory found — not a Delta table".into(),
            });
        }

        let active_files = Self::resolve_active_files(&log_dir, table_path, version)?;

        if active_files.is_empty() {
            return Err(IoError::ReadError {
                path: table_path.to_string(),
                msg: "Delta table has no active data files".into(),
            });
        }

        read_and_concat(&active_files)
    }

    /// Read the Delta table as-of a Unix-epoch timestamp (ms).
    ///
    /// Scans commit files in order and stops at the first version whose
    /// timestamp exceeds the requested time, using the previous version.
    pub fn read_as_of_timestamp(table_path: &str, timestamp_ms: i64) -> IoResult<DataFrame> {
        let log_dir = Path::new(table_path).join("_delta_log");
        if !log_dir.exists() {
            return Err(IoError::ReadError {
                path: table_path.to_string(),
                msg: "no _delta_log directory found — not a Delta table".into(),
            });
        }

        // Find the latest version whose commit timestamp ≤ requested timestamp
        let version = Self::find_version_for_timestamp(&log_dir, timestamp_ms)?;
        Self::read_version(table_path, Some(version))
    }

    /// Compare two Delta snapshots for drift analysis.
    /// Returns (reference_df, current_df) — useful with `DriftEngine::evaluate`.
    pub fn read_two_versions(
        table_path: &str,
        reference_version: u64,
        current_version: u64,
    ) -> IoResult<(DataFrame, DataFrame)> {
        let reference = Self::read_version(table_path, Some(reference_version))?;
        let current   = Self::read_version(table_path, Some(current_version))?;
        Ok((reference, current))
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    fn resolve_active_files(
        log_dir: &Path,
        table_root: &str,
        up_to_version: Option<u64>,
    ) -> IoResult<Vec<String>> {
        let mut commit_files: Vec<(u64, std::path::PathBuf)> = std::fs::read_dir(log_dir)
            .map_err(|e| IoError::ReadError {
                path: log_dir.display().to_string(),
                msg: e.to_string(),
            })?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name();
                let s = name.to_string_lossy();
                if s.ends_with(".json") {
                    let ver: u64 = s.trim_end_matches(".json").parse().ok()?;
                    Some((ver, e.path()))
                } else {
                    None
                }
            })
            .collect();

        commit_files.sort_by_key(|(v, _)| *v);

        let limit = up_to_version.unwrap_or(u64::MAX);
        commit_files.retain(|(v, _)| *v <= limit);

        // Replay transaction log: accumulate adds, remove on remove
        let mut active: std::collections::HashMap<String, String> = std::collections::HashMap::new();

        for (_, path) in &commit_files {
            let content = std::fs::read_to_string(path).map_err(|e| IoError::ReadError {
                path: path.display().to_string(),
                msg: e.to_string(),
            })?;

            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() { continue; }

                let action: DeltaAction = serde_json::from_str(line).unwrap_or_default();

                if let Some(add) = action.add {
                    let full = format!("{}/{}", table_root.trim_end_matches('/'),
                        url_decode(&add.path));
                    active.insert(add.path, full);
                }
                if let Some(remove) = action.remove {
                    active.remove(&remove.path);
                }
            }
        }

        Ok(active.into_values().collect())
    }

    fn find_version_for_timestamp(log_dir: &Path, timestamp_ms: i64) -> IoResult<u64> {
        let mut commit_files: Vec<(u64, std::path::PathBuf)> = std::fs::read_dir(log_dir)
            .map_err(|e| IoError::Io(std::io::Error::other(e.to_string())))?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name();
                let s = name.to_string_lossy();
                if s.ends_with(".json") {
                    let ver: u64 = s.trim_end_matches(".json").parse().ok()?;
                    Some((ver, e.path()))
                } else {
                    None
                }
            })
            .collect();

        commit_files.sort_by_key(|(v, _)| *v);

        let mut best_version = 0u64;
        for (ver, path) in &commit_files {
            // The commitInfo action contains a `timestamp` field
            let content = std::fs::read_to_string(path)
                .unwrap_or_default();
            let commit_ts: Option<i64> = content
                .lines()
                .find_map(|line| {
                    let v: serde_json::Value = serde_json::from_str(line).ok()?;
                    v.get("commitInfo")?.get("timestamp")?.as_i64()
                });

            match commit_ts {
                Some(ts) if ts <= timestamp_ms => best_version = *ver,
                Some(_) => break, // passed the requested time
                None => best_version = *ver, // no timestamp, include it
            }
        }

        Ok(best_version)
    }
}

fn url_decode(s: &str) -> std::borrow::Cow<str> {
    if s.contains('%') {
        // Minimal percent-decoding for common cases
        std::borrow::Cow::Owned(s.replace("%20", " ").replace("%3D", "=").replace("%2F", "/"))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

fn read_and_concat(paths: &[String]) -> IoResult<DataFrame> {
    let dfs: Vec<DataFrame> = paths
        .iter()
        .map(|p| DataReader::read_parquet(p))
        .collect::<Result<Vec<_>, _>>()?;

    vstack_all(dfs)
}

fn vstack_all(dfs: Vec<DataFrame>) -> IoResult<DataFrame> {
    let mut iter = dfs.into_iter();
    let first = match iter.next() {
        Some(df) => df,
        None => return Ok(DataFrame::default()),
    };
    iter.try_fold(first, |mut acc, df| {
        acc.vstack_mut(&df).map_err(IoError::Polars)?;
        Ok(acc)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_delta_add(log_dir: &Path, version: u64, parquet_path: &str) {
        let content = format!(
            r#"{{"add":{{"path":"{parquet_path}","size":100,"modificationTime":1000,"dataChange":true,"stats":""}}}}"#
        );
        fs::write(log_dir.join(format!("{version:020}.json")), content).unwrap();
    }

    #[test]
    fn test_resolve_active_files_add() {
        let dir = tempdir();
        let log = dir.path().join("_delta_log");
        fs::create_dir_all(&log).unwrap();
        write_delta_add(&log, 0, "part-0.parquet");
        write_delta_add(&log, 1, "part-1.parquet");

        let files = DeltaReader::resolve_active_files(&log, dir.path().to_str().unwrap(), None).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_resolve_respects_version_limit() {
        let dir = tempdir();
        let log = dir.path().join("_delta_log");
        fs::create_dir_all(&log).unwrap();
        write_delta_add(&log, 0, "part-0.parquet");
        write_delta_add(&log, 1, "part-1.parquet");

        let files = DeltaReader::resolve_active_files(&log, dir.path().to_str().unwrap(), Some(0)).unwrap();
        assert_eq!(files.len(), 1);
    }

    fn tempdir() -> tempfile::TempDir {
        tempfile::TempDir::new().unwrap()
    }
}
