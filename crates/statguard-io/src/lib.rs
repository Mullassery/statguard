pub mod delta;
pub mod iceberg;

pub use delta::DeltaReader;
pub use iceberg::{IcebergReader, IcebergDataFile, SnapshotInfo};

use polars::prelude::*;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IoError {
    #[error("IO error reading '{path}': {msg}")]
    ReadError { path: String, msg: String },

    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error(transparent)]
    Polars(#[from] PolarsError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type IoResult<T> = Result<T, IoError>;

fn open(path: &str) -> IoResult<std::fs::File> {
    std::fs::File::open(path).map_err(|e| IoError::ReadError {
        path: path.to_string(),
        msg: e.to_string(),
    })
}

/// Unified data reader — auto-detects format from file extension.
pub struct DataReader;

impl DataReader {
    pub fn read_file(path: &str) -> IoResult<DataFrame> {
        let p = Path::new(path);

        // Directory-based formats (Delta, Iceberg) — detect before extension check
        if p.is_dir() {
            if p.join("_delta_log").exists() {
                return DeltaReader::read(path);
            }
            if p.join("metadata").exists() {
                return IcebergReader::read(path);
            }
        }

        match p.extension().and_then(|e| e.to_str()) {
            Some("parquet")                  => Self::read_parquet(path),
            Some("csv") | Some("tsv")        => Self::read_csv(path),
            Some("json") | Some("ndjson")    => Self::read_json(path),
            Some("ipc") | Some("arrow")      => Self::read_ipc(path),
            Some("avro")                     => Self::read_avro(path),
            Some("orc")                      => Self::read_orc(path),
            Some(ext) => Err(IoError::UnsupportedFormat(ext.to_string())),
            None      => Err(IoError::UnsupportedFormat("(no extension)".into())),
        }
    }

    /// Explicitly read a Delta Lake table directory.
    pub fn read_delta(path: &str) -> IoResult<DataFrame> {
        DeltaReader::read(path)
    }

    /// Explicitly read an Apache Iceberg table directory.
    pub fn read_iceberg(path: &str) -> IoResult<DataFrame> {
        IcebergReader::read(path)
    }

    pub fn read_parquet(path: &str) -> IoResult<DataFrame> {
        let file = open(path)?;
        ParquetReader::new(file).finish().map_err(IoError::Polars)
    }

    pub fn read_csv(path: &str) -> IoResult<DataFrame> {
        CsvReadOptions::default()
            .with_infer_schema_length(Some(1000))
            .try_into_reader_with_file_path(Some(path.into()))
            .map_err(IoError::Polars)?
            .finish()
            .map_err(IoError::Polars)
    }

    pub fn read_json(path: &str) -> IoResult<DataFrame> {
        let file = open(path)?;
        JsonReader::new(file).finish().map_err(IoError::Polars)
    }

    pub fn read_ipc(path: &str) -> IoResult<DataFrame> {
        let file = open(path)?;
        IpcReader::new(file).finish().map_err(IoError::Polars)
    }

    /// Read an Apache Avro file.
    pub fn read_avro(path: &str) -> IoResult<DataFrame> {
        let file = open(path)?;
        polars::io::avro::AvroReader::new(file).finish().map_err(IoError::Polars)
    }

    /// Read an Apache ORC file.
    ///
    /// Requires the `orc` Polars feature. Falls back to an informative error
    /// if the file cannot be read.
    pub fn read_orc(path: &str) -> IoResult<DataFrame> {
        // ORC support in Polars is available via the `orc` feature.
        // We attempt a dynamic read; if the feature isn't compiled in, Polars
        // returns an error which we surface here.
        let _ = path; // suppress unused warning when orc feature is absent
        #[cfg(feature = "orc")]
        {
            let file = open(path)?;
            return polars::io::orc::OrcReader::new(file)
                .finish()
                .map_err(IoError::Polars);
        }
        #[allow(unreachable_code)]
        Err(IoError::UnsupportedFormat(
            "ORC: recompile with `--features orc` to enable ORC support".into(),
        ))
    }

    pub fn from_json_bytes(bytes: &[u8]) -> IoResult<DataFrame> {
        let cursor = std::io::Cursor::new(bytes);
        JsonReader::new(cursor).finish().map_err(IoError::Polars)
    }
}

/// Streaming-friendly record batcher — yields DataFrames of `batch_size` rows.
pub struct StreamingBatcher {
    pub path: String,
    pub batch_size: usize,
    offset: usize,
    total_rows: Option<usize>,
}

impl StreamingBatcher {
    pub fn new(path: impl Into<String>, batch_size: usize) -> Self {
        Self { path: path.into(), batch_size, offset: 0, total_rows: None }
    }

    pub fn next_batch(&mut self) -> IoResult<Option<DataFrame>> {
        let df = DataReader::read_file(&self.path)?;
        let n = df.height();
        self.total_rows = Some(n);

        if self.offset >= n {
            return Ok(None);
        }
        let end   = (self.offset + self.batch_size).min(n);
        let batch = df.slice(self.offset as i64, end - self.offset);
        self.offset = end;
        Ok(Some(batch))
    }

    pub fn is_exhausted(&self) -> bool {
        self.total_rows.map(|n| self.offset >= n).unwrap_or(false)
    }

    pub fn reset(&mut self) {
        self.offset = 0;
    }
}

/// In-memory micro-batch buffer for streaming event pipelines.
pub type StreamRow = std::collections::HashMap<String, String>;

pub struct RowBuffer {
    window_size: usize,
    buffer: Vec<StreamRow>,
    schema: Option<Vec<String>>,
}

impl RowBuffer {
    pub fn new(window_size: usize) -> Self {
        Self { window_size, buffer: Vec::new(), schema: None }
    }

    pub fn push(&mut self, row: StreamRow) -> IoResult<Option<DataFrame>> {
        if self.schema.is_none() {
            let mut keys: Vec<String> = row.keys().cloned().collect();
            keys.sort();
            self.schema = Some(keys);
        }
        self.buffer.push(row);
        if self.buffer.len() >= self.window_size {
            Ok(Some(self.flush()?))
        } else {
            Ok(None)
        }
    }

    pub fn flush(&mut self) -> IoResult<DataFrame> {
        let schema = self.schema.as_ref().cloned().unwrap_or_default();
        let rows   = std::mem::take(&mut self.buffer);

        let columns: Vec<Column> = schema
            .iter()
            .map(|col_name| {
                let vals: Vec<Option<String>> =
                    rows.iter().map(|r| r.get(col_name).cloned()).collect();
                let s = Series::new(col_name.as_str().into(), vals);
                s.into_column()
            })
            .collect();

        DataFrame::new(columns).map_err(IoError::Polars)
    }

    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }
}
