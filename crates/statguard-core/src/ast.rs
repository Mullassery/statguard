use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

// ── Data types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Int,
    Float,
    String,
    Bool,
    Date,
    Datetime,
    Bytes,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Int      => write!(f, "int"),
            DataType::Float    => write!(f, "float"),
            DataType::String   => write!(f, "string"),
            DataType::Bool     => write!(f, "bool"),
            DataType::Date     => write!(f, "date"),
            DataType::Datetime => write!(f, "datetime"),
            DataType::Bytes    => write!(f, "bytes"),
        }
    }
}

// ── Constraints ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Constraint {
    NotNull,
    Unique,
    Positive,
    Negative,
    PrimaryKey,
    Coerce,
    Regex { pattern: String },
    Between { min: f64, max: f64 },
    Min { value: f64 },
    Max { value: f64 },
    Len { min: usize, max: usize },
    Enum { values: Vec<String> },
    ForeignKey { table: String, column: String },
}

// ── Schema ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
}

// ── Shared literal value type (used in cross-column rules) ────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum LiteralValue {
    Number(f64),
    Str(String),
    Bool(bool),
}

impl std::fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LiteralValue::Number(n) => write!(f, "{n}"),
            LiteralValue::Str(s)   => write!(f, "\"{s}\""),
            LiteralValue::Bool(b)  => write!(f, "{b}"),
        }
    }
}

// ── Quality rules ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricFn {
    Completeness,
    Uniqueness,
    Validity,
    Consistency,
    Freshness,
    NonNullRate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOp {
    Gt,
    Lt,
    Gte,
    Lte,
    Eq,
    Neq,
}

impl ComparisonOp {
    pub fn evaluate(&self, lhs: f64, rhs: f64) -> bool {
        match self {
            ComparisonOp::Gt  => lhs > rhs,
            ComparisonOp::Lt  => lhs < rhs,
            ComparisonOp::Gte => lhs >= rhs,
            ComparisonOp::Lte => lhs <= rhs,
            ComparisonOp::Eq  => (lhs - rhs).abs() < f64::EPSILON,
            ComparisonOp::Neq => (lhs - rhs).abs() >= f64::EPSILON,
        }
    }

    pub fn evaluate_str(&self, lhs: &str, rhs: &str) -> bool {
        match self {
            ComparisonOp::Eq  => lhs == rhs,
            ComparisonOp::Neq => lhs != rhs,
            ComparisonOp::Lt  => lhs < rhs,
            ComparisonOp::Lte => lhs <= rhs,
            ComparisonOp::Gt  => lhs > rhs,
            ComparisonOp::Gte => lhs >= rhs,
        }
    }

    pub fn evaluate_bool(&self, lhs: bool, rhs: bool) -> bool {
        match self {
            ComparisonOp::Eq  => lhs == rhs,
            ComparisonOp::Neq => lhs != rhs,
            _ => false,
        }
    }
}

impl std::fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComparisonOp::Gt  => write!(f, ">"),
            ComparisonOp::Lt  => write!(f, "<"),
            ComparisonOp::Gte => write!(f, ">="),
            ComparisonOp::Lte => write!(f, "<="),
            ComparisonOp::Eq  => write!(f, "=="),
            ComparisonOp::Neq => write!(f, "!="),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
    Blocking,
}

impl Default for Severity {
    fn default() -> Self {
        Severity::Error
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityRule {
    pub metric: MetricFn,
    pub column: String,
    pub op: ComparisonOp,
    pub threshold: f64,
    pub severity: Severity,
}

/// Cross-column conditional assertion:
/// "assert <assert_column> <assert_op> <assert_value> when <when_column> <when_op> <when_value>"
///
/// Example DSL:
///   @blocking: assert amount > 0.0 when status == "paid"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossColumnRule {
    pub assert_column: String,
    pub assert_op: ComparisonOp,
    pub assert_value: LiteralValue,
    pub when_column: String,
    pub when_op: ComparisonOp,
    pub when_value: LiteralValue,
    pub severity: Severity,
}

// ── Stats / drift ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatFn {
    Mean,
    Std,
    Median,
    Min,
    Max,
    P05,
    P95,
    P99,
    P999,
}

impl std::fmt::Display for StatFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatFn::Mean   => write!(f, "mean"),
            StatFn::Std    => write!(f, "std"),
            StatFn::Median => write!(f, "median"),
            StatFn::Min    => write!(f, "min"),
            StatFn::Max    => write!(f, "max"),
            StatFn::P05    => write!(f, "p05"),
            StatFn::P95    => write!(f, "p95"),
            StatFn::P99    => write!(f, "p99"),
            StatFn::P999   => write!(f, "p999"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsRule {
    pub column: String,
    pub stat: StatFn,
    pub op: ComparisonOp,
    pub threshold: f64,
    pub severity: Severity,
}

// ── Anomaly detection ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyFn {
    DetectOutliers,
    DetectNulls,
    DetectDuplicates,
    DetectPatternBreaks,
    DetectCardinalityExplosion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyRule {
    pub function: AnomalyFn,
    pub column: String,
    pub args: IndexMap<String, String>,
    pub severity: Severity,
}

// ── Streaming config ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamConfig {
    pub window: Option<String>,
    pub watermark: Option<String>,
    pub emit: Option<String>,
}

// ── Top-level contract ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataContract {
    pub name: String,
    pub schema: Vec<FieldDef>,
    pub quality_rules: Vec<QualityRule>,
    pub cross_column_rules: Vec<CrossColumnRule>,
    pub stats_rules: Vec<StatsRule>,
    pub anomaly_rules: Vec<AnomalyRule>,
    pub stream_config: Option<StreamConfig>,
}

impl DataContract {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            schema: Vec::new(),
            quality_rules: Vec::new(),
            cross_column_rules: Vec::new(),
            stats_rules: Vec::new(),
            anomaly_rules: Vec::new(),
            stream_config: None,
        }
    }

    pub fn schema_map(&self) -> IndexMap<&str, &FieldDef> {
        self.schema.iter().map(|f| (f.name.as_str(), f)).collect()
    }

    pub fn is_streaming(&self) -> bool {
        self.stream_config.is_some()
    }
}
