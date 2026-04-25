//! Hand-rolled Prometheus-compatible metrics.
//!
//! No external `metrics` ecosystem — just atomic counters and a text renderer.
//! Exposed at `GET /metrics` on the web port.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Server metrics. Cheap to clone (`Arc` of atomic counters).
#[derive(Clone, Default)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

#[derive(Default)]
struct MetricsInner {
    /// Queries classified by kind.
    queries_select: AtomicU64,
    queries_insert: AtomicU64,
    queries_update: AtomicU64,
    queries_delete: AtomicU64,
    queries_ddl: AtomicU64,
    queries_other: AtomicU64,
    /// Failed queries (any kind).
    query_errors: AtomicU64,
    /// Currently active pgwire connections.
    active_connections: AtomicU64,
    /// Total accepted pgwire connections since startup.
    total_connections: AtomicU64,
    /// Connections rejected because the connection limit was exceeded.
    rejected_connections: AtomicU64,
    /// Total transactions committed.
    txns_committed: AtomicU64,
    /// Total transactions aborted.
    txns_aborted: AtomicU64,
    /// Checkpoints written (both manual and auto).
    checkpoints: AtomicU64,
}

/// Query kind classification based on leading keyword.
#[derive(Debug, Clone, Copy)]
pub enum QueryKind {
    Select,
    Insert,
    Update,
    Delete,
    Ddl,
    Other,
}

impl QueryKind {
    pub fn from_sql(sql: &str) -> Self {
        let trimmed = sql.trim_start();
        // Skip leading `--` line comments when classifying.
        let rest = match trimmed.strip_prefix("--") {
            Some(after) => after.splitn(2, '\n').nth(1).unwrap_or(""),
            None => trimmed,
        };
        let head = rest
            .trim_start()
            .chars()
            .take(20)
            .collect::<String>()
            .to_uppercase();
        if head.starts_with("SELECT") || head.starts_with("WITH") {
            QueryKind::Select
        } else if head.starts_with("INSERT") {
            QueryKind::Insert
        } else if head.starts_with("UPDATE") {
            QueryKind::Update
        } else if head.starts_with("DELETE") {
            QueryKind::Delete
        } else if head.starts_with("CREATE")
            || head.starts_with("DROP")
            || head.starts_with("ALTER")
        {
            QueryKind::Ddl
        } else {
            QueryKind::Other
        }
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_query(&self, kind: QueryKind) {
        let c = match kind {
            QueryKind::Select => &self.inner.queries_select,
            QueryKind::Insert => &self.inner.queries_insert,
            QueryKind::Update => &self.inner.queries_update,
            QueryKind::Delete => &self.inner.queries_delete,
            QueryKind::Ddl => &self.inner.queries_ddl,
            QueryKind::Other => &self.inner.queries_other,
        };
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query_error(&self) {
        self.inner.query_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_opened(&self) {
        self.inner
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
        self.inner.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_closed(&self) {
        self.inner
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }

    pub fn connection_rejected(&self) {
        self.inner
            .rejected_connections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_txn_commit(&self) {
        self.inner.txns_committed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_txn_abort(&self) {
        self.inner.txns_aborted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_checkpoint(&self) {
        self.inner.checkpoints.fetch_add(1, Ordering::Relaxed);
    }

    /// Overwrite the checkpoint counter. Used to pull the source-of-truth
    /// value from the engine on each /metrics scrape so auto-checkpoints
    /// fired inside commit() are reflected.
    pub fn set_checkpoint_count(&self, v: u64) {
        self.inner.checkpoints.store(v, Ordering::Relaxed);
    }

    pub fn active_connections(&self) -> u64 {
        self.inner.active_connections.load(Ordering::Relaxed)
    }

    /// Render counters in Prometheus text format.
    /// Exposes one gauge (`rustdb_active_connections`) and counters otherwise.
    pub fn render_prometheus(&self) -> String {
        let i = &self.inner;
        let mut out = String::new();

        push_counter(
            &mut out,
            "rustdb_queries_total",
            "Total queries executed, labeled by kind.",
            &[
                ("select", i.queries_select.load(Ordering::Relaxed)),
                ("insert", i.queries_insert.load(Ordering::Relaxed)),
                ("update", i.queries_update.load(Ordering::Relaxed)),
                ("delete", i.queries_delete.load(Ordering::Relaxed)),
                ("ddl", i.queries_ddl.load(Ordering::Relaxed)),
                ("other", i.queries_other.load(Ordering::Relaxed)),
            ],
            "kind",
        );

        push_scalar_counter(
            &mut out,
            "rustdb_query_errors_total",
            "Total queries that returned an error.",
            i.query_errors.load(Ordering::Relaxed),
        );

        push_scalar_counter(
            &mut out,
            "rustdb_connections_total",
            "Total pgwire connections accepted since startup.",
            i.total_connections.load(Ordering::Relaxed),
        );

        push_scalar_counter(
            &mut out,
            "rustdb_connections_rejected_total",
            "Total pgwire connections rejected due to the connection limit.",
            i.rejected_connections.load(Ordering::Relaxed),
        );

        push_scalar_gauge(
            &mut out,
            "rustdb_active_connections",
            "Currently-open pgwire connections.",
            i.active_connections.load(Ordering::Relaxed),
        );

        push_scalar_counter(
            &mut out,
            "rustdb_txns_committed_total",
            "Total transactions committed.",
            i.txns_committed.load(Ordering::Relaxed),
        );

        push_scalar_counter(
            &mut out,
            "rustdb_txns_aborted_total",
            "Total transactions aborted.",
            i.txns_aborted.load(Ordering::Relaxed),
        );

        push_scalar_counter(
            &mut out,
            "rustdb_checkpoints_total",
            "Total checkpoints written (manual + auto).",
            i.checkpoints.load(Ordering::Relaxed),
        );

        out
    }
}

fn push_scalar_counter(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!("# HELP {} {}\n", name, help));
    out.push_str(&format!("# TYPE {} counter\n", name));
    out.push_str(&format!("{} {}\n", name, value));
}

fn push_scalar_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!("# HELP {} {}\n", name, help));
    out.push_str(&format!("# TYPE {} gauge\n", name));
    out.push_str(&format!("{} {}\n", name, value));
}

fn push_counter(
    out: &mut String,
    name: &str,
    help: &str,
    labels: &[(&str, u64)],
    label_name: &str,
) {
    out.push_str(&format!("# HELP {} {}\n", name, help));
    out.push_str(&format!("# TYPE {} counter\n", name));
    for (label_value, v) in labels {
        out.push_str(&format!(
            "{}{{{}=\"{}\"}} {}\n",
            name, label_name, label_value, v
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_kind_classification() {
        assert!(matches!(QueryKind::from_sql("SELECT 1"), QueryKind::Select));
        assert!(matches!(
            QueryKind::from_sql("  select * from t"),
            QueryKind::Select
        ));
        assert!(matches!(
            QueryKind::from_sql("INSERT INTO t VALUES (1)"),
            QueryKind::Insert
        ));
        assert!(matches!(
            QueryKind::from_sql("UPDATE t SET x=1"),
            QueryKind::Update
        ));
        assert!(matches!(
            QueryKind::from_sql("DELETE FROM t"),
            QueryKind::Delete
        ));
        assert!(matches!(
            QueryKind::from_sql("CREATE TABLE t(id BIGINT)"),
            QueryKind::Ddl
        ));
        assert!(matches!(
            QueryKind::from_sql("DROP TABLE t"),
            QueryKind::Ddl
        ));
        assert!(matches!(QueryKind::from_sql("BEGIN"), QueryKind::Other));
        assert!(matches!(
            QueryKind::from_sql("-- comment\nSELECT 1"),
            QueryKind::Select
        ));
    }

    #[test]
    fn counters_increment() {
        let m = Metrics::new();
        m.record_query(QueryKind::Select);
        m.record_query(QueryKind::Select);
        m.record_query(QueryKind::Insert);
        m.record_query_error();
        m.connection_opened();
        m.connection_opened();
        m.connection_closed();
        m.record_checkpoint();

        let text = m.render_prometheus();
        assert!(text.contains("rustdb_queries_total{kind=\"select\"} 2"));
        assert!(text.contains("rustdb_queries_total{kind=\"insert\"} 1"));
        assert!(text.contains("rustdb_query_errors_total 1"));
        assert!(text.contains("rustdb_connections_total 2"));
        assert!(text.contains("rustdb_active_connections 1"));
        assert!(text.contains("rustdb_checkpoints_total 1"));
    }

    #[test]
    fn prometheus_format_includes_help_and_type() {
        let m = Metrics::new();
        let text = m.render_prometheus();
        assert!(text.contains("# HELP rustdb_queries_total"));
        assert!(text.contains("# TYPE rustdb_queries_total counter"));
        assert!(text.contains("# TYPE rustdb_active_connections gauge"));
    }
}
