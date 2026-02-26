use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType};
use prometheus::{Encoder, Registry, TextEncoder};

use crate::models::MetricsResponse;

const MAX_EXACT_INTEGER_IN_F64: u64 = 1_u64 << 53;
static PRECISION_LOSS_WARNED: AtomicBool = AtomicBool::new(false);

#[derive(Debug)]
pub(crate) enum PrometheusRenderError {
    Encode(prometheus::Error),
}

impl fmt::Display for PrometheusRenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(error) => write!(f, "failed to encode prometheus payload: {error}"),
        }
    }
}

impl std::error::Error for PrometheusRenderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
        }
    }
}

#[derive(Clone)]
struct SnapshotCollector {
    descs: Arc<Vec<Desc>>,
    families: Arc<Vec<FamilyDef>>,
    snapshot: Arc<ArcSwap<MetricsResponse>>,
}

impl SnapshotCollector {
    fn new() -> Result<Self, prometheus::Error> {
        let families = build_families()?;
        let descs = families.iter().map(|family| family.desc.clone()).collect();

        Ok(Self {
            descs: Arc::new(descs),
            families: Arc::new(families),
            snapshot: Arc::new(ArcSwap::from_pointee(MetricsResponse::default())),
        })
    }

    fn update(&self, metrics: Arc<MetricsResponse>) {
        self.snapshot.store(metrics);
    }

    fn snapshot(&self) -> Arc<MetricsResponse> {
        self.snapshot.load_full()
    }
}

impl Collector for SnapshotCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let metrics = self.snapshot();
        let mut families = Vec::with_capacity(self.families.len());

        for family in self.families.iter() {
            families.push(family.collect(&metrics));
        }

        families
    }
}

pub(crate) struct PrometheusExporter {
    registry: Registry,
    collector: SnapshotCollector,
}

impl PrometheusExporter {
    pub(crate) fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let collector = SnapshotCollector::new()?;
        registry.register(Box::new(collector.clone()))?;

        Ok(Self {
            registry,
            collector,
        })
    }

    pub(crate) fn render(
        &self,
        metrics: MetricsResponse,
    ) -> Result<Vec<u8>, PrometheusRenderError> {
        self.collector.update(Arc::new(metrics));

        let families = self.registry.gather();
        let mut encoded = Vec::new();
        TextEncoder::new()
            .encode(&families, &mut encoded)
            .map_err(PrometheusRenderError::Encode)?;

        Ok(encoded)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetricKind {
    Counter,
    Gauge,
}

impl MetricKind {
    const fn metric_type(self) -> MetricType {
        match self {
            Self::Counter => MetricType::COUNTER,
            Self::Gauge => MetricType::GAUGE,
        }
    }
}

struct MetricSpec {
    kind: MetricKind,
    name: &'static str,
    help: &'static str,
    label_names: &'static [&'static str],
    labels: &'static [(&'static str, &'static str)],
    read: fn(&MetricsResponse) -> f64,
}

#[derive(Clone)]
struct SampleDef {
    labels: Vec<LabelPair>,
    read: fn(&MetricsResponse) -> f64,
}

#[derive(Clone)]
struct FamilyDef {
    kind: MetricKind,
    name: &'static str,
    help: &'static str,
    desc: Desc,
    samples: Vec<SampleDef>,
}

impl FamilyDef {
    fn collect(&self, metrics: &MetricsResponse) -> MetricFamily {
        let mut family = MetricFamily::default();
        family.set_name(self.name.to_owned());
        family.set_help(self.help.to_owned());
        family.set_field_type(self.kind.metric_type());

        for sample in &self.samples {
            let value = (sample.read)(metrics);
            family
                .mut_metric()
                .push(metric_with_value(self.kind, value, &sample.labels));
        }

        family
    }
}

macro_rules! metric_specs {
    ($($kind:ident, $name:literal, $help:literal, $label_names:expr, $labels:expr, $read:expr;)+) => {
        const METRIC_SPECS: &[MetricSpec] = &[
            $(MetricSpec {
                kind: MetricKind::$kind,
                name: $name,
                help: $help,
                label_names: $label_names,
                labels: $labels,
                read: $read,
            },)+
        ];
    };
}

metric_specs! {
    Gauge, "aionbd_uptime_ms", "Process uptime in milliseconds.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.uptime_ms);
    Counter, "aionbd_http_requests_total", "Total number of processed HTTP requests.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.http_requests_total);
    Gauge, "aionbd_http_requests_in_flight", "Number of HTTP requests currently being processed.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.http_requests_in_flight);
    Counter, "aionbd_http_responses_total", "Total number of HTTP requests grouped by status class.", &["status"], &[("status", "2xx")], |m: &MetricsResponse| u64_to_f64(m.http_responses_2xx_total);
    Counter, "aionbd_http_responses_total", "Total number of HTTP requests grouped by status class.", &["status"], &[("status", "4xx")], |m: &MetricsResponse| u64_to_f64(m.http_responses_4xx_total);
    Counter, "aionbd_http_responses_total", "Total number of HTTP requests grouped by status class.", &["status"], &[("status", "5xx")], |m: &MetricsResponse| u64_to_f64(m.http_responses_5xx_total);
    Counter, "aionbd_http_request_duration_us_total", "Sum of HTTP request processing time in microseconds.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.http_request_duration_us_total);
    Gauge, "aionbd_http_request_duration_us_max", "Maximum HTTP request processing time in microseconds.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.http_request_duration_us_max);
    Gauge, "aionbd_http_request_duration_us_avg", "Mean HTTP request processing time in microseconds.", &[], &[], |m: &MetricsResponse| m.http_request_duration_us_avg;
    Gauge, "aionbd_ready", "Server readiness flag (1 ready, 0 not ready).", &[], &[], |m: &MetricsResponse| bool_to_f64(m.ready);
    Gauge, "aionbd_engine_loaded", "Engine readiness flag (1 ready, 0 not ready).", &[], &[], |m: &MetricsResponse| bool_to_f64(m.engine_loaded);
    Gauge, "aionbd_storage_available", "Storage readiness flag (1 ready, 0 not ready).", &[], &[], |m: &MetricsResponse| bool_to_f64(m.storage_available);
    Gauge, "aionbd_collections", "Number of collections currently loaded.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.collections);
    Gauge, "aionbd_points", "Number of points currently loaded across collections.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.points);
    Gauge, "aionbd_l2_indexes", "Number of cached L2 IVF indexes.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.l2_indexes);
    Counter, "aionbd_l2_index_cache_lookups", "Total L2 index cache lookups.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.l2_index_cache_lookups);
    Counter, "aionbd_l2_index_cache_hits", "Total L2 index cache hits.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.l2_index_cache_hits);
    Counter, "aionbd_l2_index_cache_misses", "Total L2 index cache misses.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.l2_index_cache_misses);
    Gauge, "aionbd_l2_index_cache_hit_ratio", "L2 index cache hit ratio.", &[], &[], |m: &MetricsResponse| m.l2_index_cache_hit_ratio;
    Counter, "aionbd_l2_index_build_requests", "Total asynchronous L2 index build requests.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.l2_index_build_requests);
    Counter, "aionbd_l2_index_build_successes", "Total successful asynchronous L2 index builds.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.l2_index_build_successes);
    Counter, "aionbd_l2_index_build_failures", "Total failed asynchronous L2 index builds.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.l2_index_build_failures);
    Counter, "aionbd_l2_index_build_cooldown_skips", "Total asynchronous L2 index build requests skipped due to cooldown.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.l2_index_build_cooldown_skips);
    Gauge, "aionbd_l2_index_build_cooldown_ms", "Configured cooldown window in milliseconds for asynchronous L2 index rebuild scheduling.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.l2_index_build_cooldown_ms);
    Gauge, "aionbd_l2_index_build_max_in_flight", "Configured maximum concurrent asynchronous L2 index builds.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.l2_index_build_max_in_flight);
    Gauge, "aionbd_l2_index_warmup_on_boot", "L2 index warmup-on-boot flag (1 enabled, 0 disabled).", &[], &[], |m: &MetricsResponse| bool_to_f64(m.l2_index_warmup_on_boot);
    Gauge, "aionbd_l2_index_build_in_flight", "Number of currently running asynchronous L2 index builds.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.l2_index_build_in_flight);
    Counter, "aionbd_auth_failures_total", "Total authentication failures.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.auth_failures_total);
    Counter, "aionbd_rate_limit_rejections_total", "Total rate-limited requests.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.rate_limit_rejections_total);
    Counter, "aionbd_audit_events_total", "Total emitted audit events.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.audit_events_total);
    Gauge, "aionbd_collection_write_lock_entries", "Number of collection write semaphores currently tracked.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.collection_write_lock_entries);
    Gauge, "aionbd_tenant_rate_window_entries", "Number of tenant rate-limit windows currently tracked.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.tenant_rate_window_entries);
    Gauge, "aionbd_tenant_quota_lock_entries", "Number of tenant quota semaphores currently tracked.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.tenant_quota_lock_entries);
    Counter, "aionbd_tenant_quota_collection_rejections_total", "Total collection write rejections due to tenant quota.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.tenant_quota_collection_rejections_total);
    Counter, "aionbd_tenant_quota_point_rejections_total", "Total point write rejections due to tenant quota.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.tenant_quota_point_rejections_total);
    Gauge, "aionbd_persistence_enabled", "Persistence mode flag (1 enabled, 0 disabled).", &[], &[], |m: &MetricsResponse| bool_to_f64(m.persistence_enabled);
    Gauge, "aionbd_persistence_wal_sync_on_write", "WAL fsync-on-write flag (1 enabled, 0 disabled).", &[], &[], |m: &MetricsResponse| bool_to_f64(m.persistence_wal_sync_on_write);
    Gauge, "aionbd_persistence_wal_sync_every_n_writes", "Periodic WAL fsync cadence when sync-on-write is disabled (0 means never).", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_wal_sync_every_n_writes);
    Gauge, "aionbd_persistence_wal_sync_interval_seconds", "Time-based WAL fsync cadence in seconds when sync-on-write is disabled (0 means never).", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_wal_sync_interval_seconds);
    Gauge, "aionbd_persistence_wal_group_commit_max_batch", "Maximum number of writes coalesced into one WAL group commit.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.persistence_wal_group_commit_max_batch);
    Gauge, "aionbd_persistence_wal_group_commit_flush_delay_ms", "Delay window in milliseconds used to coalesce WAL group commits.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_wal_group_commit_flush_delay_ms);
    Gauge, "aionbd_persistence_async_checkpoints", "Persistence async checkpoint scheduling flag (1 enabled, 0 disabled).", &[], &[], |m: &MetricsResponse| bool_to_f64(m.persistence_async_checkpoints);
    Gauge, "aionbd_persistence_checkpoint_compact_after", "Incremental segment count threshold before snapshot compaction.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.persistence_checkpoint_compact_after);
    Counter, "aionbd_persistence_writes", "Successful persisted writes since startup.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_writes);
    Gauge, "aionbd_persistence_checkpoint_in_flight", "Persistence checkpoint scheduler flag (1 running, 0 idle).", &[], &[], |m: &MetricsResponse| bool_to_f64(m.persistence_checkpoint_in_flight);
    Counter, "aionbd_persistence_checkpoint_degraded_total", "Total checkpoints that fell back to WAL-only mode.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_checkpoint_degraded_total);
    Counter, "aionbd_persistence_checkpoint_success_total", "Total successful checkpoints.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_checkpoint_success_total);
    Counter, "aionbd_persistence_checkpoint_error_total", "Total checkpoint attempts that failed with an internal error.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_checkpoint_error_total);
    Counter, "aionbd_persistence_checkpoint_schedule_skips_total", "Total due checkpoints skipped because another checkpoint was already in flight.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_checkpoint_schedule_skips_total);
    Counter, "aionbd_persistence_wal_group_commits_total", "Total WAL commit operations executed by the group-commit writer.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_wal_group_commits_total);
    Counter, "aionbd_persistence_wal_grouped_records_total", "Total WAL records processed by the group-commit writer.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_wal_grouped_records_total);
    Gauge, "aionbd_persistence_wal_group_queue_depth", "Current number of pending WAL writes waiting in the group-commit queue.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.persistence_wal_group_queue_depth);
    Gauge, "aionbd_persistence_wal_size_bytes", "Current WAL file size in bytes.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_wal_size_bytes);
    Gauge, "aionbd_persistence_wal_tail_open", "WAL tail truncation signal (1 when WAL does not end with a newline).", &[], &[], |m: &MetricsResponse| bool_to_f64(m.persistence_wal_tail_open);
    Gauge, "aionbd_persistence_incremental_segments", "Current number of incremental snapshot segment files.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_incremental_segments);
    Gauge, "aionbd_persistence_incremental_size_bytes", "Current total size of incremental snapshot segment files in bytes.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.persistence_incremental_size_bytes);
    Counter, "aionbd_search_queries_total", "Total search requests handled.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.search_queries_total);
    Counter, "aionbd_search_ivf_queries_total", "Total search requests executed in IVF mode.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.search_ivf_queries_total);
    Counter, "aionbd_search_ivf_fallback_exact_total", "Total explicit IVF searches that fell back to exact scan.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.search_ivf_fallback_exact_total);
    Gauge, "aionbd_max_points_per_collection", "Configured maximum number of points per collection.", &[], &[], |m: &MetricsResponse| usize_to_f64(m.max_points_per_collection);
    Gauge, "aionbd_memory_budget_bytes", "Configured in-memory vector budget in bytes (0 means unlimited).", &[], &[], |m: &MetricsResponse| u64_to_f64(m.memory_budget_bytes);
    Gauge, "aionbd_memory_used_bytes", "Estimated in-memory vector usage in bytes.", &[], &[], |m: &MetricsResponse| u64_to_f64(m.memory_used_bytes);
}

fn build_families() -> Result<Vec<FamilyDef>, prometheus::Error> {
    let mut families: Vec<FamilyDef> = Vec::new();

    for spec in METRIC_SPECS {
        let sample = SampleDef {
            labels: build_label_pairs(spec.labels),
            read: spec.read,
        };

        if let Some(existing) = families.iter_mut().find(|family| {
            family.kind == spec.kind && family.name == spec.name && family.help == spec.help
        }) {
            existing.samples.push(sample);
            continue;
        }

        families.push(FamilyDef {
            kind: spec.kind,
            name: spec.name,
            help: spec.help,
            desc: build_desc(spec.name, spec.help, spec.label_names)?,
            samples: vec![sample],
        });
    }

    Ok(families)
}

fn build_desc(name: &str, help: &str, labels: &[&str]) -> Result<Desc, prometheus::Error> {
    Desc::new(
        name.to_owned(),
        help.to_owned(),
        labels.iter().map(|label| (*label).to_owned()).collect(),
        HashMap::new(),
    )
}

fn build_label_pairs(labels: &[(&str, &str)]) -> Vec<LabelPair> {
    labels
        .iter()
        .map(|(name, value)| {
            let mut pair = LabelPair::default();
            pair.set_name((*name).to_owned());
            pair.set_value((*value).to_owned());
            pair
        })
        .collect()
}

fn metric_with_value(kind: MetricKind, value: f64, labels: &[LabelPair]) -> Metric {
    let mut metric = Metric::default();

    for label in labels {
        metric.mut_label().push(label.clone());
    }

    match kind {
        MetricKind::Counter => {
            let mut counter = Counter::default();
            counter.set_value(value);
            metric.set_counter(counter);
        }
        MetricKind::Gauge => {
            let mut gauge = Gauge::default();
            gauge.set_value(value);
            metric.set_gauge(gauge);
        }
    }

    metric
}

fn bool_to_f64(value: bool) -> f64 {
    if value {
        1.0
    } else {
        0.0
    }
}

fn u64_to_f64(value: u64) -> f64 {
    maybe_warn_precision_loss(value);
    value as f64
}

fn usize_to_f64(value: usize) -> f64 {
    let value = value.min(u64::MAX as usize) as u64;
    u64_to_f64(value)
}

fn maybe_warn_precision_loss(value: u64) {
    if value <= MAX_EXACT_INTEGER_IN_F64 {
        return;
    }

    if !PRECISION_LOSS_WARNED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            observed = value,
            max_exact = MAX_EXACT_INTEGER_IN_F64,
            "prometheus numeric value exceeds exact float64 integer range; exported value is rounded"
        );
    }
}
