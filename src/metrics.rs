use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use once_cell::sync::OnceCell;

static HANDLE: OnceCell<PrometheusHandle> = OnceCell::new();

/// Installs the Prometheus recorder once per process and returns a handle that
/// can render the current metric snapshot.
pub fn install() -> PrometheusHandle {
    HANDLE
        .get_or_init(|| {
            let handle = PrometheusBuilder::new()
                .install_recorder()
                .expect("failed to install Prometheus recorder");
            describe();
            handle
        })
        .clone()
}

fn describe() {
    metrics::describe_counter!(
        "providarr_requests_total",
        "Upstream requests sent by Providarr"
    );
    metrics::describe_counter!(
        "providarr_rate_limited_total",
        "Upstream rate-limit (429) responses observed"
    );
    metrics::describe_counter!("providarr_cache_hits_total", "Cache hits");
    metrics::describe_counter!("providarr_cache_misses_total", "Cache misses");
    metrics::describe_counter!(
        "providarr_cache_stale_total",
        "Stale cache entries served after an upstream error"
    );
    metrics::describe_counter!(
        "providarr_cache_oversized_total",
        "Cache writes skipped because the body exceeded max_body_bytes"
    );
    metrics::describe_counter!(
        "providarr_dropped_total",
        "Requests dropped because a wait exceeded its budget"
    );
    metrics::describe_counter!(
        "providarr_upstream_errors_total",
        "Upstream transport or timeout errors"
    );
    metrics::describe_counter!(
        "providarr_replay_total",
        "Requests served from replay fixtures"
    );
    metrics::describe_counter!(
        "providarr_replay_miss_total",
        "Replay lookups that had no fixture"
    );
    metrics::describe_counter!(
        "providarr_inbound_limited_total",
        "Inbound requests rejected by the per-IP limiter"
    );
    metrics::describe_counter!(
        "providarr_inbound_allowed_total",
        "Inbound requests allowed by the per-IP limiter"
    );
    metrics::describe_counter!(
        "providarr_inbound_bypassed_total",
        "Inbound requests exempted via an IP/CIDR bypass rule"
    );
    metrics::describe_counter!(
        "providarr_inbound_global_limited_total",
        "Inbound requests rejected by the server-wide (global) limiter"
    );
    metrics::describe_counter!(
        "providarr_inbound_concurrency_limited_total",
        "Inbound requests rejected because the concurrency cap was reached"
    );
    metrics::describe_gauge!(
        "providarr_backoff_seconds",
        "Current backoff wait for a provider endpoint"
    );

    // Register the label-less inbound counters eagerly so they render from zero
    // even before the first increment. Otherwise `/metrics` omits a counter
    // until some request happens to trip it.
    let _ = metrics::counter!("providarr_inbound_limited_total");
    let _ = metrics::counter!("providarr_inbound_allowed_total");
    let _ = metrics::counter!("providarr_inbound_bypassed_total");
    let _ = metrics::counter!("providarr_inbound_global_limited_total");
    let _ = metrics::counter!("providarr_inbound_concurrency_limited_total");
}

pub fn request(provider: &str, endpoint: &str, status: u16) {
    metrics::counter!(
        "providarr_requests_total",
        "provider" => provider.to_string(),
        "endpoint" => endpoint.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

pub fn rate_limited(provider: &str, endpoint: &str) {
    metrics::counter!(
        "providarr_rate_limited_total",
        "provider" => provider.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
}

pub fn cache_hit(provider: &str, endpoint: &str) {
    metrics::counter!(
        "providarr_cache_hits_total",
        "provider" => provider.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
}

pub fn cache_miss(provider: &str, endpoint: &str) {
    metrics::counter!(
        "providarr_cache_misses_total",
        "provider" => provider.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
}

pub fn cache_stale(provider: &str, endpoint: &str) {
    metrics::counter!(
        "providarr_cache_stale_total",
        "provider" => provider.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
}

pub fn cache_oversized(provider: &str, endpoint: &str) {
    metrics::counter!(
        "providarr_cache_oversized_total",
        "provider" => provider.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
}

pub fn dropped(provider: &str, endpoint: &str) {
    metrics::counter!(
        "providarr_dropped_total",
        "provider" => provider.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
}

pub fn upstream_error(provider: &str, endpoint: &str) {
    metrics::counter!(
        "providarr_upstream_errors_total",
        "provider" => provider.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
}

pub fn backoff_seconds(provider: &str, endpoint: &str, seconds: f64) {
    metrics::gauge!(
        "providarr_backoff_seconds",
        "provider" => provider.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .set(seconds);
}

pub fn replayed(provider: &str, endpoint: &str) {
    metrics::counter!(
        "providarr_replay_total",
        "provider" => provider.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
}

pub fn replay_miss(provider: &str) {
    metrics::counter!(
        "providarr_replay_miss_total",
        "provider" => provider.to_string()
    )
    .increment(1);
}

pub fn inbound_limited() {
    metrics::counter!("providarr_inbound_limited_total").increment(1);
}

pub fn inbound_allowed() {
    metrics::counter!("providarr_inbound_allowed_total").increment(1);
}

pub fn inbound_bypassed() {
    metrics::counter!("providarr_inbound_bypassed_total").increment(1);
}

pub fn inbound_global_limited() {
    metrics::counter!("providarr_inbound_global_limited_total").increment(1);
}

pub fn inbound_concurrency_limited() {
    metrics::counter!("providarr_inbound_concurrency_limited_total").increment(1);
}
