use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

use crate::ratelimit::limiter::LimitError;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("config error: {0}")]
    Config(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("http client error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("rate limited by {provider}: retry after {retry_after_ms}ms")]
    RateLimited {
        provider: String,
        retry_after_ms: u64,
    },

    #[error("request dropped: {0}")]
    Dropped(String),

    #[error("request timed out")]
    Timeout,

    #[error("unknown provider: {0}")]
    UnknownProvider(String),

    #[error("upstream {status}: {body}")]
    Upstream { status: u16, body: String },

    #[error("not found")]
    NotFound,

    #[error("not implemented: {0}")]
    NotImplemented(String),

    #[error("no replay fixture for {0}")]
    ReplayMiss(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl AppError {
    pub fn internal(msg: impl Into<String>) -> Self {
        AppError::Internal(msg.into())
    }
}

impl From<LimitError> for AppError {
    fn from(err: LimitError) -> Self {
        match err {
            LimitError::Dropped { reason, .. } => AppError::Dropped(reason),
            LimitError::Closed => AppError::Internal("rate limiter closed".into()),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message, retry_after_ms) = match self {
            AppError::UnknownProvider(_) => (StatusCode::NOT_FOUND, "unknown provider", None),
            AppError::NotFound | AppError::ReplayMiss(_) => {
                (StatusCode::NOT_FOUND, "not found", None)
            }
            AppError::NotImplemented(_) => (StatusCode::NOT_IMPLEMENTED, "not implemented", None),
            AppError::RateLimited { retry_after_ms, .. } => (
                StatusCode::TOO_MANY_REQUESTS,
                "rate limited",
                Some(retry_after_ms),
            ),
            AppError::Dropped(_) => (StatusCode::SERVICE_UNAVAILABLE, "service unavailable", None),
            AppError::Timeout => (StatusCode::GATEWAY_TIMEOUT, "upstream timed out", None),
            AppError::Http(err) => {
                tracing::error!(error = %err.without_url(), "request failed");
                (StatusCode::BAD_GATEWAY, "upstream request failed", None)
            }
            app @ AppError::Upstream { .. } => {
                tracing::error!(error = %app, "request failed");
                (StatusCode::BAD_GATEWAY, "upstream request failed", None)
            }
            app @ AppError::Config(_)
            | app @ AppError::Database(_)
            | app @ AppError::Migrate(_)
            | app @ AppError::Serde(_)
            | app @ AppError::Io(_)
            | app @ AppError::Internal(_) => {
                tracing::error!(error = %app, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error",
                    None,
                )
            }
        };

        let mut response = (status, Json(json!({ "error": message }))).into_response();

        if let Some(retry_after_ms) = retry_after_ms {
            let secs = retry_after_ms.div_ceil(1_000).max(1);
            if let Ok(value) = secs.to_string().parse() {
                response.headers_mut().insert("retry-after", value);
            }
        }

        response
    }
}
