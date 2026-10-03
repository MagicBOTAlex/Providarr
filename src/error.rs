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
        let status = match &self {
            AppError::UnknownProvider(_) | AppError::NotFound | AppError::ReplayMiss(_) => {
                StatusCode::NOT_FOUND
            }
            AppError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            AppError::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
            AppError::Dropped(_) => StatusCode::SERVICE_UNAVAILABLE,
            AppError::Timeout => StatusCode::GATEWAY_TIMEOUT,
            AppError::Http(_) | AppError::Upstream { .. } => StatusCode::BAD_GATEWAY,
            AppError::Config(_)
            | AppError::Database(_)
            | AppError::Migrate(_)
            | AppError::Serde(_)
            | AppError::Io(_)
            | AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let mut response = (status, Json(json!({ "error": self.to_string() }))).into_response();

        if let AppError::RateLimited { retry_after_ms, .. } = &self {
            let secs = (*retry_after_ms).div_ceil(1_000).max(1);
            if let Ok(value) = secs.to_string().parse() {
                response.headers_mut().insert("retry-after", value);
            }
        }

        response
    }
}
