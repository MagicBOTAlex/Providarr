use std::time::{Duration, Instant};

use parking_lot::Mutex;
use secrecy::{ExposeSecret, SecretString};

use crate::{config::AuthConfig, error::AppError, ratelimit::limiter::ProviderRuntime};

/// The credentials to attach to an upstream request.
#[derive(Debug, Clone)]
pub enum ResolvedAuth {
    None,
    Bearer(String),
    Query { param: String, value: String },
}

#[derive(Debug, Clone)]
struct CachedToken {
    token: String,
    expires_at: Instant,
}

/// Resolves and caches credentials for a provider.
///
/// `bearer` reads a static token, `query` reads a static key, and `tvdb_login`
/// performs the TVDB v4 `/login` handshake and caches the returned token.
pub struct AuthManager {
    config: AuthConfig,
    api_key: Option<SecretString>,
    token: Mutex<Option<CachedToken>>,
}

impl AuthManager {
    pub fn new(config: AuthConfig) -> Self {
        let env_var = match &config {
            AuthConfig::None => None,
            AuthConfig::Bearer { env_var }
            | AuthConfig::TvdbLogin { env_var }
            | AuthConfig::Query { env_var, .. } => Some(env_var),
        };

        let api_key = env_var.and_then(|name| match std::env::var(name) {
            Ok(value) if !value.trim().is_empty() => Some(SecretString::from(value)),
            _ => {
                tracing::warn!(env_var = name, "provider credential is not set");
                None
            }
        });

        Self {
            config,
            api_key,
            token: Mutex::new(None),
        }
    }

    pub fn has_credentials(&self) -> bool {
        self.api_key.is_some()
    }

    pub async fn resolve(&self, runtime: &ProviderRuntime) -> Result<ResolvedAuth, AppError> {
        match &self.config {
            AuthConfig::None => Ok(ResolvedAuth::None),
            AuthConfig::Bearer { env_var } => {
                let key = self.api_key.as_ref().ok_or_else(|| {
                    AppError::Config(format!("missing bearer credential for {env_var}"))
                })?;
                Ok(ResolvedAuth::Bearer(key.expose_secret().to_string()))
            }
            AuthConfig::Query { param, env_var } => {
                let key = self.api_key.as_ref().ok_or_else(|| {
                    AppError::Config(format!("missing query credential for {env_var}"))
                })?;
                Ok(ResolvedAuth::Query {
                    param: param.clone(),
                    value: key.expose_secret().to_string(),
                })
            }
            AuthConfig::TvdbLogin { .. } => {
                if let Some(token) = self.valid_token() {
                    return Ok(ResolvedAuth::Bearer(token));
                }
                let token = self.login(runtime).await?;
                Ok(ResolvedAuth::Bearer(token))
            }
        }
    }

    fn valid_token(&self) -> Option<String> {
        let guard = self.token.lock();
        guard
            .as_ref()
            .filter(|cached| cached.expires_at > Instant::now())
            .map(|cached| cached.token.clone())
    }

    async fn login(&self, runtime: &ProviderRuntime) -> Result<String, AppError> {
        let key = self
            .api_key
            .as_ref()
            .ok_or_else(|| AppError::Config("missing TVDB API key".to_string()))?;

        let endpoint = "login";
        let _permit = runtime.acquire(endpoint).await?;
        let url = format!("{}/login", runtime.config().base_url.trim_end_matches('/'));

        let response = runtime
            .http()
            .post(&url)
            .json(&serde_json::json!({ "apikey": key.expose_secret() }))
            .timeout(runtime.effective_timeout())
            .send()
            .await;

        let response = match response {
            Ok(response) => response,
            Err(err) => {
                runtime.on_failure(endpoint, None);
                if err.is_timeout() {
                    return Err(AppError::Timeout);
                }
                return Err(AppError::Http(err));
            }
        };

        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            runtime.on_rate_limited(endpoint, 429);
            return Err(AppError::RateLimited {
                provider: runtime.name().to_string(),
                retry_after_ms: runtime.backoff_remaining().as_millis() as u64,
            });
        }
        if !status.is_success() {
            runtime.on_failure(endpoint, Some(status.as_u16()));
            return Err(AppError::Upstream {
                status: status.as_u16(),
                body: format!("TVDB login failed with {status}"),
            });
        }

        let body: serde_json::Value = response.json().await.map_err(AppError::Http)?;
        let token = body
            .pointer("/data/token")
            .and_then(|value| value.as_str())
            .ok_or_else(|| AppError::Internal("TVDB login response had no token".to_string()))?
            .to_string();

        runtime.on_success(endpoint, status.as_u16());
        *self.token.lock() = Some(CachedToken {
            token: token.clone(),
            expires_at: Instant::now() + Duration::from_secs(24 * 60 * 60),
        });
        Ok(token)
    }
}
