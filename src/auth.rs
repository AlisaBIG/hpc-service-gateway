use crate::{config::ApiKey, db::now, error::ApiError, AppState};
use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Mutex, time::Instant};

#[derive(Clone, Debug)]
pub struct Principal {
    pub subject: String,
    pub admin: bool,
}
impl Principal {
    pub fn require_admin(&self) -> Result<(), ApiError> {
        if self.admin {
            Ok(())
        } else {
            Err(ApiError::forbidden())
        }
    }
}
struct Bucket {
    tokens: f64,
    updated: Instant,
}
pub struct Limiter {
    buckets: Mutex<HashMap<String, Bucket>>,
}
impl Default for Limiter {
    fn default() -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
        }
    }
}
impl Limiter {
    pub fn allow(&self, key: &str, rate: u32, burst: u32) -> bool {
        let mut all = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        let b = all.entry(key.into()).or_insert_with(|| Bucket {
            tokens: burst as f64,
            updated: Instant::now(),
        });
        b.tokens = (b.tokens + b.updated.elapsed().as_secs_f64() * rate as f64).min(burst as f64);
        b.updated = Instant::now();
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}
fn authenticate(keys: &[ApiKey], token: &str) -> Option<Principal> {
    let digest = format!("{:x}", Sha256::digest(token.as_bytes()));
    keys.iter()
        .find(|k| constant_equal(k.sha256.to_ascii_lowercase().as_bytes(), digest.as_bytes()))
        .map(|k| Principal {
            subject: k.subject.clone(),
            admin: k.admin,
        })
}
fn constant_equal(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |n, (a, b)| n | (a ^ b)) == 0
}
pub async fn guard(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    if !state.limiter.allow(
        "global",
        state.config.global_rate,
        state.config.global_burst,
    ) {
        return limited();
    }
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    let Some(user) = token.and_then(|t| authenticate(&state.config.api_keys, t)) else {
        return ApiError(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Valid Bearer API key required".into(),
        )
        .into_response();
    };
    if !state.limiter.allow(
        &format!("key:{}", user.subject),
        state.config.rate_per_key,
        state.config.burst_per_key,
    ) {
        return limited();
    }
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    request.extensions_mut().insert(user.clone());
    let response = next.run(request).await;
    // A bounded channel keeps SQLite writes off the GET latency path.
    if state
        .audit_tx
        .try_send(Audit {
            owner: user.subject,
            method,
            path,
            status: response.status().as_u16(),
        })
        .is_err()
    {
        tracing::warn!("audit queue full");
    }
    response
}
fn limited() -> Response {
    let mut r = ApiError(
        StatusCode::TOO_MANY_REQUESTS,
        "rate_limited",
        "Request rate exceeded".into(),
    )
    .into_response();
    r.headers_mut()
        .insert(header::RETRY_AFTER, "1".parse().unwrap());
    r
}
pub struct Audit {
    pub owner: String,
    pub method: String,
    pub path: String,
    pub status: u16,
}
pub async fn audit_worker(db: crate::db::Db, mut rx: tokio::sync::mpsc::Receiver<Audit>) {
    while let Some(a) = rx.recv().await {
        if let Err(e) = sqlx::query(
            "INSERT INTO audit_logs(owner,method,path,status,created_at) VALUES(?,?,?,?,?)",
        )
        .bind(a.owner)
        .bind(a.method)
        .bind(a.path)
        .bind(a.status)
        .bind(now())
        .execute(&db.0)
        .await
        {
            tracing::warn!(error=%e,"audit insert failed");
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn buckets_refuse_excess() {
        let l = Limiter::default();
        assert!(l.allow("a", 1, 1));
        assert!(!l.allow("a", 1, 1));
        assert!(l.allow("b", 1, 1));
    }
    #[test]
    fn hashes_compare() {
        assert!(constant_equal(b"abc", b"abc"));
        assert!(!constant_equal(b"abc", b"abd"));
    }
}
