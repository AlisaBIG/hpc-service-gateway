use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

pub struct ApiError(pub StatusCode, pub &'static str, pub String);
pub type ApiResult<T> = Result<T, ApiError>;
impl ApiError {
    pub fn bad(message: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, "bad_request", message.into())
    }
    pub fn not_found() -> Self {
        Self(
            StatusCode::NOT_FOUND,
            "not_found",
            "Resource not found".into(),
        )
    }
    pub fn forbidden() -> Self {
        Self(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Administrator role required".into(),
        )
    }
    pub fn conflict(message: impl Into<String>) -> Self {
        Self(StatusCode::CONFLICT, "conflict", message.into())
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(json!({"error":{"code":self.1,"message":self.2}})),
        )
            .into_response()
    }
}
impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!(error=%e,"operation failed");
        Self(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Operation failed; consult server log".into(),
        )
    }
}
impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        anyhow::Error::from(e).into()
    }
}
impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self {
        anyhow::Error::from(e).into()
    }
}
