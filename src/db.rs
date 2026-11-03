use crate::{
    auth::Principal,
    error::{ApiError, ApiResult},
};
use serde_json::Value;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};
use std::path::Path;

#[derive(Clone)]
pub struct Db(pub SqlitePool);
impl Db {
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(include_str!("../migrations/001_init.sql"))
            .execute(&pool)
            .await?;
        Ok(Self(pool))
    }
    fn table(table: &str) -> &str {
        assert!(matches!(table, "jobs" | "runs" | "files"));
        table
    }
    pub async fn put(&self, table: &str, id: &str, owner: &str, data: &Value) -> ApiResult<()> {
        let sql=format!("INSERT INTO {}(id,owner,data,updated_at) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data,updated_at=excluded.updated_at",Self::table(table));
        sqlx::query(&sql)
            .bind(id)
            .bind(owner)
            .bind(data.to_string())
            .bind(now())
            .execute(&self.0)
            .await?;
        Ok(())
    }
    pub async fn get(&self, table: &str, id: &str, user: &Principal) -> ApiResult<Value> {
        let row = sqlx::query(&format!(
            "SELECT owner,data FROM {} WHERE id=?",
            Self::table(table)
        ))
        .bind(id)
        .fetch_optional(&self.0)
        .await?
        .ok_or_else(ApiError::not_found)?;
        let owner: String = row.get("owner");
        if !user.admin && owner != user.subject {
            return Err(ApiError::not_found());
        }
        serde_json::from_str(row.get("data")).map_err(|e| anyhow::Error::from(e).into())
    }
    pub async fn list(&self, table: &str, user: &Principal) -> ApiResult<Vec<Value>> {
        let rows = sqlx::query(&format!(
            "SELECT data FROM {} WHERE (? OR owner=?) ORDER BY updated_at DESC LIMIT 1000",
            Self::table(table)
        ))
        .bind(user.admin)
        .bind(&user.subject)
        .fetch_all(&self.0)
        .await?;
        rows.iter()
            .map(|r| serde_json::from_str(r.get("data")).map_err(|e| anyhow::Error::from(e).into()))
            .collect()
    }
    pub async fn active_jobs(&self) -> ApiResult<Vec<Value>> {
        let rows=sqlx::query("SELECT data FROM jobs WHERE json_extract(data,'$.state') NOT IN ('COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED','SUBMISSION_UNKNOWN')").fetch_all(&self.0).await?;
        rows.iter()
            .map(|r| serde_json::from_str(r.get("data")).map_err(|e| anyhow::Error::from(e).into()))
            .collect()
    }
}
pub fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}
