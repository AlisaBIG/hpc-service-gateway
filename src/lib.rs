pub mod api;
pub mod auth;
pub mod config;
pub mod db;
pub mod error;
pub mod slurm;
pub mod workflow;

use axum::{extract::DefaultBodyLimit, middleware, routing::get, Router};
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex, RwLock};
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<config::Config>,
    pub db: db::Db,
    pub slurm: Arc<slurm::SlurmClient>,
    pub templates: Arc<RwLock<HashMap<String, workflow::Template>>>,
    pub limiter: Arc<auth::Limiter>,
    pub audit_tx: mpsc::Sender<auth::Audit>,
    pub mutations: Arc<Mutex<()>>,
    pub http: reqwest::Client,
}

pub async fn initialize(mut config: config::Config) -> anyhow::Result<AppState> {
    config.validate()?;
    tokio::fs::create_dir_all(&config.data_dir).await?;
    config.data_dir = tokio::fs::canonicalize(&config.data_dir).await?;
    for dir in ["uploads", "runs", "jobs"] {
        tokio::fs::create_dir_all(config.data_dir.join(dir)).await?;
    }
    let database_path = config
        .database_path
        .clone()
        .unwrap_or_else(|| config.data_dir.join("gateway.sqlite3"));
    if let Some(parent) = database_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent).await?;
    }
    let db = db::Db::open(&database_path).await?;
    let builtin = workflow::load_templates(&config.workflow_dir)?;
    for t in &builtin {
        sqlx::query(
            "INSERT OR IGNORE INTO workflow_versions(id,version,yaml,created_at) VALUES(?,?,?,?)",
        )
        .bind(&t.id)
        .bind(&t.version)
        .bind(serde_yaml::to_string(t)?)
        .bind("1970-01-01T00:00:00Z")
        .execute(&db.0)
        .await?;
    }
    let mut templates: HashMap<_, _> = builtin.into_iter().map(|t| (t.id.clone(), t)).collect();
    use sqlx::Row;
    let rows = sqlx::query("SELECT yaml FROM workflow_versions ORDER BY created_at ASC")
        .fetch_all(&db.0)
        .await?;
    for row in rows {
        let t = workflow::parse_template(row.get("yaml"))?;
        templates.insert(t.id.clone(), t);
    }
    // A process crash during sbatch must never cause automatic duplicate submission.
    sqlx::query("UPDATE jobs SET data=json_set(data,'$.state','SUBMISSION_UNKNOWN'),updated_at=? WHERE json_extract(data,'$.state')='SUBMITTING'").bind(db::now()).execute(&db.0).await?;
    sqlx::query("UPDATE runs SET data=json_set(data,'$.state',COALESCE((SELECT json_extract(j.data,'$.state') FROM jobs j WHERE j.id=json_extract(runs.data,'$.job_id')),'SUBMISSION_UNKNOWN')),updated_at=? WHERE json_extract(data,'$.state')='SUBMITTING'").bind(db::now()).execute(&db.0).await?;
    let (tx, rx) = mpsc::channel(8192);
    tokio::spawn(auth::audit_worker(db.clone(), rx));
    Ok(AppState {
        slurm: Arc::new(slurm::SlurmClient::new(
            config.mock_slurm,
            config.command_timeout_seconds,
            config.command_concurrency,
        )),
        http: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .build()?,
        config: Arc::new(config),
        db,
        templates: Arc::new(RwLock::new(templates)),
        limiter: Arc::new(auth::Limiter::default()),
        audit_tx: tx,
        mutations: Arc::new(Mutex::new(())),
    })
}

pub fn router(state: AppState) -> Router {
    let spec: serde_json::Value =
        serde_json::from_str(include_str!("../openapi.json")).expect("valid embedded OpenAPI");
    let protected = api::routes().layer(middleware::from_fn_with_state(state.clone(), auth::guard));
    Router::new()
        .route(
            "/healthz",
            get(|| async { axum::Json(json!({"status":"ok"})) }),
        )
        .route("/readyz", get(api::ready))
        .nest("/api/v1", protected)
        .merge(SwaggerUi::new("/swagger-ui").external_url_unchecked("/api-docs/openapi.json", spec))
        .layer(DefaultBodyLimit::max(
            state.config.max_upload_bytes.saturating_add(64 * 1024),
        ))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(state)
}

pub async fn poll(state: AppState) {
    let mut interval =
        tokio::time::interval(std::time::Duration::from_secs(state.config.poll_seconds));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if let Err(e) = poll_once(&state).await {
            tracing::warn!(error=%e.2,"poll failed");
        }
    }
}

pub async fn poll_once(state: &AppState) -> error::ApiResult<()> {
    use futures_util::{stream, StreamExt};
    let jobs = state.db.active_jobs().await?;
    let results = stream::iter(jobs.into_iter().filter(|j| j["slurm_id"].is_string()).map(
        |job| async move {
            let q = state
                .slurm
                .query(
                    job["cluster_id"].as_str().unwrap_or(""),
                    job["slurm_id"].as_str().unwrap_or(""),
                )
                .await;
            (job, q)
        },
    ))
    .buffer_unordered(state.config.command_concurrency);
    tokio::pin!(results);
    let admin = auth::Principal {
        subject: "poller".into(),
        admin: true,
    };
    while let Some((mut job, result)) = results.next().await {
        let _lock = state.mutations.lock().await;
        let id = job["id"].as_str().unwrap_or("").to_string();
        let current = state.db.get("jobs", &id, &admin).await?;
        if current["updated_at"] != job["updated_at"] {
            continue;
        }
        match result {
            Ok(info) => {
                // UNKNOWN is observable but never interpreted as successful completion.
                let next_state = if current["state"] == "HELD" && info.state == "PENDING" {
                    "HELD".to_string()
                } else {
                    info.state
                };
                if state.slurm.mock()
                    && next_state == "COMPLETED"
                    && current["state"] != "COMPLETED"
                {
                    if let Some(run_id) = job["run_id"].as_str() {
                        let run = state.db.get("runs", run_id, &admin).await?;
                        let template: workflow::Template =
                            serde_json::from_value(run["template"].clone())
                                .map_err(anyhow::Error::from)?;
                        let dir = std::path::Path::new(run["run_dir"].as_str().unwrap_or(""));
                        workflow::materialize_mock_outputs(&template, dir)
                            .map_err(error::ApiError::from)?;
                    }
                }
                job["state"] = json!(next_state);
                job["exit_code"] = json!(info.exit_code);
                job["elapsed_seconds"] = json!(info.elapsed_seconds);
                job["last_poll_error"] = serde_json::Value::Null;
            }
            Err(e) => {
                job["last_poll_error"] = json!(e.to_string());
            }
        }
        job["updated_at"] = json!(db::now());
        state
            .db
            .put("jobs", &id, job["owner"].as_str().unwrap_or(""), &job)
            .await?;
        if let Some(run_id) = job["run_id"].as_str() {
            let mut run = state.db.get("runs", run_id, &admin).await?;
            run["state"] = job["state"].clone();
            run["updated_at"] = job["updated_at"].clone();
            run["elapsed_seconds"] = job["elapsed_seconds"].clone();
            run["exit_code"] = job["exit_code"].clone();
            run["last_poll_error"] = job["last_poll_error"].clone();
            if api::terminal(run["state"].as_str().unwrap_or("")) && run["finished_at"].is_null() {
                run["finished_at"] = json!(db::now());
            }
            state
                .db
                .put("runs", run_id, run["owner"].as_str().unwrap_or(""), &run)
                .await?;
        }
    }
    Ok(())
}
