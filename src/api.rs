use crate::{
    auth::Principal,
    config::identifier,
    db::now,
    error::{ApiError, ApiResult},
    workflow, AppState,
};
use axum::{
    extract::{Multipart, Path, Query, Request, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get, patch, post, put},
    Extension, Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, path::PathBuf};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/clusters", get(clusters))
        .route("/applications", get(applications))
        .route("/applications/{id}/queues", get(queues))
        .route("/applications/{id}/cluster", put(bind_cluster))
        .route("/jobs", get(jobs).post(submit_job))
        .route("/jobs/{id}", get(job))
        .route("/jobs/{id}/state", patch(job_action))
        .route("/files", get(files).post(upload))
        .route("/files/{id}", get(file))
        .route("/files/{id}/content", get(preview))
        .route("/files/{id}/download", get(download))
        .route("/workflows", get(workflows))
        .route("/workflows/{id}", get(template).put(edit_template))
        .route("/workflows/{id}/deploy", post(deploy))
        .route("/runs", get(runs))
        .route("/runs/{id}", get(run))
        .route("/runs/{id}/execute", post(execute))
        .route("/runs/{id}/logs", get(logs))
        .route("/runs/{id}/results", get(results))
        .route("/runs/{id}/validate", post(validate))
        .route("/runs/{id}/report", get(report))
        .route("/gateway/{service}/{*path}", any(forward))
}

pub async fn ready(State(s): State<AppState>) -> Response {
    match sqlx::query("SELECT 1").execute(&s.db.0).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({"status":"ready","mode":if s.slurm.mock(){"mock"}else{"slurm"}})),
        )
            .into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"unavailable"})),
        )
            .into_response(),
    }
}
async fn clusters(State(s): State<AppState>) -> Json<Value> {
    Json(json!(s.config.clusters))
}
async fn applications(State(s): State<AppState>) -> ApiResult<Json<Value>> {
    let mut apps = vec![];
    for app in &s.config.applications {
        let mut a = json!(app);
        a["cluster_id"] = json!(bound_cluster(&s, &app.id).await?);
        apps.push(a);
    }
    Ok(Json(json!(apps)))
}
async fn bound_cluster(s: &AppState, application: &str) -> ApiResult<String> {
    let app = s
        .config
        .applications
        .iter()
        .find(|a| a.id == application)
        .ok_or_else(ApiError::not_found)?;
    let id: Option<String> =
        sqlx::query_scalar("SELECT cluster_id FROM app_bindings WHERE application_id=?")
            .bind(application)
            .fetch_optional(&s.db.0)
            .await?;
    Ok(id.unwrap_or_else(|| app.cluster_id.clone()))
}
async fn resolve_target(
    s: &AppState,
    application: &str,
    requested: Option<&str>,
) -> ApiResult<(String, String)> {
    let id = bound_cluster(s, application).await?;
    if requested.is_some_and(|r| r != id) {
        return Err(ApiError::bad(
            "Requested cluster does not match application binding",
        ));
    }
    let app = s
        .config
        .applications
        .iter()
        .find(|a| a.id == application)
        .ok_or_else(ApiError::not_found)?;
    let cluster = s
        .config
        .clusters
        .iter()
        .find(|c| c.id == id)
        .ok_or_else(ApiError::not_found)?;
    let partition = app
        .partitions
        .iter()
        .find(|p| cluster.partitions.contains(p))
        .ok_or_else(|| ApiError::bad("No allowed application queue on cluster"))?
        .clone();
    Ok((id, partition))
}
async fn queues(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let (cluster, _) = resolve_target(&s, &id, None).await?;
    let app = s.config.applications.iter().find(|a| a.id == id).unwrap();
    let c = s.config.clusters.iter().find(|c| c.id == cluster).unwrap();
    Ok(Json(
        json!({"application_id":id,"cluster_id":cluster,"queues":app.partitions.iter().filter(|p|c.partitions.contains(p)).collect::<Vec<_>>(),"source":"configured_allowlist"}),
    ))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterBinding {
    cluster_id: String,
}
async fn bind_cluster(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
    Json(b): Json<ClusterBinding>,
) -> ApiResult<Json<Value>> {
    u.require_admin()?;
    let a = s
        .config
        .applications
        .iter()
        .find(|a| a.id == id)
        .ok_or_else(ApiError::not_found)?;
    let c = s
        .config
        .clusters
        .iter()
        .find(|c| c.id == b.cluster_id)
        .ok_or_else(|| ApiError::bad("Unknown cluster"))?;
    if !a.partitions.iter().any(|p| c.partitions.contains(p)) {
        return Err(ApiError::bad("No compatible partition"));
    }
    sqlx::query("INSERT INTO app_bindings(application_id,cluster_id) VALUES(?,?) ON CONFLICT(application_id) DO UPDATE SET cluster_id=excluded.cluster_id").bind(&id).bind(&b.cluster_id).execute(&s.db.0).await?;
    Ok(Json(json!({"application_id":id,"cluster_id":b.cluster_id})))
}

async fn jobs(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(s.db.list("jobs", &u).await?)))
}
async fn job(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    Ok(Json(s.db.get("jobs", &id, &u).await?))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitJob {
    application_id: String,
    cluster_id: Option<String>,
    script: String,
    name: String,
}
async fn submit_job(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Json(b): Json<SubmitJob>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    u.require_admin()?;
    if !identifier(&b.name) || b.script.len() > 1024 * 1024 || !b.script.starts_with("#!") {
        return Err(ApiError::bad(
            "Invalid job name/script (max 1 MiB; shebang required)",
        ));
    }
    let (cluster, partition) =
        resolve_target(&s, &b.application_id, b.cluster_id.as_deref()).await?;
    let id = Uuid::new_v4().to_string();
    let dir = s.config.data_dir.join("jobs").join(&id);
    tokio::fs::create_dir_all(&dir).await?;
    // Raw scripts are an administrative escape hatch; they run under the gateway Unix account.
    let script = dir.join("job.sbatch");
    tokio::fs::write(
        &script,
        format!(
            "#!/bin/bash\n#SBATCH --partition={partition}\n{}",
            b.script.lines().skip(1).collect::<Vec<_>>().join("\n")
        ),
    )
    .await?;
    let j = submit_record(
        &s,
        &u,
        &id,
        &cluster,
        &b.application_id,
        &b.name,
        &script,
        None,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(j)))
}
async fn submit_record(
    s: &AppState,
    u: &Principal,
    id: &str,
    cluster: &str,
    application: &str,
    name: &str,
    script: &std::path::Path,
    run_id: Option<&str>,
) -> ApiResult<Value> {
    let mut j = json!({"id":id,"owner":u.subject,"cluster_id":cluster,"application_id":application,"name":name,"run_id":run_id,"slurm_id":null,"state":"SUBMITTING","script_path":script,"created_at":now(),"updated_at":now(),"exit_code":null,"elapsed_seconds":null,"simulated":s.slurm.mock()});
    s.db.put("jobs", id, &u.subject, &j).await?;
    match s.slurm.submit(cluster, script).await {
        Ok(slurm_id) => {
            j["slurm_id"] = json!(slurm_id);
            j["state"] = json!("PENDING");
        }
        Err(e) => {
            tracing::error!(job_id=id,error=%e,"Slurm submission outcome unknown; reconcile manually");
            j["state"] = json!("SUBMISSION_UNKNOWN");
            j["last_poll_error"] =
                json!("Submission outcome unknown; inspect Slurm before retrying");
        }
    }
    j["updated_at"] = json!(now());
    s.db.put("jobs", id, &u.subject, &j).await?;
    Ok(j)
}
pub fn terminal(state: &str) -> bool {
    matches!(
        state,
        "COMPLETED"
            | "FAILED"
            | "CANCELLED"
            | "TIMEOUT"
            | "OUT_OF_MEMORY"
            | "NODE_FAIL"
            | "PREEMPTED"
            | "BOOT_FAIL"
            | "DEADLINE"
            | "REVOKED"
    )
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Action {
    action: String,
}
async fn job_action(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
    Json(b): Json<Action>,
) -> ApiResult<Json<Value>> {
    if !matches!(b.action.as_str(), "cancel" | "hold" | "release") {
        return Err(ApiError::bad("Allowed actions: cancel, hold, release"));
    }
    let _lock = s.mutations.lock().await;
    let mut j = s.db.get("jobs", &id, &u).await?;
    let state = j["state"].as_str().unwrap_or("");
    if terminal(state) || state == "SUBMITTING" || state == "SUBMISSION_UNKNOWN" {
        return Err(ApiError::conflict("Job cannot be changed in current state"));
    }
    if b.action == "hold" && !matches!(state, "PENDING" | "HELD")
        || b.action == "release" && state != "HELD"
    {
        return Err(ApiError::conflict(
            "Hold applies to pending jobs; release applies to held jobs",
        ));
    }
    let slurm_id = j["slurm_id"]
        .as_str()
        .ok_or_else(|| ApiError::conflict("No Slurm job ID"))?;
    s.slurm
        .action(j["cluster_id"].as_str().unwrap_or(""), slurm_id, &b.action)
        .await
        .map_err(|e| ApiError(StatusCode::BAD_GATEWAY, "slurm_error", e.to_string()))?;
    j["state"] = json!(match b.action.as_str() {
        "cancel" => "CANCELLED",
        "hold" => "HELD",
        _ => "PENDING",
    });
    j["updated_at"] = json!(now());
    s.db.put("jobs", &id, j["owner"].as_str().unwrap_or(""), &j)
        .await?;
    if let Some(run_id) = j["run_id"].as_str() {
        let mut r = s.db.get("runs", run_id, &u).await?;
        r["state"] = j["state"].clone();
        r["updated_at"] = j["updated_at"].clone();
        if b.action == "cancel" {
            r["finished_at"] = json!(now());
        }
        s.db.put("runs", run_id, r["owner"].as_str().unwrap_or(""), &r)
            .await?;
    }
    Ok(Json(j))
}

async fn files(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(s.db.list("files", &u).await?)))
}
async fn file(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    Ok(Json(s.db.get("files", &id, &u).await?))
}
async fn upload(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    mut multipart: Multipart,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let mut field = multipart
        .next_field()
        .await
        .map_err(|e| ApiError::bad(e.to_string()))?
        .ok_or_else(|| ApiError::bad("Multipart field 'file' required"))?;
    if field.name() != Some("file") {
        return Err(ApiError::bad("Multipart field must be named file"));
    }
    let filename = field
        .file_name()
        .unwrap_or("upload.bin")
        .chars()
        .filter(|c| !c.is_control() && *c != '/' && *c != '\\')
        .take(200)
        .collect::<String>();
    let id = Uuid::new_v4().to_string();
    let path = s.config.data_dir.join("uploads").join(&id);
    let mut out = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await?;
    let mut size = 0usize;
    let mut hash = Sha256::new();
    let transfer: ApiResult<()> = async {
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|e| ApiError::bad(e.to_string()))?
        {
            size = size.saturating_add(chunk.len());
            if size > s.config.max_upload_bytes {
                return Err(ApiError(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "payload_too_large",
                    "Upload exceeds configured limit".into(),
                ));
            }
            hash.update(&chunk);
            out.write_all(&chunk).await?;
        }
        out.flush().await?;
        Ok(())
    }
    .await;
    drop(field);
    drop(out);
    if let Err(e) = transfer {
        let _ = tokio::fs::remove_file(&path).await;
        return Err(e);
    }
    match multipart.next_field().await {
        Ok(None) => {}
        _ => {
            let _ = tokio::fs::remove_file(&path).await;
            return Err(ApiError::bad("Exactly one file per request"));
        }
    }
    let data = json!({"id":id,"owner":u.subject,"filename":filename,"path":path,"size":size,"sha256":format!("{:x}",hash.finalize()),"created_at":now()});
    if let Err(e) = s.db.put("files", &id, &u.subject, &data).await {
        let _ = tokio::fs::remove_file(&path).await;
        return Err(e);
    }
    Ok((StatusCode::CREATED, Json(data)))
}
async fn safe_stored_path(s: &AppState, value: &Value) -> ApiResult<PathBuf> {
    let path = tokio::fs::canonicalize(value.as_str().ok_or_else(ApiError::not_found)?)
        .await
        .map_err(|_| ApiError::not_found())?;
    if !path.starts_with(&s.config.data_dir) {
        return Err(ApiError::not_found());
    }
    Ok(path)
}
async fn download(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let data = s.db.get("files", &id, &u).await?;
    let path = safe_stored_path(&s, &data["path"]).await?;
    let f = tokio::fs::File::open(path).await?;
    let size = f.metadata().await?.len();
    let mut response = Response::new(axum::body::Body::from_stream(ReaderStream::new(f)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&size.to_string()).unwrap(),
    );
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{id}\"")).unwrap(),
    );
    Ok(response)
}
async fn preview(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let data = s.db.get("files", &id, &u).await?;
    let path = safe_stored_path(&s, &data["path"]).await?;
    let f = tokio::fs::File::open(path).await?;
    let size = f.metadata().await?.len();
    let mut bytes = vec![];
    f.take(65536).read_to_end(&mut bytes).await?;
    Ok(Json(
        json!({"id":id,"content":String::from_utf8_lossy(&bytes),"truncated":size>65536,"bytes":bytes.len(),"encoding":"utf-8-lossy"}),
    ))
}

async fn workflows(State(s): State<AppState>) -> Json<Value> {
    let templates = s.templates.read().await;
    let list = templates.values().collect::<Vec<_>>();
    Json(json!(list))
}
async fn template(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let templates = s.templates.read().await;
    Ok(Json(json!(templates
        .get(&id)
        .ok_or_else(ApiError::not_found)?)))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditTemplate {
    yaml: String,
}
async fn edit_template(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
    Json(b): Json<EditTemplate>,
) -> ApiResult<Json<Value>> {
    u.require_admin()?;
    if b.yaml.len() > 1024 * 1024 {
        return Err(ApiError::bad("Template exceeds 1 MiB"));
    }
    let t = workflow::parse_template(&b.yaml).map_err(|e| ApiError::bad(e.to_string()))?;
    if t.id != id {
        return Err(ApiError::bad("Template ID must match path"));
    }
    let _lock = s.mutations.lock().await;
    if s.templates
        .read()
        .await
        .get(&id)
        .is_some_and(|old| old.version == t.version)
    {
        return Err(ApiError::conflict(
            "Increment version before editing template",
        ));
    }
    if s.config.applications.iter().all(|a| a.id != t.application) {
        return Err(ApiError::bad("Unknown template application"));
    }
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM workflow_versions WHERE id=? AND version=?)",
    )
    .bind(&id)
    .bind(&t.version)
    .fetch_one(&s.db.0)
    .await?;
    if exists {
        return Err(ApiError::conflict(
            "Immutable template version already exists",
        ));
    }
    sqlx::query("INSERT INTO workflow_versions(id,version,yaml,created_at) VALUES(?,?,?,?)")
        .bind(&id)
        .bind(&t.version)
        .bind(&b.yaml)
        .bind(now())
        .execute(&s.db.0)
        .await?;
    s.templates.write().await.insert(id, t.clone());
    Ok(Json(json!(t)))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Deploy {
    #[serde(default)]
    inputs: HashMap<String, String>,
    #[serde(default = "empty_object")]
    parameters: Value,
    cluster_id: Option<String>,
}
fn empty_object() -> Value {
    json!({})
}
async fn deploy(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
    Json(b): Json<Deploy>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let t = s
        .templates
        .read()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(ApiError::not_found)?;
    let (cluster, partition) = resolve_target(&s, &t.application, b.cluster_id.as_deref()).await?;
    let mut resolved = serde_json::Map::new();
    for (name, file_id) in &b.inputs {
        let f = s.db.get("files", file_id, &u).await?;
        let path = safe_stored_path(&s, &f["path"]).await?;
        resolved.insert(name.clone(), json!(path));
    }
    let run_id = Uuid::new_v4().to_string();
    let dir = s.config.data_dir.join("runs").join(&run_id);
    let tc = t.clone();
    let inputs = Value::Object(resolved);
    let parameters = b.parameters.clone();
    let dc = dir.clone();
    let mock = s.slurm.mock();
    let deployment = tokio::task::spawn_blocking(move || {
        workflow::deploy(&tc, &inputs, &parameters, &dc, &partition, mock)
    })
    .await
    .map_err(anyhow::Error::from)?
    .map_err(|e| ApiError::bad(e.to_string()))?;
    let data = json!({"id":run_id,"owner":u.subject,"workflow_id":t.id,"workflow_version":t.version,"template":t,"inputs":b.inputs,"parameters":b.parameters,"deployment":deployment,"cluster_id":cluster,"run_dir":dir,"state":"DEPLOYED","job_id":null,"created_at":now(),"updated_at":now(),"started_at":null,"finished_at":null,"elapsed_seconds":null,"exit_code":null,"simulated":mock});
    s.db.put("runs", &run_id, &u.subject, &data).await?;
    Ok((StatusCode::CREATED, Json(data)))
}
async fn runs(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
) -> ApiResult<Json<Value>> {
    Ok(Json(json!(s.db.list("runs", &u).await?)))
}
async fn run(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    Ok(Json(s.db.get("runs", &id, &u).await?))
}
async fn execute(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let _lock = s.mutations.lock().await;
    let mut r = s.db.get("runs", &id, &u).await?;
    if r["state"] != "DEPLOYED" {
        return Err(ApiError::conflict(
            "Only DEPLOYED runs can execute; create a new deployment to rerun",
        ));
    }
    let script = safe_stored_path(&s, &r["deployment"]["script_path"]).await?;
    let job_id = Uuid::new_v4().to_string();
    r["state"] = json!("SUBMITTING");
    r["job_id"] = json!(job_id);
    r["started_at"] = json!(now());
    r["updated_at"] = json!(now());
    s.db.put("runs", &id, r["owner"].as_str().unwrap_or(""), &r)
        .await?;
    let owner = Principal {
        subject: r["owner"].as_str().unwrap_or("").into(),
        admin: u.admin,
    };
    let j = submit_record(
        &s,
        &owner,
        &job_id,
        r["cluster_id"].as_str().unwrap_or(""),
        r["template"]["application"].as_str().unwrap_or(""),
        r["workflow_id"].as_str().unwrap_or(""),
        &script,
        Some(&id),
    )
    .await?;
    r["state"] = j["state"].clone();
    r["job_id"] = json!(job_id);
    r["updated_at"] = json!(now());
    s.db.put("runs", &id, &owner.subject, &r).await?;
    Ok((StatusCode::ACCEPTED, Json(r)))
}
#[derive(Deserialize)]
struct LogQuery {
    #[serde(default)]
    offset: u64,
    #[serde(default = "log_limit")]
    limit: usize,
}
fn log_limit() -> usize {
    65536
}
async fn logs(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
    Query(q): Query<LogQuery>,
) -> ApiResult<Json<Value>> {
    let r = s.db.get("runs", &id, &u).await?;
    let dir = safe_stored_path(&s, &r["run_dir"]).await?;
    let limit = q.limit.min(65536);
    let path = dir.join("run.log");
    let mut data = vec![];
    let mut total = 0;
    match tokio::fs::File::open(&path).await {
        Ok(mut f) => {
            let canonical = tokio::fs::canonicalize(&path).await?;
            if !canonical.starts_with(&dir) {
                return Err(ApiError::not_found());
            }
            total = f.metadata().await?.len();
            f.seek(std::io::SeekFrom::Start(q.offset)).await?;
            f.take(limit as u64).read_to_end(&mut data).await?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    Ok(Json(
        json!({"run_id":id,"content":String::from_utf8_lossy(&data),"offset":q.offset,"next_offset":q.offset.saturating_add(data.len() as u64),"truncated":q.offset.saturating_add(data.len() as u64)<total,"simulated":s.slurm.mock()}),
    ))
}
async fn results(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let r = s.db.get("runs", &id, &u).await?;
    if !terminal(r["state"].as_str().unwrap_or("")) {
        return Err(ApiError::conflict("Results available after terminal state"));
    }
    let t: workflow::Template =
        serde_json::from_value(r["template"].clone()).map_err(anyhow::Error::from)?;
    let dir = safe_stored_path(&s, &r["run_dir"]).await?;
    let paths = workflow::output_paths(&t, &dir).map_err(ApiError::from)?;
    let _lock = s.mutations.lock().await;
    let mut items = vec![];
    for (name, path) in paths {
        let Ok(canonical) = tokio::fs::canonicalize(&path).await else {
            items.push(json!({"name":name,"exists":false}));
            continue;
        };
        if !canonical.starts_with(&dir) {
            return Err(ApiError::bad("Output escapes run directory"));
        }
        let metadata = tokio::fs::metadata(&canonical).await?;
        if !metadata.is_file() {
            continue;
        }
        let fid = format!("{}-{}", id, name);
        let f = json!({"id":fid,"owner":r["owner"],"filename":path.file_name().unwrap_or_default().to_string_lossy(),"path":canonical,"size":metadata.len(),"sha256":null,"created_at":now(),"run_id":id});
        s.db.put("files", &fid, r["owner"].as_str().unwrap_or(""), &f)
            .await?;
        items.push(json!({"name":name,"exists":true,"file_id":fid,"size":metadata.len(),"download_url":format!("/api/v1/files/{fid}/download")}));
    }
    Ok(Json(
        json!({"run_id":id,"state":r["state"],"simulated":s.slurm.mock(),"outputs":items}),
    ))
}
async fn validate(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let _lock = s.mutations.lock().await;
    let r = s.db.get("runs", &id, &u).await?;
    if !terminal(r["state"].as_str().unwrap_or("")) {
        return Err(ApiError::conflict("Validation requires a terminal run"));
    }
    let t: workflow::Template =
        serde_json::from_value(r["template"].clone()).map_err(anyhow::Error::from)?;
    let dir = safe_stored_path(&s, &r["run_dir"]).await?;
    let dc = dir.clone();
    let mock = s.slurm.mock() && r["state"] == "COMPLETED";
    let mut report = tokio::task::spawn_blocking(move || workflow::validate_results(&t, &dc, mock))
        .await
        .map_err(anyhow::Error::from)??;
    report["run_id"] = json!(id);
    report["job_state"] = r["state"].clone();
    report["exit_code"] = r["exit_code"].clone();
    report["inputs"] = r["inputs"].clone();
    report["parameters"] = r["parameters"].clone();
    report["resolved_parameters"] = r["deployment"]["snapshot"]["parameters"].clone();
    report["elapsed_seconds"] = r["elapsed_seconds"].clone();
    report["started_at"] = r["started_at"].clone();
    report["finished_at"] = r["finished_at"].clone();
    report["simulated"] = r["simulated"].clone();
    if r["state"] != "COMPLETED" || r["exit_code"] != "0:0" {
        report["passed"] = json!(false);
    }
    tokio::fs::write(
        dir.join("validation-report.json"),
        serde_json::to_vec_pretty(&report).map_err(anyhow::Error::from)?,
    )
    .await?;
    Ok(Json(report))
}
async fn report(
    State(s): State<AppState>,
    Extension(u): Extension<Principal>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let _lock = s.mutations.lock().await;
    let r = s.db.get("runs", &id, &u).await?;
    let dir = safe_stored_path(&s, &r["run_dir"]).await?;
    let bytes = tokio::fs::read(dir.join("validation-report.json"))
        .await
        .map_err(|_| ApiError::not_found())?;
    Ok(Json(
        serde_json::from_slice(&bytes).map_err(anyhow::Error::from)?,
    ))
}

async fn forward(
    State(s): State<AppState>,
    Path((service, path)): Path<(String, String)>,
    request: Request,
) -> ApiResult<Response> {
    if !matches!(
        request.method().as_str(),
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE"
    ) {
        return Err(ApiError(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "Unsupported gateway method".into(),
        ));
    }
    let upstream = s
        .config
        .upstreams
        .iter()
        .find(|u| u.id == service)
        .ok_or_else(ApiError::not_found)?;
    // Push each decoded segment: no user URL, dot segment, absolute URI, or redirect escape.
    let mut url = reqwest::Url::parse(&upstream.base_url).map_err(anyhow::Error::from)?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| ApiError::bad("Invalid upstream base URL"))?;
        segments.pop_if_empty();
        for part in path.split('/') {
            if matches!(part, "." | "..") || part.contains(['\\', '\0']) {
                return Err(ApiError::bad("Invalid proxy path"));
            }
            segments.push(part);
        }
    }
    url.set_query(request.uri().query());
    let mut outgoing = s.http.request(request.method().clone(), url);
    for name in [header::CONTENT_TYPE, header::ACCEPT] {
        if let Some(v) = request.headers().get(&name) {
            outgoing = outgoing.header(name, v);
        }
    }
    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                "Gateway body limit is 1 MiB".into(),
            )
        })?;
    let upstream = outgoing
        .body(body)
        .send()
        .await
        .map_err(|e| ApiError(StatusCode::BAD_GATEWAY, "upstream_error", e.to_string()))?;
    let status = upstream.status();
    let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let mut response = Response::new(axum::body::Body::from_stream(upstream.bytes_stream()));
    *response.status_mut() = status;
    if let Some(ct) = content_type {
        response.headers_mut().insert(header::CONTENT_TYPE, ct);
    }
    Ok(response)
}
