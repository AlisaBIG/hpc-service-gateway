use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use hpc_gateway::{
    config::{ApiKey, Config},
    AppState,
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

const ADMIN: &str = "admin-test-token-at-least-16";
const USER: &str = "other-test-token-at-least-16";
async fn setup() -> (tempfile::TempDir, AppState) {
    let temp = tempfile::tempdir().unwrap();
    let mut cfg = Config {
        data_dir: temp.path().join("data"),
        workflow_dir: std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("workflows"),
        ..Default::default()
    };
    cfg.api_keys = vec![
        ApiKey {
            subject: "admin".into(),
            sha256: format!("{:x}", Sha256::digest(ADMIN.as_bytes())),
            admin: true,
        },
        ApiKey {
            subject: "user".into(),
            sha256: format!("{:x}", Sha256::digest(USER.as_bytes())),
            admin: false,
        },
    ];
    let state = hpc_gateway::initialize(cfg).await.unwrap();
    (temp, state)
}
async fn request(
    state: &AppState,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let response = hpc_gateway::router(state.clone())
        .oneshot(
            req.body(if body.is_null() {
                Body::empty()
            } else {
                Body::from(body.to_string())
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
async fn upload(state: &AppState, token: &str) -> Value {
    let body="--bound\r\nContent-Disposition: form-data; name=\"file\"; filename=\"demo.fastq\"\r\nContent-Type: application/octet-stream\r\n\r\n@demo\nACGT\n+\nIIII\n\r\n--bound--\r\n";
    let response = hpc_gateway::router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/files")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "multipart/form-data; boundary=bound")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

#[tokio::test]
async fn authentication_ownership_and_privileges() {
    let (_temp, s) = setup().await;
    assert_eq!(
        request(&s, "GET", "/api/v1/jobs", None, Value::Null)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let f = upload(&s, ADMIN).await;
    let path = format!("/api/v1/files/{}", f["id"].as_str().unwrap());
    assert_eq!(
        request(&s, "GET", &path, Some(USER), Value::Null).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(
            &s,
            "GET",
            &format!("{path}/download"),
            Some(USER),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(
            &s,
            "PUT",
            "/api/v1/applications/genomics/cluster",
            Some(USER),
            json!({"cluster_id":"local"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(
            &s,
            "POST",
            "/api/v1/jobs",
            Some(USER),
            json!({"application_id":"genomics","name":"test","script":"#!/bin/bash\necho test\n"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn two_workflows_deploy_execute_validate_and_preserve_evidence() {
    let (_temp, s) = setup().await;
    let templates = request(&s, "GET", "/api/v1/workflows", Some(USER), Value::Null)
        .await
        .1;
    assert!(templates.as_array().unwrap().len() >= 2);
    let mut ids = vec![];
    for t in templates.as_array().unwrap() {
        let mut inputs = serde_json::Map::new();
        for (name, _) in t["inputs"].as_object().unwrap() {
            inputs.insert(name.clone(), upload(&s, USER).await["id"].clone());
        }
        let path = format!("/api/v1/workflows/{}/deploy", t["id"].as_str().unwrap());
        let (status, r) = request(
            &s,
            "POST",
            &path,
            Some(USER),
            json!({"inputs":inputs,"parameters":{"threads":2}}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{r}");
        let id = r["id"].as_str().unwrap().to_string();
        let path = format!("/api/v1/runs/{id}/execute");
        let (a, b) = tokio::join!(
            request(&s, "POST", &path, Some(USER), Value::Null),
            request(&s, "POST", &path, Some(USER), Value::Null)
        );
        assert!(matches!(
            (a.0, b.0),
            (StatusCode::ACCEPTED, StatusCode::CONFLICT)
                | (StatusCode::CONFLICT, StatusCode::ACCEPTED)
        ));
        ids.push(id);
    }
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    hpc_gateway::poll_once(&s).await.map_err(|e| e.2).unwrap();
    for id in ids {
        let r = request(
            &s,
            "GET",
            &format!("/api/v1/runs/{id}"),
            Some(USER),
            Value::Null,
        )
        .await
        .1;
        assert_eq!(r["state"], "COMPLETED", "{r}");
        let (status, report) = request(
            &s,
            "POST",
            &format!("/api/v1/runs/{id}/validate"),
            Some(USER),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{report}");
        assert_eq!(report["passed"], true, "{report}");
        assert_eq!(report["simulated"], true);
        let (_, out) = request(
            &s,
            "GET",
            &format!("/api/v1/runs/{id}/results"),
            Some(USER),
            Value::Null,
        )
        .await;
        assert!(
            out["outputs"]
                .as_array()
                .unwrap()
                .iter()
                .all(|o| o["exists"] == true),
            "{out}"
        );
        let report2 = request(
            &s,
            "GET",
            &format!("/api/v1/runs/{id}/report"),
            Some(USER),
            Value::Null,
        )
        .await
        .1;
        assert_eq!(report, report2);
        // Deleting a required VCF must invalidate a report instead of recreating it.
        let template: hpc_gateway::workflow::Template =
            serde_json::from_value(r["template"].clone()).unwrap();
        let paths = hpc_gateway::workflow::output_paths(
            &template,
            std::path::Path::new(r["run_dir"].as_str().unwrap()),
        )
        .unwrap();
        tokio::fs::remove_file(paths.values().next().unwrap())
            .await
            .unwrap();
        let (_, failed) = request(
            &s,
            "POST",
            &format!("/api/v1/runs/{id}/validate"),
            Some(USER),
            Value::Null,
        )
        .await;
        assert_eq!(failed["passed"], false, "{failed}");
    }
}

#[tokio::test]
async fn openapi_and_resource_bindings_are_available() {
    let (_temp, s) = setup().await;
    let (status, spec) = request(&s, "GET", "/api-docs/openapi.json", None, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert!(spec["paths"]["/api/v1/runs/{id}/execute"].is_object());
    let (status, error) = request(
        &s,
        "PUT",
        "/api/v1/applications/genomics/cluster",
        Some(ADMIN),
        json!({"cluster_id":"missing"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
}

#[tokio::test]
async fn job_hold_survives_refresh_then_release_and_cancel() {
    let (_temp, s) = setup().await;
    let (status, j) = request(
        &s,
        "POST",
        "/api/v1/jobs",
        Some(ADMIN),
        json!({"application_id":"genomics","name":"state-test","script":"#!/bin/bash\necho ok\n"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{j}");
    let path = format!("/api/v1/jobs/{}/state", j["id"].as_str().unwrap());
    let held = request(&s, "PATCH", &path, Some(ADMIN), json!({"action":"hold"})).await;
    assert_eq!(held.0, StatusCode::OK, "{}", held.1);
    hpc_gateway::poll_once(&s).await.map_err(|e| e.2).unwrap();
    let current = request(
        &s,
        "GET",
        &format!("/api/v1/jobs/{}", j["id"].as_str().unwrap()),
        Some(ADMIN),
        Value::Null,
    )
    .await
    .1;
    assert_eq!(current["state"], "HELD");
    let released = request(&s, "PATCH", &path, Some(ADMIN), json!({"action":"release"})).await;
    assert_eq!(released.0, StatusCode::OK);
    assert_eq!(released.1["state"], "PENDING");
    let cancelled = request(&s, "PATCH", &path, Some(ADMIN), json!({"action":"cancel"})).await;
    assert_eq!(cancelled.0, StatusCode::OK);
    assert_eq!(cancelled.1["state"], "CANCELLED");
    assert_eq!(
        request(&s, "PATCH", &path, Some(ADMIN), json!({"action":"release"}))
            .await
            .0,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn template_versions_are_immutable_and_survive_restart() {
    let (_temp, s) = setup().await;
    let mut t = s.templates.read().await.get("germline").unwrap().clone();
    let original = t.clone();
    t.version = "1.0.1".into();
    let body = json!({"yaml":serde_yaml::to_string(&t).unwrap()});
    let path = "/api/v1/workflows/germline";
    assert_eq!(
        request(&s, "PUT", path, Some(USER), body.clone()).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(&s, "PUT", path, Some(ADMIN), body.clone()).await.0,
        StatusCode::OK
    );
    assert_eq!(
        request(&s, "PUT", path, Some(ADMIN), body).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        request(
            &s,
            "PUT",
            path,
            Some(ADMIN),
            json!({"yaml":serde_yaml::to_string(&original).unwrap()})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let restarted = hpc_gateway::initialize((*s.config).clone()).await.unwrap();
    assert_eq!(
        restarted
            .templates
            .read()
            .await
            .get("germline")
            .unwrap()
            .version,
        "1.0.1"
    );
}

#[tokio::test]
async fn proxy_preserves_path_and_query_but_strips_credentials_and_redirects() {
    let (_temp, mut s) = setup().await;
    let upstream=axum::Router::new().route("/base/inspect/{*path}",axum::routing::any(|request:axum::extract::Request|async move {
        axum::Json(json!({"path":request.uri().path(),"query":request.uri().query(),"authorization":request.headers().get("authorization").is_some(),"cookie":request.headers().get("cookie").is_some()}))
    })).route("/base/redirect",axum::routing::get(||async{(StatusCode::FOUND,[("location","http://127.0.0.1:1/never-follow")])}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let mut cfg = (*s.config).clone();
    cfg.upstreams = vec![hpc_gateway::config::Upstream {
        id: "test".into(),
        base_url: format!("http://{addr}/base/"),
    }];
    s.config = std::sync::Arc::new(cfg);
    let response = hpc_gateway::router(s.clone())
        .oneshot(
            Request::builder()
                .uri("/api/v1/gateway/test/inspect/a%20b?item=1&item=2")
                .header("authorization", format!("Bearer {USER}"))
                .header("cookie", "secret=do-not-forward")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["path"], "/base/inspect/a%20b");
    assert_eq!(body["query"], "item=1&item=2");
    assert_eq!(body["authorization"], false);
    assert_eq!(body["cookie"], false);
    assert_eq!(
        request(
            &s,
            "GET",
            "/api/v1/gateway/test/redirect",
            Some(USER),
            Value::Null
        )
        .await
        .0,
        StatusCode::FOUND
    );
    assert_eq!(
        request(
            &s,
            "GET",
            "/api/v1/gateway/unknown/inspect",
            Some(USER),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    server.abort();
}
