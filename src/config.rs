use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listen: String,
    pub data_dir: PathBuf,
    pub database_path: Option<PathBuf>,
    pub workflow_dir: PathBuf,
    pub mock_slurm: bool,
    pub poll_seconds: u64,
    pub command_timeout_seconds: u64,
    pub command_concurrency: usize,
    pub rate_per_key: u32,
    pub burst_per_key: u32,
    pub global_rate: u32,
    pub global_burst: u32,
    pub max_upload_bytes: usize,
    pub api_keys: Vec<ApiKey>,
    pub clusters: Vec<Cluster>,
    pub applications: Vec<Application>,
    pub upstreams: Vec<Upstream>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKey {
    pub subject: String,
    pub sha256: String,
    pub admin: bool,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    pub id: String,
    pub name: String,
    pub partitions: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Application {
    pub id: String,
    pub name: String,
    pub cluster_id: String,
    pub partitions: Vec<String>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    pub id: String,
    pub base_url: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8080".into(),
            data_dir: "data".into(),
            database_path: None,
            workflow_dir: "workflows".into(),
            mock_slurm: true,
            poll_seconds: 2,
            command_timeout_seconds: 15,
            command_concurrency: 8,
            rate_per_key: 200,
            burst_per_key: 400,
            global_rate: 1000,
            global_burst: 2000,
            max_upload_bytes: 64 * 1024 * 1024,
            api_keys: vec![],
            clusters: vec![Cluster {
                id: "local".into(),
                name: "Local Slurm".into(),
                partitions: vec!["cpu".into()],
            }],
            applications: vec![Application {
                id: "genomics".into(),
                name: "Genomics pipelines".into(),
                cluster_id: "local".into(),
                partitions: vec!["cpu".into()],
            }],
            upstreams: vec![],
        }
    }
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        let path = std::env::var("HPC_CONFIG").unwrap_or_else(|_| "config.example.json".into());
        let mut cfg: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        if let Ok(token) = std::env::var("HPC_API_TOKEN") {
            anyhow::ensure!(
                token.len() >= 16,
                "HPC_API_TOKEN must contain at least 16 characters"
            );
            use sha2::{Digest, Sha256};
            cfg.api_keys.push(ApiKey {
                subject: "local-admin".into(),
                sha256: format!("{:x}", Sha256::digest(token.as_bytes())),
                admin: true,
            });
        }
        cfg.validate()?;
        Ok(cfg)
    }
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.api_keys.is_empty(),
            "Set HPC_API_TOKEN or configure hashed api_keys"
        );
        anyhow::ensure!(
            self.command_concurrency > 0
                && self.poll_seconds > 0
                && self.command_timeout_seconds > 0,
            "Concurrency/timeouts must be positive"
        );
        anyhow::ensure!(
            self.rate_per_key > 0
                && self.burst_per_key > 0
                && self.global_rate > 0
                && self.global_burst > 0,
            "Rate limits must be positive"
        );
        for key in &self.api_keys {
            anyhow::ensure!(
                !key.subject.is_empty()
                    && key.sha256.len() == 64
                    && key.sha256.bytes().all(|b| b.is_ascii_hexdigit()),
                "Invalid API key hash/subject"
            );
        }
        for c in &self.clusters {
            anyhow::ensure!(
                identifier(&c.id)
                    && !c.partitions.is_empty()
                    && c.partitions.iter().all(|p| identifier(p)),
                "Invalid cluster/partition"
            );
        }
        for a in &self.applications {
            anyhow::ensure!(
                identifier(&a.id)
                    && self.clusters.iter().any(|c| c.id == a.cluster_id)
                    && !a.partitions.is_empty(),
                "Invalid application"
            );
        }
        for u in &self.upstreams {
            let url = reqwest::Url::parse(&u.base_url)?;
            anyhow::ensure!(
                identifier(&u.id)
                    && matches!(url.scheme(), "http" | "https")
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "Invalid upstream"
            );
        }
        Ok(())
    }
}

pub fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 80
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
        && !value.starts_with('-')
}
