//! One bounded, shell-free adapter for the five Slurm commands used by the API.
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    process::Command,
    sync::{Mutex, Semaphore},
    time::timeout,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    pub state: String,
    pub exit_code: Option<String>,
    pub elapsed_seconds: Option<i64>,
}

#[derive(Clone)]
pub struct SlurmClient {
    simulated: bool,
    timeout: Duration,
    semaphore: Arc<Semaphore>,
    jobs: Arc<Mutex<HashMap<String, MockJob>>>,
}

struct MockJob {
    created: Instant,
    held: bool,
    cancelled: bool,
}

impl SlurmClient {
    pub fn new(mock: bool, command_timeout_secs: u64, max_concurrency: usize) -> Self {
        Self {
            simulated: mock,
            timeout: Duration::from_secs(command_timeout_secs.max(1)),
            semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
            jobs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn mock(&self) -> bool {
        self.simulated
    }

    async fn command(&self, binary: &str, args: &[&str]) -> Result<String> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .context("Slurm command queue closed")?;
        let mut command = Command::new(binary);
        command.args(args).kill_on_drop(true);
        let output = timeout(self.timeout, command.output())
            .await
            .with_context(|| format!("{binary} exceeded {} seconds", self.timeout.as_secs()))?
            .with_context(|| format!("could not start {binary}"))?;
        if !output.status.success() {
            bail!(
                "{binary} failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        String::from_utf8(output.stdout).context("Slurm returned non-UTF-8 output")
    }

    pub async fn submit(&self, cluster: &str, script: &Path) -> Result<String> {
        validate_identifier(cluster)?;
        if !script.is_file() {
            bail!("Slurm script does not exist");
        }
        if self.simulated {
            // Numeric IDs fit the same route validation as real Slurm job IDs.
            let id = format!(
                "{}",
                uuid::Uuid::new_v4().as_u128() % 9_000_000_000_000_u128 + 1_000_000_000_000_u128
            );
            self.jobs.lock().await.insert(
                format!("{cluster}:{id}"),
                MockJob {
                    created: Instant::now(),
                    held: false,
                    cancelled: false,
                },
            );
            return Ok(id);
        }
        let script = script.to_str().context("script path is not UTF-8")?;
        let output = self
            .command("sbatch", &["--parsable", "--clusters", cluster, script])
            .await?;
        let id = output.trim().split(';').next().unwrap_or("");
        validate_job_id(id).context("unexpected sbatch --parsable output")?;
        Ok(id.to_owned())
    }

    pub async fn query(&self, cluster: &str, id: &str) -> Result<JobInfo> {
        validate_identifier(cluster)?;
        validate_job_id(id)?;
        if self.simulated {
            let jobs = self.jobs.lock().await;
            let Some(job) = jobs.get(&format!("{cluster}:{id}")) else {
                // A restart loses mock processes. Never turn an unknown job into a success.
                return Ok(JobInfo {
                    state: "UNKNOWN".into(),
                    exit_code: None,
                    elapsed_seconds: None,
                });
            };
            let elapsed = job.created.elapsed().as_secs() as i64;
            let state = if job.cancelled {
                "CANCELLED"
            } else if job.held {
                "HELD"
            } else if elapsed >= 3 {
                "COMPLETED"
            } else if elapsed >= 1 {
                "RUNNING"
            } else {
                "PENDING"
            };
            return Ok(JobInfo {
                state: state.into(),
                exit_code: (state == "COMPLETED").then(|| "0:0".into()),
                elapsed_seconds: Some(elapsed),
            });
        }
        // squeue may reject a recently completed/purged ID. Accounting is still authoritative.
        let live = self
            .command(
                "squeue",
                &[
                    "--noheader",
                    "--clusters",
                    cluster,
                    "--jobs",
                    id,
                    "--format=%T|%r",
                ],
            )
            .await;
        if let Ok(output) = &live {
            if let Some(info) = parse_squeue(output) {
                // A terminal queue entry has no ExitCode: resolve accounting before persisting it.
                if !terminal_state(&info.state) {
                    return Ok(info);
                }
            }
        }
        // --allocations suppresses .batch/.extern steps: only the parent job determines success.
        let history = self
            .command(
                "sacct",
                &[
                    "--noheader",
                    "--parsable2",
                    "--allocations",
                    "--clusters",
                    cluster,
                    "--jobs",
                    id,
                    "--format=State%40,ExitCode,ElapsedRaw",
                ],
            )
            .await
            .map_err(|error| match &live {
                Err(live_error) => {
                    anyhow::anyhow!("sacct failed: {error}; squeue also failed: {live_error}")
                }
                Ok(_) => error,
            })?;
        if let Some(info) = parse_sacct(&history) {
            return Ok(info);
        }
        Ok(JobInfo {
            state: "UNKNOWN".into(),
            exit_code: None,
            elapsed_seconds: None,
        })
    }

    pub async fn action(&self, cluster: &str, id: &str, action: &str) -> Result<()> {
        validate_identifier(cluster)?;
        validate_job_id(id)?;
        if !matches!(action, "cancel" | "hold" | "release") {
            bail!("supported job actions: cancel, hold, release");
        }
        if self.simulated {
            let mut jobs = self.jobs.lock().await;
            let job = jobs
                .get_mut(&format!("{cluster}:{id}"))
                .context("mock job no longer exists")?;
            if job.cancelled || (!job.held && job.created.elapsed().as_secs() >= 3) {
                bail!("job is already terminal");
            }
            match action {
                "cancel" => job.cancelled = true,
                "hold" => {
                    if job.created.elapsed().as_secs() >= 1 {
                        bail!("only a pending job can be held");
                    }
                    job.held = true;
                }
                "release" => {
                    job.held = false;
                    job.created = Instant::now();
                }
                _ => unreachable!(),
            }
            return Ok(());
        }
        match action {
            "cancel" => {
                self.command("scancel", &["--clusters", cluster, id])
                    .await?;
            }
            "hold" | "release" => {
                self.command("scontrol", &["--clusters", cluster, action, id])
                    .await?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

pub fn validate_identifier(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || value.starts_with('-')
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
    {
        bail!("invalid Slurm cluster or partition identifier");
    }
    Ok(())
}

fn validate_job_id(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 20 || !value.bytes().all(|c| c.is_ascii_digit()) {
        bail!("invalid Slurm job id");
    }
    Ok(())
}

fn normalize_state(state: &str) -> String {
    state
        .trim()
        .split_whitespace()
        .next()
        .unwrap_or("UNKNOWN")
        .trim_end_matches('+')
        .to_ascii_uppercase()
}

fn known_state(state: &str) -> bool {
    matches!(
        state,
        "PENDING"
            | "RUNNING"
            | "COMPLETING"
            | "COMPLETED"
            | "CANCELLED"
            | "FAILED"
            | "TIMEOUT"
            | "NODE_FAIL"
            | "PREEMPTED"
            | "BOOT_FAIL"
            | "DEADLINE"
            | "OUT_OF_MEMORY"
            | "SUSPENDED"
            | "REQUEUED"
            | "REQUEUE_FED"
            | "REQUEUE_HOLD"
            | "RECONFIG_FAIL"
            | "RESIZING"
            | "REVOKED"
            | "SIGNALING"
            | "STAGE_OUT"
            | "STOPPED"
            | "SPECIAL_EXIT"
            | "CONFIGURING"
            | "POWER_UP_NODE"
            | "UNKNOWN"
            | "HELD"
    )
}

fn terminal_state(state: &str) -> bool {
    matches!(
        state,
        "COMPLETED"
            | "CANCELLED"
            | "FAILED"
            | "TIMEOUT"
            | "NODE_FAIL"
            | "PREEMPTED"
            | "BOOT_FAIL"
            | "DEADLINE"
            | "OUT_OF_MEMORY"
            | "REVOKED"
    )
}

fn parse_squeue(output: &str) -> Option<JobInfo> {
    for row in output.lines() {
        let mut columns = row.trim().split('|');
        let mut state = normalize_state(columns.next().unwrap_or(""));
        if !known_state(&state) {
            continue;
        } // Multi-cluster output can include CLUSTER: headers.
        if state == "PENDING"
            && columns
                .next()
                .is_some_and(|reason| reason.trim().starts_with("JobHeld"))
        {
            state = "HELD".into();
        }
        return Some(JobInfo {
            state,
            exit_code: None,
            elapsed_seconds: None,
        });
    }
    None
}

fn parse_sacct(output: &str) -> Option<JobInfo> {
    for row in output.lines() {
        let mut columns = row.trim().split('|');
        let state = normalize_state(columns.next().unwrap_or(""));
        if !known_state(&state) {
            continue;
        }
        return Some(JobInfo {
            state,
            exit_code: columns
                .next()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
            elapsed_seconds: columns.next().and_then(|s| s.trim().parse().ok()),
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_option_injection() {
        assert!(validate_identifier("--help").is_err());
        assert!(validate_identifier("local\n#SBATCH --exclusive").is_err());
        assert!(validate_job_id("12;touch /tmp/x").is_err());
        assert_eq!(normalize_state("CANCELLED by 1001"), "CANCELLED");
    }
    #[test]
    fn parses_cluster_headers_held_jobs_and_allocation_accounting() {
        let held = parse_squeue("CLUSTER: local\nPENDING|JobHeldUser\n").unwrap();
        assert_eq!(held.state, "HELD");
        assert!(parse_squeue("CLUSTER: local\n").is_none());
        assert!(parse_squeue("unrecognized output").is_none());
        assert!(terminal_state(
            &parse_squeue("COMPLETED|None\n").unwrap().state
        ));
        let complete = parse_sacct("CLUSTER: local\nCOMPLETED|0:0|123|\n").unwrap();
        assert_eq!(complete.state, "COMPLETED");
        assert_eq!(complete.exit_code.as_deref(), Some("0:0"));
        assert_eq!(complete.elapsed_seconds, Some(123));
        assert_eq!(
            parse_sacct("CANCELLED by 1001|0:15|2|\n").unwrap().state,
            "CANCELLED"
        );
    }
    #[tokio::test]
    async fn restart_does_not_report_mock_success() {
        let client = SlurmClient::new(true, 5, 2);
        let info = client.query("local", "123").await.unwrap();
        assert_eq!(info.state, "UNKNOWN");
        assert_eq!(info.exit_code, None);
    }
}
