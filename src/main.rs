#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hpc_gateway=info,tower_http=info".into()),
        )
        .init();
    let config = hpc_gateway::config::Config::load()?;
    let listener = tokio::net::TcpListener::bind(&config.listen).await?;
    tracing::info!(listen=%config.listen,mock=config.mock_slurm,"HPC REST Gateway + Workflow Template Service");
    let state = hpc_gateway::initialize(config).await?;
    let polling = tokio::spawn(hpc_gateway::poll(state.clone()));
    axum::serve(listener, hpc_gateway::router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    polling.abort();
    Ok(())
}
