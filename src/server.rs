use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};

use crate::{
    api::{revive_and_publish, router},
    repository::SqliteRepository,
};

pub async fn run(bind: SocketAddr, database_url: &str) -> Result<()> {
    let repository = Arc::new(
        SqliteRepository::new(database_url)
            .with_context(|| format!("failed to open SQLite database '{database_url}'"))?,
    );
    let (app, state) = router(repository);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(error) = revive_and_publish(&state).await {
                eprintln!("failed to revive due tasks: {error:?}");
            }
        }
    });

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind server to {bind}"))?;
    println!("gtd server listening on http://{bind}");
    println!("SQLite database: {database_url}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("HTTP server failed")
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
