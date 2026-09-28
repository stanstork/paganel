use std::{
    process,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Exit status for a run ended by SIGINT (128 + 2), as the shell expects.
const EXIT_SIGINT: i32 = 130;
/// Exit status for a run ended by SIGTERM (128 + 15).
const EXIT_SIGTERM: i32 = 143;

/// Shutdown coordinator that listens for SIGINT and SIGTERM signals
/// and triggers a graceful shutdown of the migration system.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    cancel_token: CancellationToken,
    pause_token: CancellationToken,
    shutdown_requested: Arc<AtomicBool>,
    pause_requested: Arc<AtomicBool>,
}

impl ShutdownCoordinator {
    pub fn new(cancel_token: CancellationToken, pause_token: CancellationToken) -> Self {
        Self {
            cancel_token,
            pause_token,
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            pause_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn register_handlers(&self, can_pause: bool) {
        // SIGINT: first = pause, second = force stop
        {
            let cancel_token = self.cancel_token.clone();
            let pause_token = self.pause_token.clone();
            let shutdown_flag = self.shutdown_requested.clone();
            let pause_flag = self.pause_requested.clone();

            tokio::spawn(async move {
                signal::ctrl_c()
                    .await
                    .expect("Failed to install SIGINT handler");

                if !can_pause {
                    info!("received SIGINT, stopping");
                    shutdown_flag.store(true, Ordering::SeqCst);
                    cancel_token.cancel();
                    process::exit(EXIT_SIGINT);
                }

                info!("received SIGINT, pausing migration (press Ctrl+C again to force stop)");
                pause_flag.store(true, Ordering::SeqCst);
                pause_token.cancel();

                signal::ctrl_c()
                    .await
                    .expect("Failed to install SIGINT handler");
                info!("received second SIGINT, forcing shutdown");
                shutdown_flag.store(true, Ordering::SeqCst);
                cancel_token.cancel();
                process::exit(EXIT_SIGINT);
            });
        }

        // SIGTERM: always force-stops
        #[cfg(unix)]
        {
            let cancel_token = self.cancel_token.clone();
            let shutdown_flag = self.shutdown_requested.clone();

            tokio::spawn(async move {
                let mut term = signal::unix::signal(signal::unix::SignalKind::terminate())
                    .expect("Failed to install SIGTERM handler");
                term.recv().await;
                info!("received SIGTERM, forcing shutdown");
                shutdown_flag.store(true, Ordering::SeqCst);
                cancel_token.cancel();
                process::exit(EXIT_SIGTERM);
            });
        }
    }
}
