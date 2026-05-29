use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
static CLEANUP_DONE: AtomicBool = AtomicBool::new(false);

pub fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}

pub fn notify_cleanup_done() {
    CLEANUP_DONE.store(true, Ordering::SeqCst);
}

#[cfg(windows)]
pub fn install() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| unsafe {
        windows_sys::Win32::System::Console::SetConsoleCtrlHandler(Some(ctrl_handler), 1);
    });
}

#[cfg(windows)]
const CTRL_CLOSE_EVENT: u32 = 2;
#[cfg(windows)]
const CTRL_LOGOFF_EVENT: u32 = 5;
#[cfg(windows)]
const CTRL_SHUTDOWN_EVENT: u32 = 6;

#[cfg(windows)]
unsafe extern "system" fn ctrl_handler(ctrl_type: u32) -> i32 {
    match ctrl_type {
        CTRL_CLOSE_EVENT | CTRL_LOGOFF_EVENT | CTRL_SHUTDOWN_EVENT => {
            log::info!(target: "pasdns::signal", "received windows shutdown signal ({ctrl_type})");
            SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);

            if ctrl_type == CTRL_SHUTDOWN_EVENT || ctrl_type == CTRL_LOGOFF_EVENT {
                let start = std::time::Instant::now();
                while !CLEANUP_DONE.load(Ordering::SeqCst) {
                    if start.elapsed() > Duration::from_secs(5) {
                        log::warn!(target: "pasdns::signal", "cleanup timeout, forcing exit");
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
            1
        }
        _ => 0,
    }
}

#[cfg(not(windows))]
pub fn install() {}

pub async fn wait_for_shutdown() {
    #[cfg(windows)]
    {
        while !SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[cfg(not(windows))]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        sigterm.recv().await;
    }
}
