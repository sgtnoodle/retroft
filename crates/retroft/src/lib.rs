//! Embedding API: [`daemon::Config`], [`daemon::Recorder`], [`daemon::run`], [`Shutdown`], and
//! [`StatusEvent`]. The caller owns signals, service-manager integration, and
//! status delivery. See `docs/embedding.md` for the supported boundary.
pub mod activation;
pub mod clock;
pub mod compression;
pub mod daemon;
pub mod storage;
pub mod tracefs;

pub mod status;
pub use readiness::Shutdown;
pub use status::StatusEvent;
pub mod trigger;

mod readiness;
