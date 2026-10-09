//! Independent offline tools for recorder captures.
//!
//! This crate reads the shared container format without depending on the
//! recorder or its Linux tracing and io_uring implementation.
pub mod capture;
pub mod offline;
