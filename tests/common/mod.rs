//! Shared test utilities for kcp-io integration tests.
//!
//! This module provides common helpers used across all test files in this
//! directory. It is a private module (not compiled as a standalone test).

use kcp_io::tokio_rt::KcpSessionConfig;
use std::time::Duration;

/// Returns a session config suitable for fast-running integration tests.
///
/// Uses a 5-second timeout so tests fail quickly rather than hanging.
pub fn test_config() -> KcpSessionConfig {
    KcpSessionConfig {
        timeout: Some(Duration::from_secs(5)),
        ..KcpSessionConfig::fast()
    }
}
