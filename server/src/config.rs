use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub(crate) struct AppConfig {
    pub(crate) bind: SocketAddr,
    pub(crate) max_dimension: usize,
    pub(crate) strict_finite: bool,
    pub(crate) request_timeout_ms: u64,
    pub(crate) max_body_bytes: usize,
    pub(crate) max_concurrency: usize,
    pub(crate) persistence_enabled: bool,
    pub(crate) snapshot_path: PathBuf,
    pub(crate) wal_path: PathBuf,
}

impl AppConfig {
    pub(crate) fn from_env() -> Result<Self> {
        let bind = parse_socket_addr("AIONBD_BIND", "127.0.0.1:8080")?;
        let max_dimension = parse_usize("AIONBD_MAX_DIMENSION", 4096)?;
        let strict_finite = parse_bool("AIONBD_STRICT_FINITE", true)?;
        let request_timeout_ms = parse_u64("AIONBD_REQUEST_TIMEOUT_MS", 2000)?;
        let max_body_bytes = parse_usize("AIONBD_MAX_BODY_BYTES", 1_048_576)?;
        let max_concurrency = parse_usize("AIONBD_MAX_CONCURRENCY", 256)?;
        let persistence_enabled = parse_bool("AIONBD_PERSISTENCE_ENABLED", true)?;
        let snapshot_path = parse_path("AIONBD_SNAPSHOT_PATH", "data/aionbd_snapshot.json")?;
        let wal_path = parse_path("AIONBD_WAL_PATH", "data/aionbd_wal.jsonl")?;

        if max_dimension == 0 {
            anyhow::bail!("AIONBD_MAX_DIMENSION must be > 0");
        }
        if max_body_bytes == 0 {
            anyhow::bail!("AIONBD_MAX_BODY_BYTES must be > 0");
        }
        if max_concurrency == 0 {
            anyhow::bail!("AIONBD_MAX_CONCURRENCY must be > 0");
        }

        Ok(Self {
            bind,
            max_dimension,
            strict_finite,
            request_timeout_ms,
            max_body_bytes,
            max_concurrency,
            persistence_enabled,
            snapshot_path,
            wal_path,
        })
    }
}

fn parse_socket_addr(key: &str, default: &str) -> Result<SocketAddr> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse()
        .with_context(|| format!("{key} must be a valid socket address, got '{raw}'"))
}

fn parse_usize(key: &str, default: usize) -> Result<usize> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse()
        .with_context(|| format!("{key} must be a positive integer, got '{raw}'"))
}

fn parse_u64(key: &str, default: u64) -> Result<u64> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse()
        .with_context(|| format!("{key} must be a positive integer, got '{raw}'"))
}

fn parse_bool(key: &str, default: bool) -> Result<bool> {
    let raw = env::var(key).unwrap_or_else(|_| {
        if default {
            "true".to_string()
        } else {
            "false".to_string()
        }
    });

    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("{key} must be a boolean, got '{raw}'"),
    }
}

fn parse_path(key: &str, default: &str) -> Result<PathBuf> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    let path = PathBuf::from(raw.clone());
    if path.as_os_str().is_empty() {
        anyhow::bail!("{key} must not be empty");
    }
    Ok(path)
}
