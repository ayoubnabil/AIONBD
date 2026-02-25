use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub(crate) struct AppConfig {
    pub(crate) bind: SocketAddr,
    pub(crate) max_dimension: usize,
    pub(crate) max_points_per_collection: usize,
    pub(crate) strict_finite: bool,
    pub(crate) request_timeout_ms: u64,
    pub(crate) max_body_bytes: usize,
    pub(crate) max_concurrency: usize,
    pub(crate) max_page_limit: usize,
    pub(crate) max_topk_limit: usize,
    pub(crate) checkpoint_interval: usize,
    pub(crate) persistence_enabled: bool,
    pub(crate) wal_sync_on_write: bool,
    pub(crate) snapshot_path: PathBuf,
    pub(crate) wal_path: PathBuf,
}

impl AppConfig {
    pub(crate) fn from_env() -> Result<Self> {
        let bind = parse_socket_addr("AIONBD_BIND", "127.0.0.1:8080")?;
        let max_dimension = parse_usize("AIONBD_MAX_DIMENSION", 4096)?;
        let max_points_per_collection = parse_usize("AIONBD_MAX_POINTS_PER_COLLECTION", 1_000_000)?;
        let strict_finite = parse_bool("AIONBD_STRICT_FINITE", true)?;
        let request_timeout_ms = parse_u64("AIONBD_REQUEST_TIMEOUT_MS", 2000)?;
        let max_body_bytes = parse_usize("AIONBD_MAX_BODY_BYTES", 1_048_576)?;
        let max_concurrency = parse_usize("AIONBD_MAX_CONCURRENCY", 256)?;
        let max_page_limit = parse_usize("AIONBD_MAX_PAGE_LIMIT", 1000)?;
        let max_topk_limit = parse_usize("AIONBD_MAX_TOPK_LIMIT", 1000)?;
        let checkpoint_interval = parse_usize("AIONBD_CHECKPOINT_INTERVAL", 32)?;
        let persistence_enabled = parse_bool("AIONBD_PERSISTENCE_ENABLED", true)?;
        let wal_sync_on_write = parse_bool("AIONBD_WAL_SYNC_ON_WRITE", true)?;
        let snapshot_path = parse_path("AIONBD_SNAPSHOT_PATH", "data/aionbd_snapshot.json")?;
        let wal_path = parse_path("AIONBD_WAL_PATH", "data/aionbd_wal.jsonl")?;

        if max_dimension == 0 {
            anyhow::bail!("AIONBD_MAX_DIMENSION must be > 0");
        }
        if max_body_bytes == 0 {
            anyhow::bail!("AIONBD_MAX_BODY_BYTES must be > 0");
        }
        if max_points_per_collection == 0 {
            anyhow::bail!("AIONBD_MAX_POINTS_PER_COLLECTION must be > 0");
        }
        if max_concurrency == 0 {
            anyhow::bail!("AIONBD_MAX_CONCURRENCY must be > 0");
        }
        if max_page_limit == 0 {
            anyhow::bail!("AIONBD_MAX_PAGE_LIMIT must be > 0");
        }
        if max_topk_limit == 0 {
            anyhow::bail!("AIONBD_MAX_TOPK_LIMIT must be > 0");
        }
        if checkpoint_interval == 0 {
            anyhow::bail!("AIONBD_CHECKPOINT_INTERVAL must be > 0");
        }

        Ok(Self {
            bind,
            max_dimension,
            max_points_per_collection,
            strict_finite,
            request_timeout_ms,
            max_body_bytes,
            max_concurrency,
            max_page_limit,
            max_topk_limit,
            checkpoint_interval,
            persistence_enabled,
            wal_sync_on_write,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    const CONFIG_KEYS: &[&str] = &[
        "AIONBD_BIND",
        "AIONBD_MAX_DIMENSION",
        "AIONBD_MAX_POINTS_PER_COLLECTION",
        "AIONBD_STRICT_FINITE",
        "AIONBD_REQUEST_TIMEOUT_MS",
        "AIONBD_MAX_BODY_BYTES",
        "AIONBD_MAX_CONCURRENCY",
        "AIONBD_MAX_PAGE_LIMIT",
        "AIONBD_MAX_TOPK_LIMIT",
        "AIONBD_CHECKPOINT_INTERVAL",
        "AIONBD_PERSISTENCE_ENABLED",
        "AIONBD_WAL_SYNC_ON_WRITE",
        "AIONBD_SNAPSHOT_PATH",
        "AIONBD_WAL_PATH",
    ];

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvGuard {
        saved: Vec<(String, Option<String>)>,
    }

    impl EnvGuard {
        fn capture(keys: &[&str]) -> Self {
            let saved = keys
                .iter()
                .map(|key| ((*key).to_string(), env::var(key).ok()))
                .collect();
            Self { saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in &self.saved {
                if let Some(value) = value {
                    env::set_var(key, value);
                } else {
                    env::remove_var(key);
                }
            }
        }
    }

    fn with_env<R>(pairs: &[(&str, &str)], f: impl FnOnce() -> R) -> R {
        let _lock = env_lock().lock().expect("env test mutex must be lockable");
        let _guard = EnvGuard::capture(CONFIG_KEYS);

        for key in CONFIG_KEYS {
            env::remove_var(key);
        }
        for (key, value) in pairs {
            env::set_var(key, value);
        }

        f()
    }

    #[test]
    fn from_env_uses_expected_defaults() {
        let config = with_env(&[], || {
            AppConfig::from_env().expect("default config must parse")
        });
        assert_eq!(
            config.bind,
            "127.0.0.1:8080"
                .parse::<SocketAddr>()
                .expect("default bind should parse")
        );
        assert_eq!(config.max_dimension, 4096);
        assert_eq!(config.max_points_per_collection, 1_000_000);
        assert!(config.strict_finite);
        assert_eq!(config.request_timeout_ms, 2000);
        assert_eq!(config.max_body_bytes, 1_048_576);
        assert_eq!(config.max_concurrency, 256);
        assert_eq!(config.max_page_limit, 1000);
        assert_eq!(config.max_topk_limit, 1000);
        assert_eq!(config.checkpoint_interval, 32);
        assert!(config.persistence_enabled);
        assert!(config.wal_sync_on_write);
        assert_eq!(
            config.snapshot_path,
            PathBuf::from("data/aionbd_snapshot.json")
        );
        assert_eq!(config.wal_path, PathBuf::from("data/aionbd_wal.jsonl"));
    }

    #[test]
    fn from_env_applies_overrides() {
        let config = with_env(
            &[
                ("AIONBD_BIND", "0.0.0.0:9090"),
                ("AIONBD_MAX_DIMENSION", "128"),
                ("AIONBD_MAX_POINTS_PER_COLLECTION", "2048"),
                ("AIONBD_STRICT_FINITE", "false"),
                ("AIONBD_REQUEST_TIMEOUT_MS", "5000"),
                ("AIONBD_MAX_BODY_BYTES", "2048"),
                ("AIONBD_MAX_CONCURRENCY", "32"),
                ("AIONBD_MAX_PAGE_LIMIT", "12"),
                ("AIONBD_MAX_TOPK_LIMIT", "34"),
                ("AIONBD_CHECKPOINT_INTERVAL", "2"),
                ("AIONBD_PERSISTENCE_ENABLED", "off"),
                ("AIONBD_WAL_SYNC_ON_WRITE", "false"),
                ("AIONBD_SNAPSHOT_PATH", "/tmp/custom_snapshot.json"),
                ("AIONBD_WAL_PATH", "/tmp/custom_wal.jsonl"),
            ],
            || AppConfig::from_env().expect("override config must parse"),
        );
        assert_eq!(
            config.bind,
            "0.0.0.0:9090"
                .parse::<SocketAddr>()
                .expect("override bind should parse")
        );
        assert_eq!(config.max_dimension, 128);
        assert_eq!(config.max_points_per_collection, 2048);
        assert!(!config.strict_finite);
        assert_eq!(config.request_timeout_ms, 5000);
        assert_eq!(config.max_body_bytes, 2048);
        assert_eq!(config.max_concurrency, 32);
        assert_eq!(config.max_page_limit, 12);
        assert_eq!(config.max_topk_limit, 34);
        assert_eq!(config.checkpoint_interval, 2);
        assert!(!config.persistence_enabled);
        assert!(!config.wal_sync_on_write);
        assert_eq!(
            config.snapshot_path,
            PathBuf::from("/tmp/custom_snapshot.json")
        );
        assert_eq!(config.wal_path, PathBuf::from("/tmp/custom_wal.jsonl"));
    }

    #[test]
    fn from_env_rejects_zero_max_page_limit() {
        let error = with_env(&[("AIONBD_MAX_PAGE_LIMIT", "0")], || {
            AppConfig::from_env().expect_err("zero max page limit must fail")
        });
        assert!(error
            .to_string()
            .contains("AIONBD_MAX_PAGE_LIMIT must be > 0"));
    }

    #[test]
    fn from_env_rejects_zero_max_topk_limit() {
        let error = with_env(&[("AIONBD_MAX_TOPK_LIMIT", "0")], || {
            AppConfig::from_env().expect_err("zero max top-k limit must fail")
        });
        assert!(error
            .to_string()
            .contains("AIONBD_MAX_TOPK_LIMIT must be > 0"));
    }

    #[test]
    fn from_env_rejects_invalid_bool() {
        let error = with_env(&[("AIONBD_STRICT_FINITE", "not-a-bool")], || {
            AppConfig::from_env().expect_err("invalid bool must fail")
        });
        assert!(error
            .to_string()
            .contains("AIONBD_STRICT_FINITE must be a boolean"));
    }
}
