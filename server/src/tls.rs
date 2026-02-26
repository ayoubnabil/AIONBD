use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;

use crate::env_utils::parse_bool_env;

#[derive(Debug, Clone)]
pub(crate) struct TlsRuntimeConfig {
    enabled: bool,
    cert_path: Option<PathBuf>,
    key_path: Option<PathBuf>,
}

impl TlsRuntimeConfig {
    pub(crate) fn from_env() -> Result<Self> {
        let enabled = parse_bool_env("AIONBD_TLS_ENABLED", false)?;
        let cert_path = parse_optional_path("AIONBD_TLS_CERT_PATH")?;
        let key_path = parse_optional_path("AIONBD_TLS_KEY_PATH")?;

        if enabled {
            let cert = cert_path.as_ref().ok_or_else(|| {
                anyhow::anyhow!("AIONBD_TLS_CERT_PATH is required when AIONBD_TLS_ENABLED=true")
            })?;
            let key = key_path.as_ref().ok_or_else(|| {
                anyhow::anyhow!("AIONBD_TLS_KEY_PATH is required when AIONBD_TLS_ENABLED=true")
            })?;
            ensure_file_exists("AIONBD_TLS_CERT_PATH", cert)?;
            ensure_file_exists("AIONBD_TLS_KEY_PATH", key)?;
        }

        Ok(Self {
            enabled,
            cert_path,
            key_path,
        })
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn cert_path(&self) -> Option<&Path> {
        self.cert_path.as_deref()
    }

    pub(crate) fn key_path(&self) -> Option<&Path> {
        self.key_path.as_deref()
    }

    pub(crate) async fn rustls_config(&self) -> Result<Option<RustlsConfig>> {
        if !self.enabled {
            return Ok(None);
        }

        let cert_path = self
            .cert_path
            .as_ref()
            .expect("tls cert path is validated when enabled");
        let key_path = self
            .key_path
            .as_ref()
            .expect("tls key path is validated when enabled");

        let config = RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .with_context(|| {
                format!(
                    "failed to load TLS cert '{}' and key '{}'",
                    cert_path.display(),
                    key_path.display()
                )
            })?;
        Ok(Some(config))
    }
}

fn parse_optional_path(key: &str) -> Result<Option<PathBuf>> {
    let Ok(raw) = env::var(key) else {
        return Ok(None);
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("{key} must not be empty when set");
    }

    Ok(Some(PathBuf::from(trimmed)))
}

fn ensure_file_exists(key: &str, path: &Path) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("{key} points to a missing file: {}", path.display());
    }
    if !path.is_file() {
        anyhow::bail!("{key} must point to a file: {}", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::TlsRuntimeConfig;
    use std::env;
    use std::fs;
    use std::sync::{Mutex, OnceLock};

    const TLS_KEYS: &[&str] = &[
        "AIONBD_TLS_ENABLED",
        "AIONBD_TLS_CERT_PATH",
        "AIONBD_TLS_KEY_PATH",
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
        let _lock = env_lock().lock().expect("env lock should be available");
        let _guard = EnvGuard::capture(TLS_KEYS);

        for key in TLS_KEYS {
            env::remove_var(key);
        }
        for (key, value) in pairs {
            env::set_var(key, value);
        }

        f()
    }

    #[test]
    fn defaults_disable_tls() {
        let config = with_env(&[], || {
            TlsRuntimeConfig::from_env().expect("default TLS config should parse")
        });
        assert!(!config.enabled());
        assert!(config.cert_path().is_none());
        assert!(config.key_path().is_none());
    }

    #[test]
    fn enabled_tls_requires_paths() {
        let error = with_env(&[("AIONBD_TLS_ENABLED", "true")], || {
            TlsRuntimeConfig::from_env().expect_err("missing TLS paths should fail")
        });
        assert!(error
            .to_string()
            .contains("AIONBD_TLS_CERT_PATH is required"));
    }

    #[test]
    fn enabled_tls_accepts_existing_files() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        let temp_dir =
            std::env::temp_dir().join(format!("aionbd_tls_test_{}_{}", std::process::id(), unique));
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");
        let cert_path = temp_dir.join("cert.pem");
        let key_path = temp_dir.join("key.pem");
        fs::write(
            &cert_path,
            "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
        )
        .expect("cert file should be written");
        fs::write(
            &key_path,
            "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n",
        )
        .expect("key file should be written");

        let config = with_env(
            &[
                ("AIONBD_TLS_ENABLED", "true"),
                (
                    "AIONBD_TLS_CERT_PATH",
                    cert_path.to_str().expect("cert path should be utf-8"),
                ),
                (
                    "AIONBD_TLS_KEY_PATH",
                    key_path.to_str().expect("key path should be utf-8"),
                ),
            ],
            || TlsRuntimeConfig::from_env().expect("TLS config should parse"),
        );

        assert!(config.enabled());
        assert_eq!(
            config.cert_path().expect("cert path should be set"),
            cert_path.as_path()
        );
        assert_eq!(
            config.key_path().expect("key path should be set"),
            key_path.as_path()
        );
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn rejects_invalid_bool() {
        let error = with_env(&[("AIONBD_TLS_ENABLED", "invalid")], || {
            TlsRuntimeConfig::from_env().expect_err("invalid bool should fail")
        });
        assert!(error
            .to_string()
            .contains("AIONBD_TLS_ENABLED must be a boolean"));
    }
}
