use std::env;

use anyhow::Result;

pub(crate) fn parse_bool_env(key: &str, default: bool) -> Result<bool> {
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
