use std::collections::BTreeMap;

use anyhow::{Context, Result};

use super::AuthMode;

pub(super) fn parse_auth_mode(value: Option<&str>) -> Result<AuthMode> {
    match value
        .unwrap_or("disabled")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "disabled" => Ok(AuthMode::Disabled),
        "api_key" => Ok(AuthMode::ApiKey),
        "bearer_token" => Ok(AuthMode::BearerToken),
        "jwt" => Ok(AuthMode::Jwt),
        "api_key_or_bearer_token" => Ok(AuthMode::ApiKeyOrBearerToken),
        "api_key_or_jwt" => Ok(AuthMode::ApiKeyOrJwt),
        invalid => anyhow::bail!(
            "AIONBD_AUTH_MODE must be one of disabled|api_key|bearer_token|api_key_or_bearer_token|jwt|api_key_or_jwt, got '{invalid}'"
        ),
    }
}

pub(super) fn parse_tenant_credentials(key: &str) -> Result<BTreeMap<String, String>> {
    let raw = std::env::var(key).unwrap_or_default();
    let mut mapping = BTreeMap::new();

    for raw_pair in raw.split(',').filter(|item| !item.trim().is_empty()) {
        let (tenant, credential) = raw_pair
            .split_once(':')
            .with_context(|| format!("{key} entries must be '<tenant>:<credential>'"))?;
        let tenant = tenant.trim();
        let credential = credential.trim();
        if tenant.is_empty() || credential.is_empty() {
            anyhow::bail!("{key} contains an empty tenant or credential");
        }
        mapping.insert(credential.to_string(), tenant.to_string());
    }

    Ok(mapping)
}

pub(super) fn parse_tenant_credentials_with_fallback(
    preferred_key: &str,
    legacy_key: &str,
) -> Result<BTreeMap<String, String>> {
    let preferred = parse_tenant_credentials(preferred_key)?;
    if !preferred.is_empty() {
        return Ok(preferred);
    }
    parse_tenant_credentials(legacy_key)
}

pub(super) fn parse_u64(key: &str, default: u64) -> Result<u64> {
    let raw = std::env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse()
        .with_context(|| format!("{key} must be a non-negative integer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_auth_mode_supports_jwt_variants() {
        assert_eq!(
            parse_auth_mode(Some("jwt")).expect("jwt mode should parse"),
            AuthMode::Jwt
        );
        assert_eq!(
            parse_auth_mode(Some("api_key_or_jwt")).expect("api_key_or_jwt mode should parse"),
            AuthMode::ApiKeyOrJwt
        );
    }
}
