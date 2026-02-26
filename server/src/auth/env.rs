use std::collections::BTreeMap;

use anyhow::{Context, Result};

#[cfg(feature = "exp_auth_api_key_scopes")]
use super::AccessScope;
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

#[cfg(feature = "exp_auth_api_key_scopes")]
pub(super) fn parse_api_key_scopes(key: &str) -> Result<BTreeMap<String, AccessScope>> {
    let raw = std::env::var(key).unwrap_or_default();
    parse_api_key_scopes_raw(key, &raw)
}

#[cfg(feature = "exp_auth_api_key_scopes")]
fn parse_api_key_scopes_raw(key: &str, raw: &str) -> Result<BTreeMap<String, AccessScope>> {
    let mut mapping = BTreeMap::new();
    for raw_pair in raw.split(',').filter(|item| !item.trim().is_empty()) {
        let (credential, scope_raw) = raw_pair
            .split_once(':')
            .with_context(|| format!("{key} entries must be '<credential>:<scope>'"))?;
        let credential = credential.trim();
        let scope_raw = scope_raw.trim();
        if credential.is_empty() || scope_raw.is_empty() {
            anyhow::bail!("{key} contains an empty credential or scope");
        }
        let scope = AccessScope::parse(scope_raw)
            .with_context(|| format!("failed to parse scope for credential in {key}"))?;
        mapping.insert(credential.to_string(), scope);
    }
    Ok(mapping)
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

    #[cfg(feature = "exp_auth_api_key_scopes")]
    #[test]
    fn parse_api_key_scopes_supports_known_values() {
        let parsed =
            parse_api_key_scopes_raw("AIONBD_AUTH_API_KEY_SCOPES", "key-a:read,key-b:write")
                .expect("scopes should parse");
        assert_eq!(parsed.get("key-a"), Some(&AccessScope::Read));
        assert_eq!(parsed.get("key-b"), Some(&AccessScope::Write));
    }

    #[cfg(feature = "exp_auth_api_key_scopes")]
    #[test]
    fn parse_api_key_scopes_rejects_invalid_scope() {
        let result = parse_api_key_scopes_raw("AIONBD_AUTH_API_KEY_SCOPES", "key-a:owner");
        assert!(result.is_err());
    }
}
