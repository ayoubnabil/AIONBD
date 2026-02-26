use anyhow::{Context, Result};
use axum::http::HeaderMap;
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde_json::{Map, Value};

use crate::errors::ApiError;

use super::TenantContext;

#[derive(Clone)]
pub(crate) struct JwtConfig {
    decoding_key: DecodingKey,
    issuer: Option<String>,
    audience: Vec<String>,
    tenant_claim: String,
    principal_claim: String,
}

impl std::fmt::Debug for JwtConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtConfig")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("tenant_claim", &self.tenant_claim)
            .field("principal_claim", &self.principal_claim)
            .finish()
    }
}

impl JwtConfig {
    pub(crate) fn from_env() -> Result<Self> {
        let secret = std::env::var("AIONBD_AUTH_JWT_HS256_SECRET")
            .with_context(|| "AIONBD_AUTH_MODE jwt requires AIONBD_AUTH_JWT_HS256_SECRET")?;
        let issuer = optional_env("AIONBD_AUTH_JWT_ISSUER");
        let audience = csv_env("AIONBD_AUTH_JWT_AUDIENCE");
        let tenant_claim =
            optional_env("AIONBD_AUTH_JWT_TENANT_CLAIM").unwrap_or_else(|| "tenant".to_string());
        let principal_claim =
            optional_env("AIONBD_AUTH_JWT_PRINCIPAL_CLAIM").unwrap_or_else(|| "sub".to_string());

        Self::from_parts(secret, issuer, audience, tenant_claim, principal_claim)
    }

    #[cfg(test)]
    pub(crate) fn for_tests(secret: impl AsRef<str>) -> Self {
        Self::from_parts(
            secret.as_ref().to_string(),
            None,
            Vec::new(),
            "tenant".to_string(),
            "sub".to_string(),
        )
        .expect("test jwt config should be valid")
    }

    pub(crate) fn authenticate(&self, headers: &HeaderMap) -> Result<TenantContext, ApiError> {
        let token = bearer_token(headers)?;
        let mut validation = Validation::new(Algorithm::HS256);
        validation.required_spec_claims.insert("exp".to_string());
        if let Some(issuer) = self.issuer.as_deref() {
            validation.set_issuer(&[issuer]);
        }
        if !self.audience.is_empty() {
            validation.set_audience(&self.audience);
        }

        let token_data =
            decode::<Value>(token, &self.decoding_key, &validation).map_err(map_jwt_error)?;
        let claims = token_data
            .claims
            .as_object()
            .ok_or_else(|| ApiError::unauthorized("invalid jwt claims payload"))?;
        let tenant = string_claim(claims, &self.tenant_claim)
            .ok_or_else(|| ApiError::unauthorized("jwt token is missing tenant claim"))?;
        let principal = string_claim(claims, &self.principal_claim).unwrap_or(tenant);

        Ok(TenantContext::tenant(
            tenant.to_string(),
            principal.to_string(),
            "jwt",
        ))
    }

    fn from_parts(
        secret: String,
        issuer: Option<String>,
        audience: Vec<String>,
        tenant_claim: String,
        principal_claim: String,
    ) -> Result<Self> {
        if secret.trim().is_empty() {
            anyhow::bail!("AIONBD_AUTH_JWT_HS256_SECRET must not be empty");
        }
        if tenant_claim.trim().is_empty() || principal_claim.trim().is_empty() {
            anyhow::bail!(
                "AIONBD_AUTH_JWT_TENANT_CLAIM and AIONBD_AUTH_JWT_PRINCIPAL_CLAIM must not be empty"
            );
        }

        Ok(Self {
            decoding_key: DecodingKey::from_secret(secret.as_bytes()),
            issuer,
            audience,
            tenant_claim,
            principal_claim,
        })
    }
}

fn bearer_token(headers: &HeaderMap) -> Result<&str, ApiError> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError::unauthorized("missing Authorization header"))?;

    header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))
        .ok_or_else(|| ApiError::unauthorized("invalid Authorization format"))
}

fn string_claim<'a>(claims: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    claims
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn map_jwt_error(error: jsonwebtoken::errors::Error) -> ApiError {
    match error.kind() {
        ErrorKind::ExpiredSignature => ApiError::unauthorized("expired jwt token"),
        _ => ApiError::unauthorized("invalid jwt token"),
    }
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key).ok().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn csv_env(key: &str) -> Vec<String> {
    std::env::var(key)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}
