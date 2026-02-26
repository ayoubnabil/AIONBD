use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use anyhow::Result;
use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::errors::ApiError;
use crate::state::AppState;

mod env;
pub(crate) mod jwt;
mod rate_limit;

#[cfg(feature = "exp_auth_api_key_scopes")]
use self::env::parse_api_key_scopes;
use self::env::{
    parse_auth_mode, parse_tenant_credentials, parse_tenant_credentials_with_fallback, parse_u64,
};
use self::jwt::JwtConfig;
use self::rate_limit::enforce_rate_limit;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthMode {
    Disabled,
    ApiKey,
    BearerToken,
    ApiKeyOrBearerToken,
    Jwt,
    ApiKeyOrJwt,
}

#[cfg_attr(not(feature = "exp_auth_api_key_scopes"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccessScope {
    Read,
    Write,
    Admin,
}

impl AccessScope {
    #[cfg(feature = "exp_auth_api_key_scopes")]
    pub(super) fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "read" | "readonly" | "read_only" => Ok(Self::Read),
            "write" | "readwrite" | "read_write" => Ok(Self::Write),
            "admin" => Ok(Self::Admin),
            invalid => {
                anyhow::bail!("api key scope must be one of read|write|admin, got '{invalid}'")
            }
        }
    }

    #[cfg(feature = "exp_auth_api_key_scopes")]
    pub(crate) fn can_write(self) -> bool {
        matches!(self, Self::Write | Self::Admin)
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "admin",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AuthConfig {
    pub(crate) mode: AuthMode,
    pub(crate) api_key_to_tenant: BTreeMap<String, String>,
    #[cfg(feature = "exp_auth_api_key_scopes")]
    pub(crate) api_key_scopes: BTreeMap<String, AccessScope>,
    pub(crate) bearer_token_to_tenant: BTreeMap<String, String>,
    pub(crate) jwt: Option<JwtConfig>,
    pub(crate) rate_limit_per_minute: u64,
    pub(crate) rate_window_retention_minutes: u64,
    pub(crate) tenant_max_collections: u64,
    pub(crate) tenant_max_points: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::Disabled,
            api_key_to_tenant: BTreeMap::new(),
            #[cfg(feature = "exp_auth_api_key_scopes")]
            api_key_scopes: BTreeMap::new(),
            bearer_token_to_tenant: BTreeMap::new(),
            jwt: None,
            rate_limit_per_minute: 0,
            rate_window_retention_minutes: 60,
            tenant_max_collections: 0,
            tenant_max_points: 0,
        }
    }
}

impl AuthConfig {
    pub(crate) fn from_env() -> Result<Self> {
        let mode = parse_auth_mode(std::env::var("AIONBD_AUTH_MODE").ok().as_deref())?;
        let api_key_to_tenant = parse_tenant_credentials("AIONBD_AUTH_API_KEYS")?;
        #[cfg(feature = "exp_auth_api_key_scopes")]
        let api_key_scopes = parse_api_key_scopes("AIONBD_AUTH_API_KEY_SCOPES")?;
        let bearer_tokens_raw = std::env::var("AIONBD_AUTH_BEARER_TOKENS").unwrap_or_default();
        let legacy_jwt_tokens_raw = std::env::var("AIONBD_AUTH_JWT_TOKENS").unwrap_or_default();
        if bearer_tokens_raw.trim().is_empty() && !legacy_jwt_tokens_raw.trim().is_empty() {
            tracing::warn!(
                "AIONBD_AUTH_JWT_TOKENS is deprecated; use AIONBD_AUTH_BEARER_TOKENS. \
Tokens are treated as opaque bearer credentials."
            );
        }
        let bearer_token_to_tenant = parse_tenant_credentials_with_fallback(
            "AIONBD_AUTH_BEARER_TOKENS",
            "AIONBD_AUTH_JWT_TOKENS",
        )?;
        let rate_limit_per_minute = parse_u64("AIONBD_AUTH_RATE_LIMIT_PER_MINUTE", 0)?;
        let rate_window_retention_minutes =
            parse_u64("AIONBD_AUTH_RATE_WINDOW_RETENTION_MINUTES", 60)?;
        let tenant_max_collections = parse_u64("AIONBD_AUTH_TENANT_MAX_COLLECTIONS", 0)?;
        let tenant_max_points = parse_u64("AIONBD_AUTH_TENANT_MAX_POINTS", 0)?;
        let jwt = if matches!(mode, AuthMode::Jwt | AuthMode::ApiKeyOrJwt) {
            Some(JwtConfig::from_env()?)
        } else {
            None
        };
        if rate_window_retention_minutes == 0 {
            anyhow::bail!("AIONBD_AUTH_RATE_WINDOW_RETENTION_MINUTES must be > 0");
        }
        #[cfg(feature = "exp_auth_api_key_scopes")]
        if api_key_scopes
            .keys()
            .any(|credential| !api_key_to_tenant.contains_key(credential))
        {
            anyhow::bail!(
                "AIONBD_AUTH_API_KEY_SCOPES contains entries for unknown API key credentials"
            );
        }

        if matches!(
            mode,
            AuthMode::ApiKey | AuthMode::ApiKeyOrBearerToken | AuthMode::ApiKeyOrJwt
        ) && api_key_to_tenant.is_empty()
        {
            anyhow::bail!("AIONBD_AUTH_MODE requires configured AIONBD_AUTH_API_KEYS entries");
        }
        if matches!(mode, AuthMode::BearerToken | AuthMode::ApiKeyOrBearerToken)
            && bearer_token_to_tenant.is_empty()
        {
            anyhow::bail!(
                "AIONBD_AUTH_MODE requires configured AIONBD_AUTH_BEARER_TOKENS (or legacy AIONBD_AUTH_JWT_TOKENS) entries"
            );
        }
        Ok(Self {
            mode,
            api_key_to_tenant,
            #[cfg(feature = "exp_auth_api_key_scopes")]
            api_key_scopes,
            bearer_token_to_tenant,
            jwt,
            rate_limit_per_minute,
            rate_window_retention_minutes,
            tenant_max_collections,
            tenant_max_points,
        })
    }

    fn authenticate(&self, headers: &axum::http::HeaderMap) -> Result<TenantContext, ApiError> {
        match self.mode {
            AuthMode::Disabled => Ok(TenantContext::public()),
            AuthMode::ApiKey => self.authenticate_api_key(headers),
            AuthMode::BearerToken => self.authenticate_bearer_token(headers),
            AuthMode::ApiKeyOrBearerToken => self
                .authenticate_api_key(headers)
                .or_else(|_| self.authenticate_bearer_token(headers)),
            AuthMode::Jwt => self.authenticate_jwt(headers),
            AuthMode::ApiKeyOrJwt => self
                .authenticate_api_key(headers)
                .or_else(|_| self.authenticate_jwt(headers)),
        }
    }

    fn authenticate_api_key(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<TenantContext, ApiError> {
        let api_key = headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| ApiError::unauthorized("missing x-api-key header"))?;

        let tenant = self
            .api_key_to_tenant
            .get(api_key)
            .ok_or_else(|| ApiError::unauthorized("invalid API key"))?;
        let access_scope = {
            #[cfg(feature = "exp_auth_api_key_scopes")]
            {
                self.api_key_scopes
                    .get(api_key)
                    .copied()
                    .unwrap_or(AccessScope::Admin)
            }
            #[cfg(not(feature = "exp_auth_api_key_scopes"))]
            {
                AccessScope::Admin
            }
        };
        Ok(TenantContext::tenant_with_scope(
            tenant.clone(),
            "api_key".to_string(),
            "api_key",
            access_scope,
        ))
    }

    fn authenticate_bearer_token(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<TenantContext, ApiError> {
        let header = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| ApiError::unauthorized("missing Authorization header"))?;

        let token = header
            .strip_prefix("Bearer ")
            .or_else(|| header.strip_prefix("bearer "))
            .ok_or_else(|| ApiError::unauthorized("invalid Authorization format"))?;

        let tenant = self
            .bearer_token_to_tenant
            .get(token)
            .ok_or_else(|| ApiError::unauthorized("invalid bearer token"))?;
        Ok(TenantContext::tenant(
            tenant.clone(),
            "bearer_token".to_string(),
            "bearer_token",
        ))
    }

    fn authenticate_jwt(&self, headers: &axum::http::HeaderMap) -> Result<TenantContext, ApiError> {
        self.jwt
            .as_ref()
            .ok_or_else(|| ApiError::unauthorized("jwt auth is not configured"))?
            .authenticate(headers)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TenantContext {
    tenant_id: Option<String>,
    principal: String,
    auth_scheme: &'static str,
    access_scope: AccessScope,
}

impl TenantContext {
    pub(crate) fn public() -> Self {
        Self {
            tenant_id: None,
            principal: "anonymous".to_string(),
            auth_scheme: "disabled",
            access_scope: AccessScope::Admin,
        }
    }

    pub(crate) fn tenant(tenant_id: String, principal: String, auth_scheme: &'static str) -> Self {
        Self::tenant_with_scope(tenant_id, principal, auth_scheme, AccessScope::Admin)
    }

    pub(crate) fn tenant_with_scope(
        tenant_id: String,
        principal: String,
        auth_scheme: &'static str,
        access_scope: AccessScope,
    ) -> Self {
        Self {
            tenant_id: Some(tenant_id),
            principal,
            auth_scheme,
            access_scope,
        }
    }

    pub(crate) fn tenant_key(&self) -> &str {
        self.tenant_id.as_deref().unwrap_or("public")
    }

    pub(crate) fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    pub(crate) fn access_scope(&self) -> AccessScope {
        self.access_scope
    }

    pub(crate) fn require_write(&self) -> Result<(), ApiError> {
        #[cfg(feature = "exp_auth_api_key_scopes")]
        {
            if self.access_scope.can_write() {
                return Ok(());
            }
            return Err(ApiError::forbidden(
                "write operation requires write or admin API key scope",
            ));
        }

        #[cfg(not(feature = "exp_auth_api_key_scopes"))]
        {
            Ok(())
        }
    }
}

pub(crate) async fn auth_rate_limit_audit(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();

    let tenant = match state.auth_config.authenticate(request.headers()) {
        Ok(tenant) => tenant,
        Err(error) => {
            let _ = state
                .metrics
                .auth_failures_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(method = %method, path = %path, "authentication failed");
            return error.into_response();
        }
    };

    if let Err(error) = enforce_rate_limit(&state, &tenant).await {
        let _ = state
            .metrics
            .rate_limit_rejections_total
            .fetch_add(1, Ordering::Relaxed);
        tracing::warn!(tenant = %tenant.tenant_key(), method = %method, path = %path, "rate limit rejected request");
        return error.into_response();
    }

    request.extensions_mut().insert(tenant.clone());
    let response = next.run(request).await;

    let _ = state
        .metrics
        .audit_events_total
        .fetch_add(1, Ordering::Relaxed);
    tracing::info!(
        target: "audit",
        tenant = %tenant.tenant_key(),
        principal = %tenant.principal,
        auth_scheme = %tenant.auth_scheme,
        access_scope = tenant.access_scope().as_str(),
        method = %method,
        path = %path,
        status = response.status().as_u16(),
        "audit_log"
    );

    response
}
