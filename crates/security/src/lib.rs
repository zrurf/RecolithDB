//! Opaque HMAC tokens, roles, and collection-scoped authorization.
#![forbid(unsafe_code)]

mod base64url;
mod hmac;
pub mod roles;
pub mod token;

pub use roles::{Action, AuthTarget, Authenticator, Permission, Principal, RoleSet, SecurityError};
pub use token::{
    CLOCK_SKEW_SECONDS, DEFAULT_TTL_SECONDS, MAX_TTL_SECONDS, TokenClaims, TokenError, TokenIssuer,
    TokenVerifier, unix_seconds,
};
