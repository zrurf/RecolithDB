use std::fs;
use std::io::Read;
use std::path::Path;

use recolith_security::{
    RevocationList, TokenClaims, TokenIssuer, TokenVerifier, fingerprint_hex, token_fingerprint,
    unix_seconds,
};

use crate::error::CliError;

pub(crate) fn issue(
    secret_file: &Path,
    subject: String,
    roles: Vec<String>,
    ttl: u64,
) -> Result<String, CliError> {
    let issuer = TokenIssuer::from_secret_file(secret_file)?;
    let issued_at = unix_seconds();
    let expires_at = issued_at.checked_add(ttl).ok_or(CliError::InvalidTtl)?;
    let claims = TokenClaims {
        subject,
        roles,
        issued_at,
        expires_at,
    };

    Ok(issuer.issue(&claims)?)
}

pub(crate) fn inspect(token_file: Option<&Path>) -> Result<(TokenClaims, String), CliError> {
    let token = read_token(token_file)?;
    let fingerprint = fingerprint_hex(&token_fingerprint(&token));
    let claims = TokenVerifier::decode_unverified(&token)?;

    Ok((claims, fingerprint))
}

pub(crate) fn verify(
    secret_file: &Path,
    previous_secret_file: Option<&Path>,
    token_file: Option<&Path>,
) -> Result<(TokenClaims, String), CliError> {
    let token = read_token(token_file)?;
    let fingerprint = fingerprint_hex(&token_fingerprint(&token));
    let verifier = TokenVerifier::from_secret_files(secret_file, previous_secret_file)?;
    let claims = verifier.verify(&token, unix_seconds())?;

    Ok((claims, fingerprint))
}

pub(crate) fn hash(token_file: Option<&Path>) -> Result<String, CliError> {
    let token = read_token(token_file)?;

    Ok(fingerprint_hex(&token_fingerprint(&token)))
}

pub(crate) fn revoke(
    token_file: Option<&Path>,
    revoked_file: &Path,
) -> Result<(bool, String), CliError> {
    let token = read_token(token_file)?;
    let fingerprint = token_fingerprint(&token);
    let mut list = if revoked_file.exists() {
        RevocationList::load(revoked_file)?
    } else {
        RevocationList::default()
    };
    let added = list.insert(fingerprint);
    fs::write(revoked_file, list.to_toml_string())?;

    Ok((added, fingerprint_hex(&fingerprint)))
}

pub(crate) fn format_claims(claims: &TokenClaims, verified: bool, fingerprint: &str) -> String {
    let now = unix_seconds();
    let status = if claims.expires_at <= now {
        "expired"
    } else {
        "valid"
    };
    format!(
        "verified: {verified}
subject: {}
roles: {}
issued_at: {}
expires_at: {} ({status})
fingerprint: {fingerprint}
",
        claims.subject,
        claims.roles.join(", "),
        claims.issued_at,
        claims.expires_at
    )
}

fn read_token(token_file: Option<&Path>) -> Result<String, CliError> {
    let token = match token_file {
        Some(path) => fs::read_to_string(path)?,
        None => {
            let mut input = String::new();
            std::io::stdin().read_to_string(&mut input)?;
            input
        }
    };
    let token = token.trim();
    if token.is_empty() {
        return Err(CliError::EmptyToken);
    }

    Ok(token.to_owned())
}
