use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::SystemTime;

use serde::Deserialize;
use thiserror::Error;

use crate::revocation::RevocationList;
use crate::token::{TokenError, TokenVerifier, token_fingerprint};

/// Authorization action.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    /// DOC_GET and QUERY.
    Read,
    /// DOC_PUT.
    Write,
    /// DESCRIBE_COLLECTION.
    Describe,
    /// Administrative operations, implying all actions.
    Admin,
}

/// Permission over a keyspace/collection scope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Permission {
    /// Keyspace name or `*`.
    pub keyspace: String,
    /// Collection name or `*`.
    pub collection: String,
    /// Allowed actions.
    pub actions: Vec<Action>,
}

/// Resource being authorized.
#[derive(Clone, Copy, Debug)]
pub struct AuthTarget<'a> {
    /// Keyspace name.
    pub keyspace: &'a str,
    /// Collection name.
    pub collection: &'a str,
}

/// Authenticated principal with resolved permissions.
#[derive(Clone, Debug)]
pub struct Principal {
    id: String,
    permissions: Vec<Permission>,
    expires_at: u64,
    fingerprint: Option<[u8; 32]>,
}

impl Principal {
    /// Creates an anonymous principal with full development permissions.
    #[must_use]
    pub fn anonymous() -> Self {
        Self::admin("anonymous")
    }

    /// Creates a principal with full permissions.
    #[must_use]
    pub fn admin(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            permissions: vec![Permission {
                keyspace: "*".to_owned(),
                collection: "*".to_owned(),
                actions: vec![Action::Admin],
            }],
            expires_at: u64::MAX,
            fingerprint: None,
        }
    }

    /// Returns the principal identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the expiration Unix time.
    #[must_use]
    pub const fn expires_at(&self) -> u64 {
        self.expires_at
    }

    /// Returns the token fingerprint, or `None` for non-token principals.
    #[must_use]
    pub const fn fingerprint(&self) -> Option<[u8; 32]> {
        self.fingerprint
    }

    pub(crate) fn with_fingerprint(mut self, fingerprint: [u8; 32]) -> Self {
        self.fingerprint = Some(fingerprint);
        self
    }

    /// Returns whether the principal has expired.
    #[must_use]
    pub const fn is_expired(&self, now: u64) -> bool {
        self.expires_at != u64::MAX && now >= self.expires_at
    }

    /// Checks an action against a resource.
    #[must_use]
    pub fn authorize(&self, target: AuthTarget<'_>, action: Action) -> bool {
        self.permissions
            .iter()
            .any(|permission| permission.matches(target, action))
    }
}

impl Permission {
    fn matches(&self, target: AuthTarget<'_>, action: Action) -> bool {
        let scope = (self.keyspace == "*" || self.keyspace == target.keyspace)
            && (self.collection == "*" || self.collection == target.collection);
        scope && (self.actions.contains(&action) || self.actions.contains(&Action::Admin))
    }
}

/// Role and permission loading failure.
#[derive(Debug, Error)]
pub enum SecurityError {
    /// Token handling failed.
    #[error(transparent)]
    Token(#[from] TokenError),
    /// Roles file could not be read.
    #[error("failed to read roles {path}: {source}")]
    RolesRead {
        /// Roles path.
        path: PathBuf,
        /// I/O failure.
        source: std::io::Error,
    },
    /// Revocation list could not be read.
    #[error("failed to read token revocations {path}: {source}")]
    RevocationRead {
        /// Revocation path.
        path: PathBuf,
        /// I/O failure.
        source: std::io::Error,
    },
    /// Revocation list is not valid TOML.
    #[error("invalid token revocations: {0}")]
    RevocationParse(toml::de::Error),
    /// A revoked fingerprint is malformed.
    #[error("invalid token fingerprint `{0}`")]
    InvalidRevocation(String),
    /// The presented token has been revoked.
    #[error("token has been revoked")]
    RevokedToken,
    /// Authentication file metadata could not be read.
    #[error("failed to read authentication metadata {path}: {source}")]
    Metadata {
        /// File path.
        path: PathBuf,
        /// I/O failure.
        source: std::io::Error,
    },
    /// An internal authentication lock was poisoned.
    #[error("authentication lock poisoned")]
    LockPoisoned,
    /// Roles file is not valid TOML.
    #[error("invalid roles file: {0}")]
    RolesParse(#[from] toml::de::Error),
    /// A role definition is invalid.
    #[error("invalid role `{role}`: {message}")]
    InvalidRole {
        /// Role name.
        role: String,
        /// Validation failure.
        message: String,
    },
}

/// Set of named roles with collection-scoped permissions.
#[derive(Debug, Default)]
pub struct RoleSet {
    roles: BTreeMap<String, Vec<Permission>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleFile {
    #[serde(default)]
    roles: Vec<RoleDefinition>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleDefinition {
    name: String,
    permissions: Vec<Permission>,
}

impl RoleSet {
    /// Loads roles from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] for I/O, TOML, or validation failures.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, SecurityError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| SecurityError::RolesRead {
            path: path.to_path_buf(),
            source,
        })?;

        Self::parse(&contents)
    }

    /// Parses roles from TOML.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] for TOML or validation failures.
    pub fn parse(contents: &str) -> Result<Self, SecurityError> {
        let file: RoleFile = toml::from_str(contents)?;
        if file.roles.is_empty() {
            return Err(SecurityError::InvalidRole {
                role: String::new(),
                message: "roles file is empty".to_owned(),
            });
        }

        let mut roles = BTreeMap::new();
        for role in file.roles {
            validate_role(&role)?;
            if roles.insert(role.name.clone(), role.permissions).is_some() {
                return Err(SecurityError::InvalidRole {
                    role: role.name,
                    message: "duplicate role".to_owned(),
                });
            }
        }

        Ok(Self { roles })
    }

    /// Resolves a principal from token roles.
    #[must_use]
    pub fn principal(&self, id: impl Into<String>, roles: &[String], expires_at: u64) -> Principal {
        let permissions = roles
            .iter()
            .filter_map(|role| self.roles.get(role))
            .flatten()
            .cloned()
            .collect();

        Principal {
            id: id.into(),
            permissions,
            expires_at,
            fingerprint: None,
        }
    }
}

fn validate_role(role: &RoleDefinition) -> Result<(), SecurityError> {
    if role.name.is_empty() || role.name.chars().any(char::is_control) {
        return Err(SecurityError::InvalidRole {
            role: role.name.clone(),
            message: "role name is invalid".to_owned(),
        });
    }
    if role.permissions.is_empty() {
        return Err(SecurityError::InvalidRole {
            role: role.name.clone(),
            message: "role has no permissions".to_owned(),
        });
    }
    for permission in &role.permissions {
        if permission.keyspace.is_empty() || permission.collection.is_empty() {
            return Err(SecurityError::InvalidRole {
                role: role.name.clone(),
                message: "permission scope is empty".to_owned(),
            });
        }
        if permission.actions.is_empty() {
            return Err(SecurityError::InvalidRole {
                role: role.name.clone(),
                message: "permission has no actions".to_owned(),
            });
        }
        let mut actions = BTreeSet::new();
        for action in &permission.actions {
            if !actions.insert(*action) {
                return Err(SecurityError::InvalidRole {
                    role: role.name.clone(),
                    message: "permission repeats an action".to_owned(),
                });
            }
        }
    }

    Ok(())
}

/// Token verifier, role registry, and revocation list.
#[derive(Debug)]
pub struct Authenticator {
    verifier: TokenVerifier,
    roles_path: PathBuf,
    revoked_path: Option<PathBuf>,
    state: RwLock<AuthState>,
}

#[derive(Debug)]
struct AuthState {
    roles: RoleSet,
    revoked: RevocationList,
    roles_stamp: FileStamp,
    revoked_stamp: Option<FileStamp>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileStamp {
    modified: Option<SystemTime>,
    len: u64,
}

impl Authenticator {
    /// Loads secrets and roles.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] for secret, roles, or token configuration failures.
    pub fn from_files(
        secret: impl AsRef<Path>,
        previous: Option<impl AsRef<Path>>,
        roles: impl AsRef<Path>,
    ) -> Result<Self, SecurityError> {
        Self::from_files_with_revocations(secret, previous, roles, None::<&Path>)
    }

    /// Loads secrets, roles, and an optional revocation list.
    ///
    /// Roles and revocations are reloaded when their files change.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] for secret, roles, revocation, or token
    /// configuration failures.
    pub fn from_files_with_revocations(
        secret: impl AsRef<Path>,
        previous: Option<impl AsRef<Path>>,
        roles: impl AsRef<Path>,
        revocations: Option<impl AsRef<Path>>,
    ) -> Result<Self, SecurityError> {
        let verifier = TokenVerifier::from_secret_files(secret, previous)?;
        let roles_path = roles.as_ref().to_path_buf();
        let roles_value = RoleSet::load(&roles_path)?;
        let roles_stamp = file_stamp(&roles_path)?;
        let revoked_path = revocations.map(|path| path.as_ref().to_path_buf());
        let (revoked, revoked_stamp) = match &revoked_path {
            Some(path) => (RevocationList::load(path)?, Some(file_stamp(path)?)),
            None => (RevocationList::default(), None),
        };

        Ok(Self {
            verifier,
            roles_path,
            revoked_path,
            state: RwLock::new(AuthState {
                roles: roles_value,
                revoked,
                roles_stamp,
                revoked_stamp,
            }),
        })
    }

    /// Verifies a token and resolves its roles.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] for invalid, expired, malformed, or revoked
    /// tokens.
    pub fn authenticate(&self, token: &str, now: u64) -> Result<Principal, SecurityError> {
        self.reload_if_changed()?;
        let claims = self.verifier.verify(token, now)?;
        let fingerprint = token_fingerprint(token);
        let state = self.state.read().map_err(|_| SecurityError::LockPoisoned)?;
        if state.revoked.contains(&fingerprint) {
            return Err(SecurityError::RevokedToken);
        }

        Ok(state
            .roles
            .principal(claims.subject, &claims.roles, claims.expires_at)
            .with_fingerprint(fingerprint))
    }

    /// Returns whether a token fingerprint is revoked.
    ///
    /// Reload failures fail closed.
    #[must_use]
    pub fn is_revoked(&self, fingerprint: [u8; 32]) -> bool {
        if self.reload_if_changed().is_err() {
            return true;
        }

        self.state
            .read()
            .map(|state| state.revoked.contains(&fingerprint))
            .unwrap_or(true)
    }

    fn reload_if_changed(&self) -> Result<(), SecurityError> {
        let roles_stamp = file_stamp(&self.roles_path)?;
        let revoked_stamp = match &self.revoked_path {
            Some(path) => Some(file_stamp(path)?),
            None => None,
        };
        {
            let state = self.state.read().map_err(|_| SecurityError::LockPoisoned)?;
            if state.roles_stamp == roles_stamp && state.revoked_stamp == revoked_stamp {
                return Ok(());
            }
        }

        let mut state = self
            .state
            .write()
            .map_err(|_| SecurityError::LockPoisoned)?;
        if state.roles_stamp != roles_stamp {
            state.roles = RoleSet::load(&self.roles_path)?;
            state.roles_stamp = roles_stamp;
        }
        if state.revoked_stamp != revoked_stamp {
            state.revoked = match &self.revoked_path {
                Some(path) => RevocationList::load(path)?,
                None => RevocationList::default(),
            };
            state.revoked_stamp = revoked_stamp;
        }

        Ok(())
    }
}

fn file_stamp(path: &Path) -> Result<FileStamp, SecurityError> {
    let metadata = fs::metadata(path).map_err(|source| SecurityError::Metadata {
        path: path.to_path_buf(),
        source,
    })?;

    Ok(FileStamp {
        modified: metadata.modified().ok(),
        len: metadata.len(),
    })
}
