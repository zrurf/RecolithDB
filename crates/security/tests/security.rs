use recolith_security::{
    Action, AuthTarget, Authenticator, MAX_TTL_SECONDS, RevocationList, RoleSet, SecurityError,
    TokenClaims, TokenError, TokenIssuer, TokenVerifier, fingerprint_hex, token_fingerprint,
    unix_seconds,
};

fn claims(issued_at: u64, expires_at: u64) -> TokenClaims {
    TokenClaims {
        subject: "game-service".to_owned(),
        roles: vec!["game-writer".to_owned()],
        issued_at,
        expires_at,
    }
}

#[test]
fn issues_and_verifies_token() {
    let key = [7_u8; 32];
    let issuer = TokenIssuer::new(key);
    let verifier = TokenVerifier::new(vec![key]).expect("verifier");
    let token = issuer.issue(&claims(1_000, 4_600)).expect("issue");

    let verified = verifier.verify(&token, 2_000).expect("verify");
    assert_eq!(verified, claims(1_000, 4_600));
}

#[test]
fn rejects_tampered_token() {
    let key = [7_u8; 32];
    let issuer = TokenIssuer::new(key);
    let verifier = TokenVerifier::new(vec![key]).expect("verifier");
    let token = issuer.issue(&claims(1_000, 4_600)).expect("issue");
    let mut tampered = token.into_bytes();
    let index = tampered.len() - 2;
    tampered[index] ^= 0x01;
    let tampered = String::from_utf8(tampered).expect("utf8");

    assert!(matches!(
        verifier.verify(&tampered, 2_000),
        Err(TokenError::InvalidSignature | TokenError::Malformed)
    ));
}

#[test]
fn rejects_wrong_secret() {
    let issuer = TokenIssuer::new([7_u8; 32]);
    let verifier = TokenVerifier::new(vec![[8_u8; 32]]).expect("verifier");
    let token = issuer.issue(&claims(1_000, 4_600)).expect("issue");

    assert!(matches!(
        verifier.verify(&token, 2_000),
        Err(TokenError::InvalidSignature)
    ));
}

#[test]
fn accepts_previous_secret() {
    let issuer = TokenIssuer::new([7_u8; 32]);
    let verifier = TokenVerifier::new(vec![[8_u8; 32], [7_u8; 32]]).expect("verifier");
    let token = issuer.issue(&claims(1_000, 4_600)).expect("issue");

    assert!(verifier.verify(&token, 2_000).is_ok());
}

#[test]
fn rejects_expired_and_future_tokens() {
    let key = [7_u8; 32];
    let issuer = TokenIssuer::new(key);
    let verifier = TokenVerifier::new(vec![key]).expect("verifier");
    let expired = issuer.issue(&claims(1_000, 1_100)).expect("issue");
    let future = issuer.issue(&claims(5_000, 6_000)).expect("issue");

    assert!(matches!(
        verifier.verify(&expired, 2_000),
        Err(TokenError::Expired)
    ));
    assert!(matches!(
        verifier.verify(&future, 2_000),
        Err(TokenError::NotYetValid)
    ));
}

#[test]
fn rejects_excessive_ttl() {
    let issuer = TokenIssuer::new([7_u8; 32]);
    let token = issuer.issue(&claims(0, MAX_TTL_SECONDS + 1));

    assert!(matches!(token, Err(TokenError::LifetimeTooLong)));
}

#[test]
fn roles_apply_collection_scope_and_admin_override() {
    let roles = RoleSet::parse(
        r#"
[[roles]]
name = "game-writer"
permissions = [
  { keyspace = "game", collection = "player", actions = ["read", "write"] },
]

[[roles]]
name = "admin"
permissions = [
  { keyspace = "*", collection = "*", actions = ["admin"] },
]
"#,
    )
    .expect("roles");

    let writer = roles.principal("writer", &["game-writer".to_owned()], 100);
    let player = AuthTarget {
        keyspace: "game",
        collection: "player",
    };
    let inventory = AuthTarget {
        keyspace: "game",
        collection: "inventory",
    };
    assert!(writer.authorize(player, Action::Read));
    assert!(writer.authorize(player, Action::Write));
    assert!(!writer.authorize(player, Action::Describe));
    assert!(!writer.authorize(inventory, Action::Read));

    let admin = roles.principal("admin", &["admin".to_owned()], 100);
    assert!(admin.authorize(inventory, Action::Describe));
    assert!(admin.authorize(inventory, Action::Write));
}

#[test]
fn unknown_roles_grant_no_permissions() {
    let roles = RoleSet::parse(
        r#"
[[roles]]
name = "reader"
permissions = [{ keyspace = "game", collection = "*", actions = ["read"] }]
"#,
    )
    .expect("roles");
    let principal = roles.principal("unknown", &["missing".to_owned()], 100);

    assert!(!principal.authorize(
        AuthTarget {
            keyspace: "game",
            collection: "player",
        },
        Action::Read,
    ));
}

#[test]
fn decodes_claims_without_signature_verification() {
    let issuer = TokenIssuer::new([7_u8; 32]);
    let token = issuer.issue(&claims(1_000, 4_600)).expect("issue");
    let decoded = TokenVerifier::decode_unverified(&token).expect("decode");

    assert_eq!(decoded, claims(1_000, 4_600));
    assert!(matches!(
        TokenVerifier::decode_unverified("not-a-token"),
        Err(TokenError::Malformed)
    ));
}

#[test]
fn rejects_revoked_token_fingerprints() {
    let fixture = AuthFixture::new("revocation");
    let issuer = TokenIssuer::from_secret_file(&fixture.secret).expect("issuer");
    let token = issuer
        .issue(&claims(unix_seconds(), unix_seconds() + 600))
        .expect("token");
    let fingerprint = token_fingerprint(&token);
    fixture.revoke(&[fingerprint]);

    let authenticator = fixture.authenticator();
    assert!(matches!(
        authenticator.authenticate(&token, unix_seconds()),
        Err(SecurityError::RevokedToken)
    ));
    assert!(authenticator.is_revoked(fingerprint));
    assert!(matches!(
        RevocationList::parse(r#"hashes = ["not-hex"]"#),
        Err(SecurityError::InvalidRevocation(_))
    ));
}

#[test]
fn reloads_roles_and_revocations_without_restart() {
    let fixture = AuthFixture::new("reload");
    let token = TokenIssuer::from_secret_file(&fixture.secret)
        .expect("issuer")
        .issue(&claims(unix_seconds(), unix_seconds() + 600))
        .expect("token");
    let authenticator = fixture.authenticator();
    let target = AuthTarget {
        keyspace: "game",
        collection: "player",
    };

    let principal = authenticator
        .authenticate(&token, unix_seconds())
        .expect("authenticate");
    assert!(!principal.authorize(target, Action::Write));

    std::thread::sleep(std::time::Duration::from_millis(1_100));
    fixture.write_roles(true);
    let principal = authenticator
        .authenticate(&token, unix_seconds())
        .expect("authenticate reloaded roles");
    assert!(principal.authorize(target, Action::Write));

    std::thread::sleep(std::time::Duration::from_millis(1_100));
    fixture.revoke(&[token_fingerprint(&token)]);
    assert!(matches!(
        authenticator.authenticate(&token, unix_seconds()),
        Err(SecurityError::RevokedToken)
    ));
    assert!(authenticator.is_revoked(token_fingerprint(&token)));
}

struct AuthFixture {
    dir: std::path::PathBuf,
    secret: std::path::PathBuf,
    roles: std::path::PathBuf,
    revocations: std::path::PathBuf,
}

impl AuthFixture {
    fn new(label: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("recolith-security-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let secret = dir.join("secret.hex");
        let roles = dir.join("roles.toml");
        let revocations = dir.join("revoked.toml");
        std::fs::write(&secret, "11".repeat(32)).expect("secret");
        std::fs::write(&revocations, "hashes = []\n").expect("revocations");
        let fixture = Self {
            dir,
            secret,
            roles,
            revocations,
        };
        fixture.write_roles(false);

        fixture
    }

    fn write_roles(&self, writable: bool) {
        let actions = if writable {
            r#"actions = ["read", "write"]"#
        } else {
            r#"actions = ["read"]"#
        };
        std::fs::write(
            &self.roles,
            format!(
                r#"
[[roles]]
name = "game-writer"
permissions = [{{ keyspace = "game", collection = "player", {actions} }}]
"#
            ),
        )
        .expect("roles");
    }

    fn revoke(&self, fingerprints: &[[u8; 32]]) {
        let hashes = fingerprints
            .iter()
            .map(|fingerprint| format!("\"{}\"", fingerprint_hex(fingerprint)))
            .collect::<Vec<_>>()
            .join(", ");
        std::fs::write(&self.revocations, format!("hashes = [{hashes}]\n")).expect("revocations");
    }

    fn authenticator(&self) -> Authenticator {
        Authenticator::from_files_with_revocations(
            &self.secret,
            None::<&std::path::PathBuf>,
            &self.roles,
            Some(&self.revocations),
        )
        .expect("authenticator")
    }
}

impl Drop for AuthFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
