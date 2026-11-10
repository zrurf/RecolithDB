use recolith_security::{
    Action, AuthTarget, MAX_TTL_SECONDS, RoleSet, TokenClaims, TokenError, TokenIssuer,
    TokenVerifier,
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
