use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use recolith_security::{TokenVerifier, fingerprint_hex, token_fingerprint, unix_seconds};

struct Fixture {
    dir: PathBuf,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("recolith-cli-token-{label}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("temp dir");

        Self { dir }
    }

    fn secret(&self, name: &str, value: &str) -> PathBuf {
        let path = self.dir.join(name);
        fs::write(&path, value.repeat(32)).expect("secret file");

        path
    }

    fn token(&self, name: &str, token: &str) -> PathBuf {
        let path = self.dir.join(name);
        fs::write(&path, token).expect("token file");

        path
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn recolith() -> Command {
    Command::new(env!("CARGO_BIN_EXE_recolith"))
}

fn issue(secret: &PathBuf) -> String {
    let output = recolith()
        .args(["token", "issue", "--secret-file"])
        .arg(secret)
        .args(["--subject", "game-service", "--role", "game-writer"])
        .args(["--ttl", "600"])
        .output()
        .expect("run issue");
    assert!(output.status.success(), "issue failed: {output:?}");

    String::from_utf8(output.stdout)
        .expect("utf8")
        .trim()
        .to_owned()
}

#[test]
fn issues_inspects_and_verifies_tokens_without_a_server() {
    let fixture = Fixture::new("verify");
    let secret = fixture.secret("secret.hex", "11");
    let previous = fixture.secret("previous.hex", "22");
    let token = issue(&secret);
    let token_file = fixture.token("token.txt", &token);

    let verifier = TokenVerifier::from_secret_files(&secret, None::<&PathBuf>).expect("verifier");
    let claims = verifier.verify(&token, unix_seconds()).expect("verify");
    assert_eq!(claims.subject, "game-service");
    assert_eq!(claims.roles, vec!["game-writer".to_owned()]);

    let inspected = recolith()
        .args(["token", "inspect", "--token-file"])
        .arg(&token_file)
        .output()
        .expect("run inspect");
    assert!(inspected.status.success(), "inspect failed: {inspected:?}");
    let inspected = String::from_utf8(inspected.stdout).expect("utf8");
    assert!(inspected.contains("verified: false"));
    assert!(inspected.contains("subject: game-service"));
    assert!(inspected.contains("roles: game-writer"));
    assert!(inspected.contains("(valid)"));
    assert!(inspected.contains("fingerprint:"));

    let verified = recolith()
        .args(["token", "verify", "--secret-file"])
        .arg(&secret)
        .args(["--token-file"])
        .arg(&token_file)
        .output()
        .expect("run verify");
    assert!(verified.status.success(), "verify failed: {verified:?}");
    assert!(
        String::from_utf8(verified.stdout)
            .expect("utf8")
            .contains("verified: true")
    );

    let rotated = recolith()
        .args(["token", "verify", "--secret-file"])
        .arg(&previous)
        .args(["--previous-secret-file"])
        .arg(&secret)
        .args(["--token-file"])
        .arg(&token_file)
        .output()
        .expect("run verify with previous");
    assert!(
        rotated.status.success(),
        "rotate verify failed: {rotated:?}"
    );
}

#[test]
fn reads_tokens_from_stdin() {
    let fixture = Fixture::new("stdin");
    let secret = fixture.secret("secret.hex", "11");
    let token = issue(&secret);

    let mut child = recolith()
        .args(["token", "inspect"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn inspect");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(token.as_bytes())
        .expect("write token");
    let output = child.wait_with_output().expect("inspect output");
    assert!(output.status.success(), "inspect failed: {output:?}");
    assert!(
        String::from_utf8(output.stdout)
            .expect("utf8")
            .contains("subject: game-service")
    );
}

#[test]
fn rejects_invalid_signatures_and_malformed_input() {
    let fixture = Fixture::new("reject");
    let secret = fixture.secret("secret.hex", "11");
    let other = fixture.secret("other.hex", "22");
    let token = issue(&secret);
    let token_file = fixture.token("token.txt", &token);

    let wrong = recolith()
        .args(["token", "verify", "--secret-file"])
        .arg(&other)
        .args(["--token-file"])
        .arg(&token_file)
        .output()
        .expect("run verify");
    assert!(!wrong.status.success());
    assert!(String::from_utf8_lossy(&wrong.stderr).contains("signature"));

    let malformed = fixture.token("malformed.txt", "not-a-token");
    let inspected = recolith()
        .args(["token", "inspect", "--token-file"])
        .arg(&malformed)
        .output()
        .expect("run inspect");
    assert!(!inspected.status.success());
    assert!(String::from_utf8_lossy(&inspected.stderr).contains("malformed"));

    let empty = fixture.token("empty.txt", "  \n");
    let inspected = recolith()
        .args(["token", "inspect", "--token-file"])
        .arg(&empty)
        .output()
        .expect("run inspect");
    assert!(!inspected.status.success());
    assert!(String::from_utf8_lossy(&inspected.stderr).contains("empty"));
}

#[test]
fn prints_fingerprints_for_revocation() {
    let fixture = Fixture::new("hash");
    let secret = fixture.secret("secret.hex", "11");
    let token = issue(&secret);
    let token_file = fixture.token("token.txt", &token);

    let output = recolith()
        .args(["token", "hash", "--token-file"])
        .arg(&token_file)
        .output()
        .expect("run hash");
    assert!(output.status.success(), "hash failed: {output:?}");
    let printed = String::from_utf8(output.stdout).expect("utf8");
    assert_eq!(printed.trim(), fingerprint_hex(&token_fingerprint(&token)));
}

#[test]
fn revokes_tokens_into_a_revocation_file() {
    let fixture = Fixture::new("revoke");
    let secret = fixture.secret("secret.hex", "11");
    let token = issue(&secret);
    let token_file = fixture.token("token.txt", &token);
    let revoked = fixture.dir.join("revoked.toml");
    let fingerprint = fingerprint_hex(&token_fingerprint(&token));

    let output = recolith()
        .args(["token", "revoke", "--token-file"])
        .arg(&token_file)
        .args(["--revoked-file"])
        .arg(&revoked)
        .output()
        .expect("revoke");
    assert!(output.status.success(), "revoke failed: {output:?}");
    assert!(
        String::from_utf8(output.stdout)
            .expect("utf8")
            .contains(&fingerprint)
    );
    let contents = std::fs::read_to_string(&revoked).expect("revocation file");
    assert!(contents.contains(&fingerprint));

    let output = recolith()
        .args(["token", "revoke", "--token-file"])
        .arg(&token_file)
        .args(["--revoked-file"])
        .arg(&revoked)
        .output()
        .expect("revoke again");
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .expect("utf8")
            .contains("already revoked")
    );
    let contents = std::fs::read_to_string(&revoked).expect("revocation file");
    assert_eq!(contents.matches(&fingerprint).count(), 1);
}
