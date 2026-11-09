use std::fs;
use std::path::PathBuf;

use recolith_server::{Config, ConfigError, StorageSettings};

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("recolith-config-{label}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).expect("create temp dir");

    path
}

#[test]
fn loads_config_and_resolves_relative_paths() {
    let dir = temp_dir("relative");
    let config_path = dir.join("recolith.toml");
    fs::write(
        &config_path,
        r#"
[server]
listen = "127.0.0.1:7420"
auth_token = "secret"

[storage]
backend = "fjall"
path = "data"

[tls]
certificate_chain = "certs/server.pem"
private_key = "certs/server.key"

[catalog]
bootstrap = "catalog.toml"
"#,
    )
    .expect("write config");

    let config = Config::load(&config_path).expect("load config");
    assert_eq!(
        config.listen_addr().expect("listen address").to_string(),
        "127.0.0.1:7420"
    );
    match &config.storage {
        StorageSettings::Fjall { path } => assert_eq!(path, &dir.join("data")),
        StorageSettings::Memory => panic!("unexpected memory backend"),
    }
    let tls = config.tls.as_ref().expect("tls");
    assert_eq!(tls.certificate_chain, dir.join("certs/server.pem"));
    assert_eq!(tls.private_key, dir.join("certs/server.key"));
    assert_eq!(
        config.catalog.bootstrap.as_ref().expect("bootstrap"),
        &dir.join("catalog.toml")
    );

    let server = config.server_config().expect("server config");
    assert_eq!(server.auth_token.as_deref(), Some(b"secret".as_slice()));
    assert!(server.tls.is_some());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn rejects_unknown_configuration_fields() {
    let dir = temp_dir("unknown");
    let config_path = dir.join("recolith.toml");
    fs::write(
        &config_path,
        r#"
[server]
listen = "127.0.0.1:7420"
unexpected = true

[storage]
backend = "memory"
"#,
    )
    .expect("write config");

    assert!(matches!(
        Config::load(&config_path),
        Err(ConfigError::Parse { .. })
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn rejects_insecure_remote_plaintext() {
    let dir = temp_dir("insecure");
    let config_path = dir.join("recolith.toml");
    fs::write(
        &config_path,
        r#"
[server]
listen = "0.0.0.0:7420"
allow_plaintext_remote = true

[storage]
backend = "memory"
"#,
    )
    .expect("write config");

    assert!(matches!(
        Config::load(&config_path),
        Err(ConfigError::Invalid(_))
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn accepts_remote_plaintext_with_auth_token() {
    let dir = temp_dir("remote-token");
    let config_path = dir.join("recolith.toml");
    fs::write(
        &config_path,
        r#"
[server]
listen = "0.0.0.0:7420"
allow_plaintext_remote = true
auth_token = "secret"

[storage]
backend = "memory"
"#,
    )
    .expect("write config");

    let config = Config::load(&config_path).expect("load config");
    assert_eq!(config.server.auth_token.as_deref(), Some("secret"));

    let _ = fs::remove_dir_all(dir);
}
