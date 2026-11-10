use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const CATALOG: &str = r#"
[[keyspaces]]
id = 1
name = "game"

[[schemas]]
id = 1
version = 1
idl = """
record Profile {
    name: string @1;
}
"""

[[buckets]]
id = 1
keyspace_id = 1
name = "player_profile"
partitions = 16
policy = "strict"
schema_id = 1
schema_version = 1

[[collections]]
id = 1
keyspace_id = 1
name = "player"

[[collections.key]]
name = "player_id"
type = "u64"

[[collections.fragments]]
id = 1
name = "profile"
bucket_id = 1
cardinality = "one"
"#;

#[test]
fn starts_with_bootstrap_and_accepts_connections() {
    let port = free_port();
    let dir = temp_dir("process");
    fs::write(dir.join("catalog.toml"), CATALOG).expect("write catalog");
    fs::write(
        dir.join("recolith.toml"),
        format!(
            r#"
[server]
listen = "127.0.0.1:{port}"

[storage]
backend = "memory"

[catalog]
bootstrap = "catalog.toml"
"#
        ),
    )
    .expect("write config");

    let mut child = spawn(&dir);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
            drop(stream);
            break;
        }
        if let Some(status) = child.try_wait().expect("child status") {
            panic!("recolithd exited before accepting connections: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "recolithd did not accept connections"
        );
        thread::sleep(Duration::from_millis(100));
    }

    child.kill().expect("stop recolithd");
    child.wait().expect("wait for recolithd");
    let _ = fs::remove_dir_all(dir);
}

fn spawn(dir: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_recolithd"))
        .arg("--config")
        .arg(dir.join("recolith.toml"))
        .current_dir(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn recolithd")
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("address").port()
}

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("recolith-{label}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).expect("create temp dir");

    path
}

#[test]
fn validates_configuration_without_starting() {
    let dir = temp_dir("check-config");
    fs::write(dir.join("catalog.toml"), CATALOG).expect("write catalog");
    fs::write(
        dir.join("recolith.toml"),
        r#"
[server]
listen = "127.0.0.1:0"

[storage]
backend = "memory"

[catalog]
bootstrap = "catalog.toml"
"#,
    )
    .expect("write config");

    let output = Command::new(env!("CARGO_BIN_EXE_recolithd"))
        .arg("--config")
        .arg(dir.join("recolith.toml"))
        .arg("--check-config")
        .output()
        .expect("check config");
    assert!(output.status.success(), "check failed: {output:?}");
    assert!(
        String::from_utf8(output.stdout)
            .expect("utf8")
            .contains("configuration is valid")
    );

    fs::remove_file(dir.join("catalog.toml")).expect("remove catalog");
    let output = Command::new(env!("CARGO_BIN_EXE_recolithd"))
        .arg("--config")
        .arg(dir.join("recolith.toml"))
        .arg("--check-config")
        .output()
        .expect("check invalid config");
    assert!(!output.status.success());

    let _ = fs::remove_dir_all(dir);
}
