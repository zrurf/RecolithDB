# Example Server

Start the server with the example catalog:

```text
cargo run -p recolith-server --bin recolithd -- --config examples/recolithd/recolith.toml
```

In another terminal, write and read one document:

```text
cargo run -p recolith-cli --bin recolith -- ping
cargo run -p recolith-cli --bin recolith -- describe player --keyspace game
cargo run -p recolith-cli --bin recolith -- doc put --collection player --keyspace game --key player_id=u64:1 --idempotency-key demo-1 --field profile.name=string:alex --field profile.level=u32:10
cargo run -p recolith-cli --bin recolith -- doc get --collection player --keyspace game --key player_id=u64:1 --fragment profile
cargo run -p recolith-cli --bin recolith -- query "SELECT profile.name FROM player WHERE player_id = :id INCLUDE FRAGMENTS profile" --param id=u64:1
```

Named keys and fields are validated against the collection metadata. Numeric
`--field fragment:field=type:value` specifications and `--key-hex` remain available.

The example uses the persistent fjall backend under `examples/recolithd/data/`.
The catalog bootstrap is applied only when the catalog is empty. Later starts reuse
the persisted catalog.

## Authentication

Generate a secret and uncomment the `[auth]` section in `recolith.toml`:

```text
openssl rand -hex 32 > secret.hex
```

```toml
[auth]
secret_file = "secret.hex"
roles_file = "roles.toml"
```

Issue a token locally:

```text
cargo run -p recolith-cli --bin recolith -- token issue --secret-file examples/recolithd/secret.hex --subject admin --role admin
```

Pass it to commands with `--token` or `RECOLITH_TOKEN`.
