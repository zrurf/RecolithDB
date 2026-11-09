# Example Server

Start the server with the example catalog:

```text
cargo run -p recolith-server --bin recolithd -- --config examples/recolithd/recolith.toml
```

In another terminal:

```text
cargo run -p recolith-cli --bin recolith -- ping
cargo run -p recolith-cli --bin recolith -- query "SELECT profile.name FROM player WHERE player_id = :id INCLUDE FRAGMENTS profile" --param id=u64:1
```

The example uses the persistent fjall backend under `examples/recolithd/data/`.
The catalog bootstrap is applied only when the catalog is empty. Later starts reuse
the persisted catalog.
