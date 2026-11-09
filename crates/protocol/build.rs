fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/recolith/protocol/v1/messages.proto");

    prost_build::Config::new()
        .compile_protos(&["proto/recolith/protocol/v1/messages.proto"], &["proto"])?;

    Ok(())
}
