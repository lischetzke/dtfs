fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    let protos = &[
        format!("{}/common.proto", proto_dir),
        format!("{}/data.proto", proto_dir),
        format!("{}/manager.proto", proto_dir),
    ];

    // Re-run when any of the .proto files change. Without this Cargo never
    // re-runs `build.rs` on proto-only edits and the generated code drifts
    // from the live schema — discovered while fixing F-014.
    println!("cargo:rerun-if-changed=build.rs");
    for p in protos {
        println!("cargo:rerun-if-changed={}", p);
    }

    // Use protox to parse protos (no external protoc binary needed)
    let file_descriptors = protox::compile(protos, &[proto_dir])?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(file_descriptors)?;

    Ok(())
}
