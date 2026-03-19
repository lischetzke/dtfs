fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    let protos = &[
        format!("{}/common.proto", proto_dir),
        format!("{}/data.proto", proto_dir),
        format!("{}/manager.proto", proto_dir),
        format!("{}/cluster.proto", proto_dir),
        format!("{}/admin.proto", proto_dir),
    ];

    // Use protox to parse protos (no external protoc binary needed)
    let file_descriptors = protox::compile(protos, &[proto_dir])?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(file_descriptors)?;

    Ok(())
}
