fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();

    config.out_dir("src/proto");
    config.bytes(["."]);
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    config.compile_protos(&["proto/murrdb.proto"], &["proto"])?;
    Ok(())
}
