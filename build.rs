use std::env;

fn main() {
    prost_build::compile_protos(&["proto/record.proto"], &["proto"])
        .expect("Failed to compile protobuf files");
}