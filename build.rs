use std::env;
use protoc_rust::Customize;

fn main() {
    // TODO: don't need protoc
    protoc_rust::Codegen::new()
        .out_dir("src/raftpb")
        .inputs(&["src/raftpb/raft.proto"])
        .includes(&["src/raftpb"])
        .customize(protoc_rust::Customize {
            carllerche_bytes_for_bytes: Some(true),
            carllerche_bytes_for_string: Some(true),
            ..Default::default()
        })
        .run()
        .expect("protoc must be installed at your system");
}
