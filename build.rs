use protoc_rust::Customize;

fn main () {
    protoc_rust::run(protoc_rust::Args {
        out_dir: "src/raftpb",
        input: &["src/raftpb/raft.proto"],
        includes: &["src/raftpb"],
        customize: Customize{
            carllerche_bytes_for_bytes: Some(true),
            carllerche_bytes_for_string: Some(true),
            ..Default::default()
        },
    }).expect("protoc");
}
