mod walpb;

use std::borrow::Cow;
use std::cell::{Ref, RefCell};
use std::time::Duration;
use std::error::Error;
use std::fs::File;
use thiserror::Error;
use eraft_rs::raftpb::raft::HardState;
use crate::wal::walpb::SnapShot;

pub enum MetaDataType {
    Entry,
    State,
    CrcType,
    Snapshot,
    // warnSyncDuration is the amount of time allotted to an fsync before
    // logging a warning
    WarnSyncDuration(Duration),
}

// SEGMENT_SIZE_BYTES is the preallocated size of each wal segment file.
// The actual size might be larger than this. In general, the default
// value should be used, but this is defined as an exported variable
// so that tests can set a different segment size.
const SEGMENT_SIZE_BYTES: usize = 64 * 1000 * 1000; // 64MB

#[derive(Error, Clone, Debug, PartialEq)]
pub enum WalError {
    #[error("wal: conflicting metadata found")]
    MetadataConflict,
    #[error("wal: file not found")]
    FileNotFound,
    #[error("wal: crc mismatch")]
    CRCMismatch,
    #[error("wal: snapshot mismatch")]
    SnapshotMismatch,
    #[error("wal: snapshot not found")]
    SnapshotNotFound,
    #[error("wal: slice bounds out of range")]
    SliceOutOfRange,
    #[error("wal: decoder not found")]
    DecoderNotFound,
}

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
pub struct Wal {
    dir: String,
    // dirFile is a fd for the wal directory for syncing on Rename
    DirFile: Option<File>,
    metadata: Vec<u8>,
    state: HardState,
    start: SnapShot,
    no_sync: bool,
    enti: u64,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it() {}
}
