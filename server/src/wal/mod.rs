use std::time::Duration;
use std::error::Error;
use thiserror::Error;

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
const SEGMENT_SIZE_BYTES: usize = 64 * 1000 * 1000 ; // 64MB

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

#[cfg(test)]
mod tests{
    #[test]
    fn it() {

    }
}