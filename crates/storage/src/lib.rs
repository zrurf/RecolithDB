//! Schema-aware record and index storage primitives.
#![deny(unsafe_op_in_unsafe_fn)]

pub mod codec;
pub mod commit;
pub mod document;
pub mod encoding;
pub mod error;
pub mod index;
pub mod record;
pub mod store;
pub mod value;

pub use codec::{decode_fields, encode_fields};
pub use commit::CommitClock;
pub use document::{
    DocumentFragments, FragmentData, FragmentMode, FragmentReader, IndexFilter, read_document,
    read_fragment,
};
pub use encoding::document_key::{MAX_DOCUMENT_KEY_LEN, encode_document_key};
pub use encoding::key::{
    RecordKeyHead, decode_record_key_head, encode_bucket_prefix, encode_document_prefix,
    encode_partition_prefix, encode_record_key,
};
pub use error::{Result, StorageError};
pub use index::{
    encode_index_entry_prefix, encode_index_key, encode_index_prefix, encode_index_value,
};
pub use record::{DecodedRecord, HEADER_LEN, RecordHeader, crc32c, decode_record, encode_record};
pub use store::{
    StoredRecord, delete_index_entry, delete_record, get_record, get_record_from_snapshot,
    put_index_entry, put_unique_index_entry, read_fields, read_fields_from_snapshot, scan_index,
    write_fields,
};
pub use value::{RecordData, Value};
