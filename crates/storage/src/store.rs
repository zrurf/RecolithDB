//! Backend-backed record and index operations.

pub mod index;
pub mod record;

pub use index::{delete_index_entry, put_index_entry, put_unique_index_entry, scan_index};
pub use record::{
    StoredRecord, delete_record, get_record, get_record_from_snapshot, read_fields,
    read_fields_from_snapshot, write_fields,
};
