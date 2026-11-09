//! Record framing and headers.

pub mod checksum;
mod header;

pub mod frame;

pub use checksum::crc32c;
pub use frame::{DecodedRecord, HEADER_LEN, decode_record, encode_record};
pub use header::RecordHeader;
