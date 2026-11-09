//! Schema-aware field codec.

mod decode;
mod encode;
mod wire;

pub use decode::decode_fields;
pub use encode::encode_fields;
