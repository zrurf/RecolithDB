//! Index key and value encoding.

mod key;
mod value;

pub use key::{encode_index_entry_prefix, encode_index_key, encode_index_prefix};
pub use value::encode_index_value;
