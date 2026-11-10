//! Index key and value encoding.

mod key;
mod value;

pub(crate) use key::prefix_end;
pub use key::{encode_index_entry_prefix, encode_index_key, encode_index_prefix};
pub use value::encode_index_value;
pub(crate) use value::{index_values_count, index_values_len};
