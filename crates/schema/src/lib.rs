//! Schema types, validation, compatibility rules, and minimal IDL parsing.
#![forbid(unsafe_code)]

pub mod compat;
pub mod error;
pub mod field;
pub mod idl;
pub mod record;
pub mod types;
pub mod validate;

pub use compat::{Compatibility, check_compatibility};
pub use error::{Result, SchemaError};
pub use field::Field;
pub use idl::parse_record;
pub use record::RecordSchema;
pub use types::{EnumSchema, Type, WireType};
pub use validate::validate_record;
