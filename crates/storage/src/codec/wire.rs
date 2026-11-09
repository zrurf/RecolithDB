use recolith_schema::{Type, WireType};

pub(crate) const WIRE_VARINT: u8 = 0;
pub(crate) const WIRE_I64: u8 = 1;
pub(crate) const WIRE_I32: u8 = 2;
pub(crate) const WIRE_F64: u8 = 3;
pub(crate) const WIRE_F32: u8 = 4;
pub(crate) const WIRE_BYTES: u8 = 5;
pub(crate) const WIRE_STRING: u8 = 6;
pub(crate) const WIRE_LIST: u8 = 7;
pub(crate) const WIRE_MAP: u8 = 8;
pub(crate) const WIRE_NESTED: u8 = 9;

pub(crate) fn wire_byte(wire: WireType) -> u8 {
    match wire {
        WireType::Varint => WIRE_VARINT,
        WireType::I64 => WIRE_I64,
        WireType::I32 => WIRE_I32,
        WireType::F64 => WIRE_F64,
        WireType::F32 => WIRE_F32,
        WireType::Bytes => WIRE_BYTES,
        WireType::String => WIRE_STRING,
        WireType::List => WIRE_LIST,
        WireType::Map => WIRE_MAP,
        WireType::Nested => WIRE_NESTED,
    }
}

pub(crate) fn field_wire(ty: &Type) -> u8 {
    wire_byte(ty.wire_type())
}
