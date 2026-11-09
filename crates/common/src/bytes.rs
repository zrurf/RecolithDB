use crate::error::{Error, Result};

/// Appends an unsigned LEB128 varint to `output`.
pub fn put_uvarint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

/// Reads an unsigned LEB128 varint and returns the value and consumed length.
///
/// # Errors
///
/// Returns [`Error::InvalidInput`] when the input is truncated or overflows a `u64`.
pub fn get_uvarint(input: &[u8]) -> Result<(u64, usize)> {
    let mut value = 0_u64;

    for (index, byte) in input.iter().enumerate() {
        let shift = index * 7;
        if shift >= 64 {
            return Err(Error::InvalidInput("varint is too long".to_owned()));
        }

        let part = u64::from(byte & 0x7F);
        if shift == 63 && part > 1 {
            return Err(Error::InvalidInput("varint overflows u64".to_owned()));
        }

        value |= part << shift;
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }

    Err(Error::InvalidInput("truncated varint".to_owned()))
}
