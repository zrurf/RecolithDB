use crate::error::{Error, Result};

/// Appends an order-preserving, self-terminating byte component to `output`.
pub fn put_ordered_bytes(value: &[u8], output: &mut Vec<u8>) {
    for byte in value {
        output.push(*byte);
        if *byte == 0 {
            output.push(0xFF);
        }
    }
    output.extend_from_slice(&[0, 0]);
}

/// Appends an unsigned LEB128 varint to `output`.
pub fn put_uvarint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

/// Reads a self-terminating ordered byte component.
///
/// Returns the decoded bytes and the number of input bytes consumed.
///
/// # Errors
///
/// Returns [`Error::InvalidInput`] when the component is truncated or contains
/// an invalid escape sequence.
pub fn get_ordered_bytes(input: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut output = Vec::new();
    let mut offset = 0;
    loop {
        let byte = *input
            .get(offset)
            .ok_or_else(|| Error::InvalidInput("truncated ordered bytes".to_owned()))?;
        offset += 1;
        if byte != 0 {
            output.push(byte);
            continue;
        }
        let escape = *input
            .get(offset)
            .ok_or_else(|| Error::InvalidInput("truncated ordered bytes".to_owned()))?;
        offset += 1;
        match escape {
            0 => return Ok((output, offset)),
            0xFF => output.push(0),
            _ => {
                return Err(Error::InvalidInput(
                    "invalid ordered byte escape".to_owned(),
                ));
            }
        }
    }
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
