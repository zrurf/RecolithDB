/// Appends an order-preserving, self-terminating byte component.
///
/// Embedded NUL bytes are escaped as `0x00 0xFF` and the component ends with
/// `0x00 0x00`, so concatenated components are unambiguous and lexicographic
/// order matches logical order.
pub(crate) fn encode_ordered_bytes(value: &[u8], output: &mut Vec<u8>) {
    for byte in value {
        output.push(*byte);
        if *byte == 0 {
            output.push(0xFF);
        }
    }
    output.extend_from_slice(&[0, 0]);
}
