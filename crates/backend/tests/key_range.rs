use recolith_backend::KeyRange;

#[test]
fn prefix_range_contains_matching_keys() {
    let range = KeyRange::prefix(b"player:");

    assert!(range.contains(b"player:1"));
    assert!(range.contains(b"player:2"));
    assert!(!range.contains(b"player"));
    assert!(!range.contains(b"players"));
}

#[test]
fn prefix_range_handles_max_byte() {
    let range = KeyRange::prefix(&[0xFF]);

    assert!(range.contains(&[0xFF, 0x00]));
    assert!(!range.contains(&[0xFE]));
}
