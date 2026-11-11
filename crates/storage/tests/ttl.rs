use recolith_common::{CollectionId, PartitionId};
use recolith_storage::{
    decode_ttl_index_key, decode_ttl_value, encode_ttl_index_key, encode_ttl_key, encode_ttl_value,
};

#[test]
fn encodes_and_orders_ttl_keys() {
    let collection = CollectionId::new(7);
    let partition = PartitionId::new(3);
    let document = b"player:42";
    let early = encode_ttl_index_key(1_000, collection, partition, document);
    let late = encode_ttl_index_key(2_000, collection, partition, document);
    assert!(early < late);

    let entry = decode_ttl_index_key(&late).expect("decode");
    assert_eq!(entry.expires_at_micros(), 2_000);
    assert_eq!(entry.collection(), collection);
    assert_eq!(entry.partition(), partition);
    assert_eq!(entry.document_key(), document);

    let metadata = encode_ttl_key(collection, partition, document);
    assert_ne!(metadata, early);
    assert_eq!(decode_ttl_value(&encode_ttl_value(42)), Some(42));
}

#[test]
fn rejects_malformed_ttl_values_and_keys() {
    assert_eq!(decode_ttl_value(&[1, 2, 3]), None);
    assert_eq!(decode_ttl_index_key(b"short"), None);
    assert_eq!(
        decode_ttl_index_key(&encode_ttl_key(
            CollectionId::new(1),
            PartitionId::new(0),
            b"doc"
        )),
        None
    );
}
