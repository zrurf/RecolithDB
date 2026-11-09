use std::collections::{BTreeMap, BTreeSet};

use recolith_backend::{KeyRange, ScanOptions, Snapshot};
use recolith_common::bytes::get_uvarint;
use recolith_common::{BucketId, FieldId, FragmentId, IndexId, PartitionId};
use recolith_schema::RecordSchema;

use crate::codec::decode_fields;
use crate::encoding::key::{encode_document_prefix, encode_record_key};
use crate::error::{Result, StorageError};
use crate::record::{RecordHeader, decode_record};
use crate::store::index::scan_index;
use crate::store::record::ensure_schema;
use crate::value::RecordData;

/// Fragment cardinality for document assembly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentMode {
    /// Read at most one record with an empty subkey.
    One,
    /// Read every record under the fragment prefix.
    Many,
}

/// Index equality lookup applied while reading a `many` fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexFilter {
    index: IndexId,
    encoded_values: Vec<u8>,
}

impl IndexFilter {
    /// Creates an index equality filter from already encoded index values.
    #[must_use]
    pub fn new(index: IndexId, encoded_values: impl Into<Vec<u8>>) -> Self {
        Self {
            index,
            encoded_values: encoded_values.into(),
        }
    }

    /// Returns the index identifier.
    #[must_use]
    pub fn index(&self) -> IndexId {
        self.index
    }

    /// Returns the encoded index values.
    #[must_use]
    pub fn encoded_values(&self) -> &[u8] {
        &self.encoded_values
    }
}

/// Fragment read descriptor.
#[derive(Clone, Debug)]
pub struct FragmentReader {
    bucket: BucketId,
    partition: PartitionId,
    fragment: FragmentId,
    mode: FragmentMode,
    schema: RecordSchema,
    index: Option<IndexFilter>,
}

impl FragmentReader {
    /// Creates a fragment reader.
    #[must_use]
    pub fn new(
        bucket: BucketId,
        partition: PartitionId,
        fragment: FragmentId,
        mode: FragmentMode,
        schema: RecordSchema,
    ) -> Self {
        Self {
            bucket,
            partition,
            fragment,
            mode,
            schema,
            index: None,
        }
    }

    /// Reads a `many` fragment through an index equality lookup.
    ///
    /// The index is ignored for `one` fragments.
    #[must_use]
    pub fn with_index(mut self, index: IndexId, encoded_values: impl Into<Vec<u8>>) -> Self {
        self.index = Some(IndexFilter::new(index, encoded_values));
        self
    }

    /// Returns the fragment identifier.
    #[must_use]
    pub fn fragment_id(&self) -> FragmentId {
        self.fragment
    }

    /// Returns the index filter, if any.
    #[must_use]
    pub fn index_filter(&self) -> Option<&IndexFilter> {
        self.index.as_ref()
    }
}

/// Decoded fragment record.
#[derive(Clone, Debug, PartialEq)]
pub struct FragmentData {
    subkey: Vec<u8>,
    header: RecordHeader,
    data: RecordData,
}

impl FragmentData {
    /// Returns the fragment subkey.
    #[must_use]
    pub fn subkey(&self) -> &[u8] {
        &self.subkey
    }

    /// Returns the record header.
    #[must_use]
    pub fn header(&self) -> &RecordHeader {
        &self.header
    }

    /// Returns the decoded fields.
    #[must_use]
    pub fn data(&self) -> &RecordData {
        &self.data
    }

    /// Returns a copy containing only the selected known fields.
    ///
    /// An empty field set keeps all known fields.
    #[must_use]
    pub fn project(&self, fields: &BTreeSet<FieldId>) -> Self {
        if fields.is_empty() {
            return self.clone();
        }

        let mut data = RecordData::new();
        for field in fields {
            if let Some(value) = self.data.get(*field) {
                data.insert(*field, value.clone());
            }
        }

        Self {
            subkey: self.subkey.clone(),
            header: self.header.clone(),
            data,
        }
    }
}

/// Fragments assembled for one document.
pub type DocumentFragments = BTreeMap<FragmentId, Vec<FragmentData>>;

/// Reads and assembles the requested document fragments.
///
/// # Errors
///
/// Returns [`StorageError`] for backend, frame, schema, or key decoding failures.
pub fn read_document<S: Snapshot>(
    snapshot: &S,
    document_key: &[u8],
    fragments: &[FragmentReader],
) -> Result<DocumentFragments> {
    let mut assembled = BTreeMap::new();

    for fragment in fragments {
        let records = read_fragment(snapshot, document_key, fragment)?;
        if !records.is_empty() {
            assembled.insert(fragment.fragment_id(), records);
        }
    }

    Ok(assembled)
}

/// Reads one fragment of a document.
///
/// # Errors
///
/// Returns [`StorageError`] for backend, frame, schema, or key decoding failures.
pub fn read_fragment<S: Snapshot>(
    snapshot: &S,
    document_key: &[u8],
    fragment: &FragmentReader,
) -> Result<Vec<FragmentData>> {
    match fragment.mode {
        FragmentMode::One => read_one(snapshot, document_key, fragment),
        FragmentMode::Many => match fragment.index_filter() {
            Some(filter) => read_many_by_index(snapshot, document_key, fragment, filter),
            None => read_many(snapshot, document_key, fragment),
        },
    }
}

fn read_one<S: Snapshot>(
    snapshot: &S,
    document_key: &[u8],
    fragment: &FragmentReader,
) -> Result<Vec<FragmentData>> {
    let key = encode_record_key(
        fragment.bucket,
        fragment.partition,
        document_key,
        fragment.fragment,
        &[],
    );
    let Some(frame) = snapshot.get(&key)? else {
        return Ok(Vec::new());
    };
    let decoded = decode_record(&frame)?;
    ensure_schema(decoded.header(), &fragment.schema)?;
    let data = decode_fields(&fragment.schema, decoded.payload())?;

    Ok(vec![FragmentData {
        subkey: Vec::new(),
        header: decoded.header().clone(),
        data,
    }])
}

fn read_many<S: Snapshot>(
    snapshot: &S,
    document_key: &[u8],
    fragment: &FragmentReader,
) -> Result<Vec<FragmentData>> {
    let prefix = fragment_prefix(fragment, document_key);
    let range = KeyRange::prefix(&prefix);
    let entries = snapshot.scan(&range, ScanOptions::default())?;
    let mut records = Vec::with_capacity(entries.len());

    for (key, frame) in entries {
        let subkey = decode_subkey(&key, prefix.len())?;
        let decoded = decode_record(&frame)?;
        ensure_schema(decoded.header(), &fragment.schema)?;
        let data = decode_fields(&fragment.schema, decoded.payload())?;
        records.push(FragmentData {
            subkey,
            header: decoded.header().clone(),
            data,
        });
    }

    Ok(records)
}

fn read_many_by_index<S: Snapshot>(
    snapshot: &S,
    document_key: &[u8],
    fragment: &FragmentReader,
    filter: &IndexFilter,
) -> Result<Vec<FragmentData>> {
    let prefix = fragment_prefix(fragment, document_key);
    let record_keys = scan_index(
        snapshot,
        fragment.bucket,
        fragment.partition,
        filter.index(),
        filter.encoded_values(),
        ScanOptions::default(),
    )?;
    let mut records = Vec::with_capacity(record_keys.len());

    for record_key in record_keys {
        if !record_key.starts_with(&prefix) {
            continue;
        }
        let Some(frame) = snapshot.get(&record_key)? else {
            continue;
        };
        let subkey = decode_subkey(&record_key, prefix.len())?;
        let decoded = decode_record(&frame)?;
        ensure_schema(decoded.header(), &fragment.schema)?;
        let data = decode_fields(&fragment.schema, decoded.payload())?;
        records.push(FragmentData {
            subkey,
            header: decoded.header().clone(),
            data,
        });
    }

    Ok(records)
}

fn fragment_prefix(fragment: &FragmentReader, document_key: &[u8]) -> Vec<u8> {
    let mut prefix = encode_document_prefix(fragment.bucket, fragment.partition, document_key);
    prefix.extend_from_slice(&fragment.fragment.get().to_be_bytes());
    prefix
}

fn decode_subkey(key: &[u8], offset: usize) -> Result<Vec<u8>> {
    let tail = key
        .get(offset..)
        .ok_or_else(|| StorageError::InvalidKey("fragment prefix exceeds key".to_owned()))?;
    let (length, consumed) =
        get_uvarint(tail).map_err(|error| StorageError::InvalidKey(error.to_string()))?;
    let length = usize::try_from(length)
        .map_err(|_| StorageError::InvalidKey("subkey length overflows".to_owned()))?;
    let start = offset
        .checked_add(consumed)
        .ok_or_else(|| StorageError::InvalidKey("subkey offset overflows".to_owned()))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| StorageError::InvalidKey("subkey length overflows".to_owned()))?;
    if end != key.len() {
        return Err(StorageError::InvalidKey(
            "subkey length does not match key".to_owned(),
        ));
    }

    Ok(key[start..end].to_vec())
}
