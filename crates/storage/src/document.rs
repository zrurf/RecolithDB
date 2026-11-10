use std::collections::{BTreeMap, BTreeSet};

use recolith_backend::{KeyRange, ScanOptions, Snapshot};
use recolith_common::bytes::get_uvarint;
use recolith_common::{BucketId, FieldId, FragmentId, IndexId, PartitionId};
use recolith_schema::RecordSchema;

use crate::codec::decode_fields;
use crate::encoding::key::{encode_document_prefix, encode_record_key};
use crate::error::{Result, StorageError};
use crate::record::{RecordHeader, decode_record};
use crate::store::index::{scan_index, scan_index_components, scan_index_range};
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

/// One bound of an encoded index range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexBound {
    encoded_values: Vec<u8>,
    inclusive: bool,
}

impl IndexBound {
    /// Creates an index bound from already encoded index values.
    #[must_use]
    pub fn new(encoded_values: impl Into<Vec<u8>>, inclusive: bool) -> Self {
        Self {
            encoded_values: encoded_values.into(),
            inclusive,
        }
    }

    /// Returns the encoded index values.
    #[must_use]
    pub fn encoded_values(&self) -> &[u8] {
        &self.encoded_values
    }

    /// Returns whether the bound is inclusive.
    #[must_use]
    pub const fn inclusive(&self) -> bool {
        self.inclusive
    }
}

/// Index lookup applied while reading a `many` fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexFilter {
    /// Equality lookup on encoded index values.
    Equality {
        /// Index identifier.
        index: IndexId,
        /// Encoded index values.
        encoded_values: Vec<u8>,
        /// Number of index components before the record key.
        components: usize,
    },
    /// Ordered range lookup on encoded index values.
    Range {
        /// Index identifier.
        index: IndexId,
        /// Lower bound.
        lower: Option<IndexBound>,
        /// Upper bound.
        upper: Option<IndexBound>,
        /// Number of index components before the record key.
        components: usize,
    },
}

impl IndexFilter {
    /// Creates an index equality filter from already encoded index values.
    #[must_use]
    pub fn new(index: IndexId, encoded_values: impl Into<Vec<u8>>) -> Self {
        Self::new_with_components(index, encoded_values, 1)
    }

    /// Creates an equality filter with an explicit component count.
    #[must_use]
    pub fn new_with_components(
        index: IndexId,
        encoded_values: impl Into<Vec<u8>>,
        components: usize,
    ) -> Self {
        Self::Equality {
            index,
            encoded_values: encoded_values.into(),
            components,
        }
    }

    /// Creates an index range filter from already encoded bounds.
    #[must_use]
    pub fn range(index: IndexId, lower: Option<IndexBound>, upper: Option<IndexBound>) -> Self {
        Self::range_with_components(index, lower, upper, 1)
    }

    /// Creates a range filter with an explicit component count.
    #[must_use]
    pub fn range_with_components(
        index: IndexId,
        lower: Option<IndexBound>,
        upper: Option<IndexBound>,
        components: usize,
    ) -> Self {
        Self::Range {
            index,
            lower,
            upper,
            components,
        }
    }

    /// Returns the index identifier.
    #[must_use]
    pub const fn index(&self) -> IndexId {
        match self {
            Self::Equality { index, .. } | Self::Range { index, .. } => *index,
        }
    }

    /// Returns equality values, if this is an equality filter.
    #[must_use]
    pub fn encoded_values(&self) -> Option<&[u8]> {
        match self {
            Self::Equality { encoded_values, .. } => Some(encoded_values),
            Self::Range { .. } => None,
        }
    }

    /// Returns range bounds, if this is a range filter.
    #[must_use]
    pub fn bounds(&self) -> Option<(Option<&IndexBound>, Option<&IndexBound>)> {
        match self {
            Self::Equality { .. } => None,
            Self::Range { lower, upper, .. } => Some((lower.as_ref(), upper.as_ref())),
        }
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
    indexes: Vec<IndexFilter>,
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
            indexes: Vec::new(),
        }
    }

    /// Reads a `many` fragment through an index equality lookup.
    ///
    /// The index is ignored for `one` fragments.
    #[must_use]
    pub fn with_index(mut self, index: IndexId, encoded_values: impl Into<Vec<u8>>) -> Self {
        self.indexes.push(IndexFilter::new(index, encoded_values));
        self
    }

    /// Reads a `many` fragment through an index equality lookup with a component count.
    #[must_use]
    pub fn with_index_components(
        mut self,
        index: IndexId,
        encoded_values: impl Into<Vec<u8>>,
        components: usize,
    ) -> Self {
        self.indexes.push(IndexFilter::new_with_components(
            index,
            encoded_values,
            components,
        ));
        self
    }

    /// Reads a `many` fragment through a single-field index range.
    ///
    /// The index is ignored for `one` fragments.
    #[must_use]
    pub fn with_index_range(
        mut self,
        index: IndexId,
        lower: Option<IndexBound>,
        upper: Option<IndexBound>,
    ) -> Self {
        self.indexes.push(IndexFilter::range(index, lower, upper));
        self
    }

    /// Reads a `many` fragment through a range filter with a component count.
    #[must_use]
    pub fn with_index_range_components(
        mut self,
        index: IndexId,
        lower: Option<IndexBound>,
        upper: Option<IndexBound>,
        components: usize,
    ) -> Self {
        self.indexes.push(IndexFilter::range_with_components(
            index, lower, upper, components,
        ));
        self
    }

    /// Returns the fragment identifier.
    #[must_use]
    pub fn fragment_id(&self) -> FragmentId {
        self.fragment
    }

    /// Returns the first index filter, if any.
    #[must_use]
    pub fn index_filter(&self) -> Option<&IndexFilter> {
        self.indexes.first()
    }

    /// Returns all index filters.
    #[must_use]
    pub fn index_filters(&self) -> &[IndexFilter] {
        &self.indexes
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
        FragmentMode::Many => {
            if fragment.index_filters().is_empty() {
                read_many(snapshot, document_key, fragment)
            } else {
                read_many_by_indexes(snapshot, document_key, fragment)
            }
        }
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

fn read_many_by_indexes<S: Snapshot>(
    snapshot: &S,
    document_key: &[u8],
    fragment: &FragmentReader,
) -> Result<Vec<FragmentData>> {
    let mut filters = fragment.index_filters().iter();
    let Some(first) = filters.next() else {
        return read_many(snapshot, document_key, fragment);
    };
    let first_keys = scan_index_filter(snapshot, fragment, first)?;
    let mut allowed: Option<BTreeSet<Vec<u8>>> = None;
    for filter in filters {
        let keys = scan_index_filter(snapshot, fragment, filter)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        allowed = Some(match allowed {
            Some(current) => current.intersection(&keys).cloned().collect(),
            None => keys,
        });
    }
    let record_keys = match allowed {
        Some(allowed) => first_keys
            .into_iter()
            .filter(|key| allowed.contains(key))
            .collect(),
        None => first_keys,
    };

    read_many_by_record_keys(snapshot, document_key, fragment, record_keys)
}

fn scan_index_filter<S: Snapshot>(
    snapshot: &S,
    fragment: &FragmentReader,
    filter: &IndexFilter,
) -> Result<Vec<Vec<u8>>> {
    match filter {
        IndexFilter::Equality {
            index,
            encoded_values,
            components,
        } => {
            if *components == 1 {
                scan_index(
                    snapshot,
                    fragment.bucket,
                    fragment.partition,
                    *index,
                    encoded_values,
                    ScanOptions::default(),
                )
            } else {
                scan_index_components(
                    snapshot,
                    fragment.bucket,
                    fragment.partition,
                    *index,
                    encoded_values,
                    *components,
                    ScanOptions::default(),
                )
            }
        }
        IndexFilter::Range {
            index,
            lower,
            upper,
            components,
        } => scan_index_range(
            snapshot,
            fragment.bucket,
            fragment.partition,
            *index,
            lower
                .as_ref()
                .map(|bound| (bound.encoded_values(), bound.inclusive())),
            upper
                .as_ref()
                .map(|bound| (bound.encoded_values(), bound.inclusive())),
            *components,
            ScanOptions::default(),
        ),
    }
}

fn read_many_by_record_keys<S: Snapshot>(
    snapshot: &S,
    document_key: &[u8],
    fragment: &FragmentReader,
    record_keys: Vec<Vec<u8>>,
) -> Result<Vec<FragmentData>> {
    let prefix = fragment_prefix(fragment, document_key);
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
