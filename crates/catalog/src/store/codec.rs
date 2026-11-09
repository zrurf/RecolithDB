use recolith_common::bytes::{get_uvarint, put_uvarint};
use recolith_common::{
    BackendId, BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, SchemaId,
    SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_schema::{EnumSchema, Field, RecordSchema, Type};

use crate::error::{CatalogError, Result};
use crate::placement::BucketPlacement;

const METADATA_FORMAT_VERSION: u8 = 1;

const TYPE_BOOL: u8 = 0;
const TYPE_I32: u8 = 1;
const TYPE_I64: u8 = 2;
const TYPE_U32: u8 = 3;
const TYPE_U64: u8 = 4;
const TYPE_F32: u8 = 5;
const TYPE_F64: u8 = 6;
const TYPE_STRING: u8 = 7;
const TYPE_BYTES: u8 = 8;
const TYPE_TIMESTAMP: u8 = 9;
const TYPE_DURATION: u8 = 10;
const TYPE_ENUM: u8 = 11;
const TYPE_LIST: u8 = 12;
const TYPE_MAP: u8 = 13;
const TYPE_RECORD: u8 = 14;

const POLICY_STRICT: u8 = 0;
const POLICY_MANAGED: u8 = 1;
const POLICY_RAW: u8 = 2;

const CARDINALITY_ONE: u8 = 0;
const CARDINALITY_MANY: u8 = 1;

const INDEX_EQUALITY: u8 = 0;
const INDEX_RANGE: u8 = 1;
const INDEX_COMPOSITE: u8 = 2;
const INDEX_UNIQUE: u8 = 3;

const KEY_U64: u8 = 0;
const KEY_I64: u8 = 1;
const KEY_STRING: u8 = 2;
const KEY_BYTES: u8 = 3;
const KEY_UUID: u8 = 4;

pub(super) fn encode_generation(generation: u64) -> Vec<u8> {
    let mut output = start();
    put_u64(&mut output, generation);
    output
}

pub(super) fn decode_generation(value: &[u8]) -> Result<u64> {
    let mut reader = Reader::start(value)?;
    let generation = reader.u64()?;
    reader.finish()?;

    Ok(generation)
}

pub(super) fn encode_keyspace(keyspace: &Keyspace) -> Vec<u8> {
    let mut output = start();
    put_u64(&mut output, u64::from(keyspace.id().get()));
    put_string(&mut output, keyspace.name());
    output
}

pub(super) fn decode_keyspace(value: &[u8]) -> Result<Keyspace> {
    let mut reader = Reader::start(value)?;
    let id = KeyspaceId::new(u32_id(reader.u64()?)?);
    let name = reader.string()?;
    reader.finish()?;

    Ok(Keyspace::new(id, name))
}

pub(super) fn encode_bucket(bucket: &Bucket) -> Vec<u8> {
    let mut output = start();
    put_u64(&mut output, bucket.id().get());
    put_string(&mut output, bucket.name());
    put_u32(&mut output, bucket.partition_count());
    output.push(match bucket.policy() {
        BucketPolicy::Strict => POLICY_STRICT,
        BucketPolicy::Managed => POLICY_MANAGED,
        BucketPolicy::Raw => POLICY_RAW,
    });
    put_u64(&mut output, bucket.schema_id().get());
    put_u64(&mut output, u64::from(bucket.schema_version().get()));
    output
}

pub(super) fn decode_bucket(value: &[u8]) -> Result<Bucket> {
    let mut reader = Reader::start(value)?;
    let id = BucketId::new(reader.u64()?);
    let name = reader.string()?;
    let partition_count = reader.u32()?;
    let policy = match reader.u8()? {
        POLICY_STRICT => BucketPolicy::Strict,
        POLICY_MANAGED => BucketPolicy::Managed,
        POLICY_RAW => BucketPolicy::Raw,
        tag => return Err(invalid(format!("unknown bucket policy tag {tag}"))),
    };
    let schema_id = SchemaId::new(reader.u64()?);
    let schema_version = SchemaVersion::new(u32_id(reader.u64()?)?);
    reader.finish()?;

    Ok(Bucket::new(
        id,
        name,
        partition_count,
        policy,
        schema_id,
        schema_version,
    ))
}

pub(super) fn encode_collection(collection: &Collection) -> Vec<u8> {
    let mut output = start();
    put_u64(&mut output, collection.id().get());
    put_u64(&mut output, u64::from(collection.keyspace_id().get()));
    put_string(&mut output, collection.name());
    put_string(&mut output, collection.affinity_group());
    put_key_schema(&mut output, collection.key_schema());
    put_uvarint(collection.fragments().len() as u64, &mut output);
    for fragment in collection.fragments() {
        put_fragment(&mut output, fragment);
    }
    output
}

pub(super) fn decode_collection(value: &[u8]) -> Result<Collection> {
    let mut reader = Reader::start(value)?;
    let id = CollectionId::new(reader.u64()?);
    let keyspace_id = KeyspaceId::new(u32_id(reader.u64()?)?);
    let name = reader.string()?;
    let affinity_group = reader.string()?;
    let key = read_key_schema(&mut reader)?;
    let fragment_count = reader.count()?;
    let mut fragments = Vec::with_capacity(fragment_count);
    for _ in 0..fragment_count {
        fragments.push(read_fragment(&mut reader)?);
    }
    reader.finish()?;

    Ok(Collection::new(
        id,
        keyspace_id,
        name,
        affinity_group,
        key,
        fragments,
    ))
}

pub(super) fn encode_index(index: &IndexDefinition) -> Vec<u8> {
    let mut output = start();
    put_u64(&mut output, index.id().get());
    put_string(&mut output, index.name());
    output.push(match index.kind() {
        IndexKind::Equality => INDEX_EQUALITY,
        IndexKind::Range => INDEX_RANGE,
        IndexKind::Composite => INDEX_COMPOSITE,
        IndexKind::Unique => INDEX_UNIQUE,
    });
    put_uvarint(index.fields().len() as u64, &mut output);
    for field in index.fields() {
        put_u64(&mut output, u64::from(field.get()));
    }
    output
}

pub(super) fn decode_index(value: &[u8]) -> Result<IndexDefinition> {
    let mut reader = Reader::start(value)?;
    let id = IndexId::new(reader.u64()?);
    let name = reader.string()?;
    let kind = match reader.u8()? {
        INDEX_EQUALITY => IndexKind::Equality,
        INDEX_RANGE => IndexKind::Range,
        INDEX_COMPOSITE => IndexKind::Composite,
        INDEX_UNIQUE => IndexKind::Unique,
        tag => return Err(invalid(format!("unknown index kind tag {tag}"))),
    };
    let field_count = reader.count()?;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        fields.push(FieldId::new(u32_id(reader.u64()?)?));
    }
    reader.finish()?;

    Ok(IndexDefinition::new(id, name, kind, fields))
}

pub(super) fn encode_placement(placement: &BucketPlacement) -> Vec<u8> {
    let mut output = start();
    put_u64(&mut output, placement.bucket_id().get());
    put_u64(&mut output, placement.backend_id().get());
    put_string(&mut output, placement.affinity_group());
    put_u32(&mut output, placement.partition_count());
    output
}

pub(super) fn decode_placement(value: &[u8]) -> Result<BucketPlacement> {
    let mut reader = Reader::start(value)?;
    let bucket_id = BucketId::new(reader.u64()?);
    let backend_id = BackendId::new(reader.u64()?);
    let affinity_group = reader.string()?;
    let partition_count = reader.u32()?;
    reader.finish()?;

    Ok(BucketPlacement::new(
        bucket_id,
        backend_id,
        affinity_group,
        partition_count,
    ))
}

pub(super) fn encode_schema(schema: &RecordSchema) -> Vec<u8> {
    let mut output = start();
    put_schema(&mut output, schema);
    output
}

pub(super) fn decode_schema(value: &[u8]) -> Result<RecordSchema> {
    let mut reader = Reader::start(value)?;
    let schema = read_schema(&mut reader)?;
    reader.finish()?;

    Ok(schema)
}

fn put_schema(output: &mut Vec<u8>, schema: &RecordSchema) {
    put_u64(output, schema.id().get());
    put_u64(output, u64::from(schema.version().get()));
    put_string(output, schema.name());
    put_uvarint(schema.fields().len() as u64, output);
    for field in schema.fields() {
        put_u64(output, u64::from(field.id().get()));
        put_string(output, field.name());
        put_type(output, field.ty());
        put_bool(output, field.optional());
    }
}

fn read_schema(reader: &mut Reader<'_>) -> Result<RecordSchema> {
    let id = SchemaId::new(reader.u64()?);
    let version = SchemaVersion::new(u32_id(reader.u64()?)?);
    let name = reader.string()?;
    let field_count = reader.count()?;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        fields.push(Field::new(
            FieldId::new(u32_id(reader.u64()?)?),
            reader.string()?,
            read_type(reader)?,
            reader.bool()?,
        ));
    }

    Ok(RecordSchema::new(id, version, name, fields))
}

fn put_type(output: &mut Vec<u8>, ty: &Type) {
    match ty {
        Type::Bool => output.push(TYPE_BOOL),
        Type::I32 => output.push(TYPE_I32),
        Type::I64 => output.push(TYPE_I64),
        Type::U32 => output.push(TYPE_U32),
        Type::U64 => output.push(TYPE_U64),
        Type::F32 => output.push(TYPE_F32),
        Type::F64 => output.push(TYPE_F64),
        Type::String => output.push(TYPE_STRING),
        Type::Bytes => output.push(TYPE_BYTES),
        Type::Timestamp => output.push(TYPE_TIMESTAMP),
        Type::Duration => output.push(TYPE_DURATION),
        Type::Enum(schema) => {
            output.push(TYPE_ENUM);
            put_string(output, schema.name());
            put_uvarint(schema.variants().len() as u64, output);
            for variant in schema.variants() {
                put_string(output, variant);
            }
        }
        Type::List(element) => {
            output.push(TYPE_LIST);
            put_type(output, element);
        }
        Type::Map(key, value) => {
            output.push(TYPE_MAP);
            put_type(output, key);
            put_type(output, value);
        }
        Type::Record(schema) => {
            output.push(TYPE_RECORD);
            put_schema(output, schema);
        }
    }
}

fn read_type(reader: &mut Reader<'_>) -> Result<Type> {
    let tag = reader.u8()?;
    let ty = match tag {
        TYPE_BOOL => Type::Bool,
        TYPE_I32 => Type::I32,
        TYPE_I64 => Type::I64,
        TYPE_U32 => Type::U32,
        TYPE_U64 => Type::U64,
        TYPE_F32 => Type::F32,
        TYPE_F64 => Type::F64,
        TYPE_STRING => Type::String,
        TYPE_BYTES => Type::Bytes,
        TYPE_TIMESTAMP => Type::Timestamp,
        TYPE_DURATION => Type::Duration,
        TYPE_ENUM => {
            let name = reader.string()?;
            let variant_count = reader.count()?;
            let mut variants = Vec::with_capacity(variant_count);
            for _ in 0..variant_count {
                variants.push(reader.string()?);
            }
            Type::Enum(EnumSchema::new(name, variants))
        }
        TYPE_LIST => Type::List(Box::new(read_type(reader)?)),
        TYPE_MAP => Type::Map(Box::new(read_type(reader)?), Box::new(read_type(reader)?)),
        TYPE_RECORD => Type::Record(Box::new(read_schema(reader)?)),
        other => return Err(invalid(format!("unknown type tag {other}"))),
    };

    Ok(ty)
}

fn put_key_schema(output: &mut Vec<u8>, schema: &DocumentKeySchema) {
    put_uvarint(schema.fields().len() as u64, output);
    for field in schema.fields() {
        put_string(output, field.name());
        output.push(match field.ty() {
            KeyType::U64 => KEY_U64,
            KeyType::I64 => KEY_I64,
            KeyType::String => KEY_STRING,
            KeyType::Bytes => KEY_BYTES,
            KeyType::Uuid => KEY_UUID,
        });
    }
}

fn read_key_schema(reader: &mut Reader<'_>) -> Result<DocumentKeySchema> {
    let count = reader.count()?;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let name = reader.string()?;
        let ty = match reader.u8()? {
            KEY_U64 => KeyType::U64,
            KEY_I64 => KeyType::I64,
            KEY_STRING => KeyType::String,
            KEY_BYTES => KeyType::Bytes,
            KEY_UUID => KeyType::Uuid,
            tag => return Err(invalid(format!("unknown key type tag {tag}"))),
        };
        fields.push(KeyField::new(name, ty));
    }

    Ok(DocumentKeySchema::new(fields))
}

fn put_fragment(output: &mut Vec<u8>, fragment: &Fragment) {
    put_u64(output, u64::from(fragment.id().get()));
    put_string(output, fragment.name());
    put_u64(output, fragment.bucket_id().get());
    output.push(match fragment.cardinality() {
        FragmentCardinality::One => CARDINALITY_ONE,
        FragmentCardinality::Many => CARDINALITY_MANY,
    });
}

fn read_fragment(reader: &mut Reader<'_>) -> Result<Fragment> {
    let id = FragmentId::new(u32_id(reader.u64()?)?);
    let name = reader.string()?;
    let bucket_id = BucketId::new(reader.u64()?);
    let cardinality = match reader.u8()? {
        CARDINALITY_ONE => FragmentCardinality::One,
        CARDINALITY_MANY => FragmentCardinality::Many,
        tag => return Err(invalid(format!("unknown fragment cardinality tag {tag}"))),
    };

    Ok(Fragment::new(id, name, bucket_id, cardinality))
}

fn start() -> Vec<u8> {
    vec![METADATA_FORMAT_VERSION]
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_bool(output: &mut Vec<u8>, value: bool) {
    output.push(u8::from(value));
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) {
    put_uvarint(value.len() as u64, output);
    output.extend_from_slice(value);
}

fn put_string(output: &mut Vec<u8>, value: &str) {
    put_bytes(output, value.as_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn start(bytes: &'a [u8]) -> Result<Self> {
        let mut reader = Self { bytes, offset: 0 };
        let version = reader.u8()?;
        if version != METADATA_FORMAT_VERSION {
            return Err(invalid(format!("unsupported metadata version {version}")));
        }

        Ok(reader)
    }

    fn u8(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| invalid("metadata value is truncated"))?;
        self.offset += 1;

        Ok(value)
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.fixed(4)?.try_into().expect("four bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.fixed(8)?.try_into().expect("eight bytes"),
        ))
    }

    fn bool(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(invalid(format!("invalid boolean byte {value}"))),
        }
    }

    fn count(&mut self) -> Result<usize> {
        let (value, consumed) =
            get_uvarint(&self.bytes[self.offset..]).map_err(|error| invalid(error.to_string()))?;
        self.offset += consumed;

        usize::try_from(value).map_err(|_| invalid("count overflows usize"))
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let (length, consumed) =
            get_uvarint(&self.bytes[self.offset..]).map_err(|error| invalid(error.to_string()))?;
        self.offset += consumed;
        let length = usize::try_from(length).map_err(|_| invalid("byte length overflows usize"))?;
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| invalid("byte length overflows"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("metadata value is truncated"))?;
        self.offset = end;

        Ok(value)
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|_| invalid("metadata string is not UTF-8"))
    }

    fn fixed(&mut self, width: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(width)
            .ok_or_else(|| invalid("metadata offset overflows"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| invalid("metadata value is truncated"))?;
        self.offset = end;

        Ok(value)
    }

    fn finish(self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(invalid("metadata value has trailing bytes"));
        }

        Ok(())
    }
}

fn u32_id(value: u64) -> Result<u32> {
    u32::try_from(value).map_err(|_| invalid("identifier exceeds u32"))
}

fn invalid(message: impl Into<String>) -> CatalogError {
    CatalogError::InvalidMetadata(message.into())
}
