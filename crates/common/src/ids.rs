use std::fmt;

macro_rules! define_id {
    ($name:ident, $inner:ty) => {
        /// Stable identifier newtype.
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name($inner);

        impl $name {
            /// Creates an identifier from its raw value.
            #[must_use]
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            /// Returns the raw identifier value.
            #[must_use]
            pub const fn get(self) -> $inner {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }
    };
}

define_id!(KeyspaceId, u32);
define_id!(CollectionId, u64);
define_id!(BucketId, u64);
define_id!(PartitionId, u32);
define_id!(FragmentId, u32);
define_id!(FieldId, u32);
define_id!(PlacementGroupId, u64);
define_id!(IndexId, u64);
define_id!(SchemaId, u64);
define_id!(SchemaVersion, u32);
define_id!(CommitTs, u64);
define_id!(TxnId, u64);
define_id!(BackendId, u64);
define_id!(CatalogGeneration, u64);
