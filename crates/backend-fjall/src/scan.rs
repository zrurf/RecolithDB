use std::ops::Bound;

use recolith_backend::{KeyRange, KeyValue, Result, ScanOptions};

use crate::error::map_error;

pub(crate) fn bounds(range: &KeyRange) -> (Bound<&[u8]>, Bound<&[u8]>) {
    (map_bound(range.start_bound()), map_bound(range.end_bound()))
}

fn map_bound(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(value) => Bound::Included(value.as_slice()),
        Bound::Excluded(value) => Bound::Excluded(value.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn collect(iter: fjall::Iter, options: ScanOptions) -> Result<Vec<KeyValue>> {
    let limit = options.limit.unwrap_or(usize::MAX);
    let mut entries = Vec::new();

    if options.reverse {
        for guard in iter.rev().take(limit) {
            push(&mut entries, guard)?;
        }
    } else {
        for guard in iter.take(limit) {
            push(&mut entries, guard)?;
        }
    }

    Ok(entries)
}

fn push(entries: &mut Vec<KeyValue>, guard: fjall::Guard) -> Result<()> {
    let (key, value) = guard.into_inner().map_err(map_error)?;
    entries.push((key.to_vec(), value.to_vec()));

    Ok(())
}
