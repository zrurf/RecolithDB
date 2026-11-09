use recolith_backend::BackendError;

pub(crate) fn map_error(error: fjall::Error) -> BackendError {
    match &error {
        fjall::Error::Io(inner) => BackendError::Io(inner.to_string()),
        fjall::Error::JournalRecovery(_)
        | fjall::Error::InvalidVersion(_)
        | fjall::Error::Decompress(_)
        | fjall::Error::InvalidTrailer
        | fjall::Error::InvalidTag(_) => BackendError::Corruption(error.to_string()),
        fjall::Error::Poisoned | fjall::Error::Unrecoverable => BackendError::Closed,
        fjall::Error::Locked => BackendError::Other(error.to_string()),
        _ => BackendError::Other(error.to_string()),
    }
}
