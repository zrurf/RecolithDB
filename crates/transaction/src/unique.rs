use std::fmt;

use crate::error::{Result, TransactionError};

/// Global unique reservation state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservationState {
    /// Claimed with a TTL, not yet confirmed.
    Reserved,
    /// Confirmed by the owner document write.
    Committed,
    /// Being released after a rename or deletion.
    Releasing,
}

impl fmt::Display for ReservationState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reserved => formatter.write_str("Reserved"),
            Self::Committed => formatter.write_str("Committed"),
            Self::Releasing => formatter.write_str("Releasing"),
        }
    }
}

/// Reservation record for a globally unique value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UniqueReservation {
    normalized_value: Vec<u8>,
    owner: Vec<u8>,
    state: ReservationState,
    expires_at: u64,
}

impl UniqueReservation {
    /// Creates a reserved claim.
    #[must_use]
    pub fn new(
        normalized_value: impl Into<Vec<u8>>,
        owner: impl Into<Vec<u8>>,
        expires_at: u64,
    ) -> Self {
        Self {
            normalized_value: normalized_value.into(),
            owner: owner.into(),
            state: ReservationState::Reserved,
            expires_at,
        }
    }

    /// Returns the normalized unique value.
    #[must_use]
    pub fn normalized_value(&self) -> &[u8] {
        &self.normalized_value
    }

    /// Returns the owning document key.
    #[must_use]
    pub fn owner(&self) -> &[u8] {
        &self.owner
    }

    /// Returns the reservation state.
    #[must_use]
    pub fn state(&self) -> ReservationState {
        self.state
    }

    /// Returns the reservation expiry.
    #[must_use]
    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }

    /// Confirms the reservation after the owner document write.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::InvalidReservationTransition`] outside `Reserved`.
    pub fn confirm(&mut self) -> Result<()> {
        if self.state != ReservationState::Reserved {
            return Err(TransactionError::InvalidReservationTransition {
                from: self.state,
                to: ReservationState::Committed,
            });
        }

        self.state = ReservationState::Committed;
        Ok(())
    }

    /// Marks the reservation for release.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::InvalidReservationTransition`] when already releasing.
    pub fn release(&mut self) -> Result<()> {
        if self.state == ReservationState::Releasing {
            return Err(TransactionError::InvalidReservationTransition {
                from: self.state,
                to: ReservationState::Releasing,
            });
        }

        self.state = ReservationState::Releasing;
        Ok(())
    }
}
