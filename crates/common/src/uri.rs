use std::fmt;

use crate::error::{Error, Result};

/// URI scheme used by logical and physical Recolith resources.
pub const SCHEME: &str = "recolith";

/// A validated Recolith URI.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Uri(String);

impl Uri {
    /// Parses a Recolith URI.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] when the scheme is missing or the URI is empty.
    pub fn parse(input: &str) -> Result<Self> {
        if input.is_empty() {
            return Err(Error::InvalidInput("URI is empty".to_owned()));
        }
        if !input.starts_with("recolith://") {
            return Err(Error::InvalidInput(
                "URI must use the recolith scheme".to_owned(),
            ));
        }

        Ok(Self(input.to_owned()))
    }

    /// Returns the URI as a string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Uri {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
