#[cfg(feature = "sqlite-index")]
pub use provenance_indexing_backend::{
    BackendIndex as SqliteIndex, BackendIndexError as SqliteIndexError,
};

#[cfg(not(feature = "sqlite-index"))]
mod disabled {
    use std::path::Path;

    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum SqliteIndexError {
        #[error("SQLite projection index support is disabled; enable the sqlite-index feature")]
        Disabled,
    }

    /// Marker API for builds that intentionally exclude SQLite.
    #[derive(Debug)]
    pub struct SqliteIndex;

    impl SqliteIndex {
        pub fn open(_path: impl AsRef<Path>) -> Result<Self, SqliteIndexError> {
            Err(SqliteIndexError::Disabled)
        }

        pub fn in_memory() -> Result<Self, SqliteIndexError> {
            Err(SqliteIndexError::Disabled)
        }
    }
}

#[cfg(not(feature = "sqlite-index"))]
pub use disabled::{SqliteIndex, SqliteIndexError};
