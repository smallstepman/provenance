//! Concrete projection storage implementations.
//!
//! The backend crate intentionally compiles one database implementation at a
//! time. Enable exactly one database feature.

#[cfg(feature = "duckdb")]
pub mod duckdb;

#[cfg(any(feature = "sqlite", feature = "doltlite"))]
pub mod sqlite;

#[cfg(feature = "doltlite")]
pub mod doltlite {
    pub use super::sqlite::{
        SqliteIndex as DoltliteIndex, SqliteIndexError as DoltliteIndexError,
        SqliteStorage as DoltliteStorage, SqliteStorageError as DoltliteStorageError,
    };
}

#[cfg(feature = "turso")]
pub mod turso;

#[cfg(feature = "firebird")]
pub mod firebird;

#[cfg(feature = "lbug")]
pub mod lbug;
