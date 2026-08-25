//! Concrete projection storage implementations.
//!
//! The backend crate intentionally compiles one database implementation at a
//! time. Enable exactly one database feature.

#[cfg(any(
    all(feature = "sqlite", feature = "duckdb"),
    all(feature = "sqlite", feature = "doltlite"),
    all(feature = "sqlite", feature = "turso"),
    all(feature = "sqlite", feature = "firebird"),
    all(feature = "sqlite", feature = "lbug"),
    all(feature = "duckdb", feature = "doltlite"),
    all(feature = "duckdb", feature = "turso"),
    all(feature = "duckdb", feature = "firebird"),
    all(feature = "duckdb", feature = "lbug"),
    all(feature = "doltlite", feature = "turso"),
    all(feature = "doltlite", feature = "firebird"),
    all(feature = "doltlite", feature = "lbug"),
    all(feature = "turso", feature = "firebird"),
    all(feature = "turso", feature = "lbug"),
    all(feature = "firebird", feature = "lbug"),
))]
compile_error!("database backend features are mutually exclusive");

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

#[cfg(feature = "sqlite")]
mod selected {
    pub use super::sqlite::{
        SqliteIndex as BackendIndex, SqliteIndexError as BackendIndexError,
        SqliteStorage as BackendStorage, SqliteStorageError as BackendStorageError,
    };
}

#[cfg(all(not(feature = "sqlite"), feature = "duckdb"))]
mod selected {
    pub use super::duckdb::{
        DuckDbIndex as BackendIndex, DuckDbIndexError as BackendIndexError,
        DuckDbStorage as BackendStorage, DuckDbStorageError as BackendStorageError,
    };
}

#[cfg(all(not(feature = "sqlite"), not(feature = "duckdb"), feature = "doltlite"))]
mod selected {
    pub use super::doltlite::{
        DoltliteIndex as BackendIndex, DoltliteIndexError as BackendIndexError,
        DoltliteStorage as BackendStorage, DoltliteStorageError as BackendStorageError,
    };
}

#[cfg(all(
    not(feature = "sqlite"),
    not(feature = "duckdb"),
    not(feature = "doltlite"),
    feature = "turso"
))]
mod selected {
    pub use super::turso::{
        TursoIndex as BackendIndex, TursoIndexError as BackendIndexError,
        TursoStorage as BackendStorage, TursoStorageError as BackendStorageError,
    };
}

#[cfg(all(
    not(feature = "sqlite"),
    not(feature = "duckdb"),
    not(feature = "doltlite"),
    not(feature = "turso"),
    feature = "firebird"
))]
mod selected {
    pub use super::firebird::{
        FirebirdIndex as BackendIndex, FirebirdIndexError as BackendIndexError,
        FirebirdStorage as BackendStorage, FirebirdStorageError as BackendStorageError,
    };
}

#[cfg(all(
    not(feature = "sqlite"),
    not(feature = "duckdb"),
    not(feature = "doltlite"),
    not(feature = "turso"),
    not(feature = "firebird"),
    feature = "lbug"
))]
mod selected {
    pub use super::lbug::{
        LbugIndex as BackendIndex, LbugIndexError as BackendIndexError,
        LbugStorage as BackendStorage, LbugStorageError as BackendStorageError,
    };
}

#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "firebird",
    feature = "lbug",
))]
pub use selected::{BackendIndex, BackendIndexError, BackendStorage, BackendStorageError};
