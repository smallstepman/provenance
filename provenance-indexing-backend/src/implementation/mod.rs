//! Concrete projection storage implementations.
//!
//! The backend crate intentionally compiles one database implementation at a
//! time. Enable exactly one database feature.

#[cfg(any(
    all(feature = "sqlite", feature = "duckdb"),
    all(feature = "sqlite", feature = "doltlite"),
    all(feature = "sqlite", feature = "turso"),
    all(feature = "sqlite", feature = "lbug"),
    all(feature = "sqlite", feature = "redb"),
    all(feature = "sqlite", feature = "heed"),
    all(feature = "sqlite", feature = "mnestic"),
    all(feature = "duckdb", feature = "doltlite"),
    all(feature = "duckdb", feature = "turso"),
    all(feature = "duckdb", feature = "lbug"),
    all(feature = "duckdb", feature = "redb"),
    all(feature = "duckdb", feature = "heed"),
    all(feature = "duckdb", feature = "mnestic"),
    all(feature = "doltlite", feature = "turso"),
    all(feature = "doltlite", feature = "lbug"),
    all(feature = "doltlite", feature = "redb"),
    all(feature = "doltlite", feature = "heed"),
    all(feature = "doltlite", feature = "mnestic"),
    all(feature = "turso", feature = "lbug"),
    all(feature = "turso", feature = "redb"),
    all(feature = "turso", feature = "heed"),
    all(feature = "turso", feature = "mnestic"),
    all(feature = "lbug", feature = "redb"),
    all(feature = "lbug", feature = "heed"),
    all(feature = "lbug", feature = "mnestic"),
    all(feature = "redb", feature = "heed"),
    all(feature = "redb", feature = "mnestic"),
    all(feature = "heed", feature = "mnestic"),
))]
compile_error!("database backend features are mutually exclusive");
#[cfg(any(feature = "redb", feature = "heed", feature = "mnestic"))]
mod kv;

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

#[cfg(feature = "lbug")]
pub mod lbug;

#[cfg(feature = "redb")]
pub mod redb;

#[cfg(feature = "heed")]
pub mod heed;

#[cfg(feature = "mnestic")]
pub mod mnestic;

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
    feature = "lbug"
))]
mod selected {
    pub use super::lbug::{
        LbugIndex as BackendIndex, LbugIndexError as BackendIndexError,
        LbugStorage as BackendStorage, LbugStorageError as BackendStorageError,
    };
}

#[cfg(all(
    not(feature = "sqlite"),
    not(feature = "duckdb"),
    not(feature = "doltlite"),
    not(feature = "turso"),
    not(feature = "lbug"),
    feature = "redb"
))]
mod selected {
    pub use super::redb::{
        RedbIndex as BackendIndex, RedbIndexError as BackendIndexError,
        RedbStorage as BackendStorage, RedbStorageError as BackendStorageError,
    };
}

#[cfg(all(
    not(feature = "sqlite"),
    not(feature = "duckdb"),
    not(feature = "doltlite"),
    not(feature = "turso"),
    not(feature = "lbug"),
    not(feature = "redb"),
    feature = "heed"
))]
mod selected {
    pub use super::heed::{
        HeedIndex as BackendIndex, HeedIndexError as BackendIndexError,
        HeedStorage as BackendStorage, HeedStorageError as BackendStorageError,
    };
}

#[cfg(all(
    not(feature = "sqlite"),
    not(feature = "duckdb"),
    not(feature = "doltlite"),
    not(feature = "turso"),
    not(feature = "lbug"),
    not(feature = "redb"),
    not(feature = "heed"),
    feature = "mnestic"
))]
mod selected {
    pub use super::mnestic::{
        MnesticIndex as BackendIndex, MnesticIndexError as BackendIndexError,
        MnesticStorage as BackendStorage, MnesticStorageError as BackendStorageError,
    };
}

#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "lbug",
    feature = "redb",
    feature = "heed",
    feature = "mnestic",
))]
pub use selected::{BackendIndex, BackendIndexError, BackendStorage, BackendStorageError};
