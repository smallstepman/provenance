#[cfg(feature = "sqlite-index")]
mod enabled {
    use std::path::Path;

    use provenance_core::Operation;
    use rusqlite::{Connection, TransactionBehavior, params};
    use thiserror::Error;

    use crate::{DirectoryProvenanceStore, JjModel};

    const SCHEMA: &str = r#"
    PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS operation_index (
        operation_id TEXT PRIMARY KEY NOT NULL,
        parent_count INTEGER NOT NULL
    );
    "#;

    #[derive(Debug, Error)]
    pub enum SqliteIndexError {
        #[error("SQLite projection index error: {0}")]
        Sql(#[from] rusqlite::Error),
        #[error("could not create SQLite projection index directory: {0}")]
        Io(#[from] std::io::Error),
        #[error("could not read authoritative provenance for index rebuild: {0}")]
        Authority(String),
    }

    /// Disposable query accelerator rebuilt from authoritative provenance.
    ///
    /// This type deliberately stores no operation payload and is never
    /// consulted for publication, idempotency, or state reconstruction.
    #[derive(Debug)]
    pub struct SqliteIndex {
        connection: Connection,
    }

    impl SqliteIndex {
        pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteIndexError> {
            let path = path.as_ref();
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)?;
            }
            Self::from_connection(Connection::open(path)?)
        }

        pub fn in_memory() -> Result<Self, SqliteIndexError> {
            Self::from_connection(Connection::open_in_memory()?)
        }

        fn from_connection(connection: Connection) -> Result<Self, SqliteIndexError> {
            connection.busy_timeout(std::time::Duration::from_secs(5))?;
            connection.execute_batch(SCHEMA)?;
            Ok(Self { connection })
        }

        pub fn clear(&mut self) -> Result<(), SqliteIndexError> {
            self.connection.execute("DELETE FROM operation_index", [])?;
            Ok(())
        }

        pub fn rebuild(
            &mut self,
            operations: &[Operation<JjModel>],
        ) -> Result<(), SqliteIndexError> {
            let transaction = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute("DELETE FROM operation_index", [])?;
            for operation in operations {
                transaction.execute(
                    "INSERT INTO operation_index(operation_id, parent_count) VALUES (?1, ?2)",
                    params![operation.id.raw(), operation.parents.len() as i64],
                )?;
            }
            transaction.commit()?;
            Ok(())
        }

        pub fn rebuild_from(
            &mut self,
            store: &DirectoryProvenanceStore,
        ) -> Result<(), SqliteIndexError> {
            let operations = store
                .reachable_operations()
                .map_err(|error| SqliteIndexError::Authority(error.to_string()))?;
            self.rebuild(&operations)
        }

        pub fn operation_count(&self) -> Result<usize, SqliteIndexError> {
            let count: i64 =
                self.connection
                    .query_row("SELECT COUNT(*) FROM operation_index", [], |row| row.get(0))?;
            Ok(count as usize)
        }

        pub fn contains(&self, operation_id: &str) -> Result<bool, SqliteIndexError> {
            let count: i64 = self.connection.query_row(
                "SELECT COUNT(*) FROM operation_index WHERE operation_id = ?1",
                [operation_id],
                |row| row.get(0),
            )?;
            Ok(count != 0)
        }
    }
}

#[cfg(feature = "sqlite-index")]
pub use enabled::{SqliteIndex, SqliteIndexError};

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
