//! Concrete projection storage implementations.
//!
//! The backend crate intentionally compiles one database implementation at a
//! time. Enable exactly one of the `sqlite` or `duckdb` features.

#[cfg(feature = "duckdb")]
pub mod duckdb;

#[cfg(feature = "sqlite")]
pub mod sqlite;
