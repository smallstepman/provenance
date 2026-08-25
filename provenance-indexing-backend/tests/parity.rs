#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "firebird",
    feature = "lbug",
))]
mod feature_tests {
    use std::path::Path;

    use provenance_core::QueryEngine;
    use provenance_indexing_backend::{state_from_operations, test_support};
    use tempfile::TempDir;

    type BackendIndex = provenance_indexing_backend::BackendIndex<test_support::TestModel>;

    fn firebird_tests_enabled() -> bool {
        !cfg!(feature = "firebird") || std::env::var_os("PROVENANCE_FIREBIRD_CLIENT").is_some()
    }

    fn in_memory() -> Option<BackendIndex> {
        if !firebird_tests_enabled() {
            eprintln!("skipping Firebird parity test; set PROVENANCE_FIREBIRD_CLIENT to run it");
            return None;
        }
        Some(BackendIndex::in_memory().expect("open in-memory projection index"))
    }

    fn open(path: impl AsRef<Path>) -> Option<BackendIndex> {
        if !firebird_tests_enabled() {
            eprintln!(
                "skipping Firebird persistence test; set PROVENANCE_FIREBIRD_CLIENT to run it"
            );
            return None;
        }
        Some(BackendIndex::open(path).expect("open file projection index"))
    }

    #[test]
    fn backend_matches_core_query_semantics() {
        let operations = vec![test_support::operation_with_nodes(8)];
        let state = state_from_operations(&operations).expect("build reference state");
        let queries = vec![
            test_support::explain_query(),
            test_support::history_query(),
            test_support::traverse_query(),
        ];

        let Some(mut index) = in_memory() else {
            return;
        };
        index
            .rebuild(&operations)
            .expect("rebuild projection index");

        for query in queries {
            let expected = state.query(&query).expect("reference query");
            assert_eq!(index.execute(&query).expect("backend query"), expected);
        }
    }

    #[test]
    fn backend_persists_and_reloads_a_projection() {
        let directory = TempDir::new().expect("create temporary directory");
        let filename = if cfg!(feature = "sqlite") {
            "projection.sqlite"
        } else if cfg!(feature = "duckdb") {
            "projection.duckdb"
        } else if cfg!(feature = "doltlite") {
            "projection.dolt"
        } else if cfg!(feature = "turso") {
            "projection.turso"
        } else if cfg!(feature = "firebird") {
            "projection.fdb"
        } else {
            "projection.lbug"
        };
        let path = directory.path().join(filename);
        let operations = vec![test_support::operation_with_nodes(4)];

        let Some(mut index) = open(&path) else {
            return;
        };
        index.rebuild(&operations).expect("rebuild projection file");
        assert_eq!(index.operation_count().expect("operation count"), 1);
        drop(index);

        let Some(index) = open(&path) else {
            return;
        };
        let result = index
            .execute(&test_support::explain_query())
            .expect("query reloaded projection file");
        assert_eq!(result.entities.len(), 4);
    }
}
