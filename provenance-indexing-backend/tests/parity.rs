#[cfg(all(feature = "sqlite", feature = "duckdb"))]
compile_error!("features `sqlite` and `duckdb` are mutually exclusive");

#[cfg(any(feature = "sqlite", feature = "duckdb"))]
mod feature_tests {
    use std::path::Path;

    use provenance_core::QueryEngine;
    use provenance_indexing_backend::{state_from_operations, test_support};
    use tempfile::TempDir;

    #[cfg(feature = "sqlite")]
    type BackendIndex = provenance_indexing_backend::SqliteIndex<test_support::TestModel>;

    #[cfg(feature = "duckdb")]
    type BackendIndex = provenance_indexing_backend::DuckDbIndex<test_support::TestModel>;

    fn in_memory() -> BackendIndex {
        BackendIndex::in_memory().expect("open in-memory projection index")
    }

    fn open(path: impl AsRef<Path>) -> BackendIndex {
        BackendIndex::open(path).expect("open file projection index")
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

        let mut index = in_memory();
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
        } else {
            "projection.duckdb"
        };
        let path = directory.path().join(filename);
        let operations = vec![test_support::operation_with_nodes(4)];

        let mut index = open(&path);
        index.rebuild(&operations).expect("rebuild projection file");
        assert_eq!(index.operation_count().expect("operation count"), 1);
        drop(index);

        let index = open(&path);
        let result = index
            .execute(&test_support::explain_query())
            .expect("query reloaded projection file");
        assert_eq!(result.entities.len(), 4);
    }
}
