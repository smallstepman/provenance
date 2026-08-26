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
mod feature_tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;

    use provenance_core::{
        Object, ObjectId, Operation, OperationId, ProvenanceStore, PublishOutcome, QueryEngine,
    };
    use provenance_indexing_backend::{state_from_operations, test_support};
    use tempfile::TempDir;

    #[derive(Default)]
    struct MemoryStore {
        operations:
            BTreeMap<OperationId<test_support::TestModel>, Operation<test_support::TestModel>>,
        heads: BTreeSet<OperationId<test_support::TestModel>>,
    }

    impl MemoryStore {
        fn set_head(&mut self, operation: Operation<test_support::TestModel>) {
            self.operations
                .insert(operation.id.clone(), operation.clone());
            self.heads = [operation.id].into_iter().collect();
        }
    }

    impl ProvenanceStore<test_support::TestModel> for MemoryStore {
        type Error = String;

        fn get_operation(
            &self,
            id: &OperationId<test_support::TestModel>,
        ) -> Result<Option<Operation<test_support::TestModel>>, Self::Error> {
            Ok(self.operations.get(id).cloned())
        }

        fn has_operation(
            &self,
            id: &OperationId<test_support::TestModel>,
        ) -> Result<bool, Self::Error> {
            Ok(self.operations.contains_key(id))
        }

        fn put_operation(
            &mut self,
            operation: &Operation<test_support::TestModel>,
        ) -> Result<(), Self::Error> {
            self.operations
                .insert(operation.id.clone(), operation.clone());
            Ok(())
        }

        fn get_object(
            &self,
            _id: &ObjectId<test_support::TestModel>,
        ) -> Result<Option<Object<test_support::TestModel>>, Self::Error> {
            Ok(None)
        }

        fn put_object(
            &mut self,
            _object: &Object<test_support::TestModel>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn heads(&self) -> Result<BTreeSet<OperationId<test_support::TestModel>>, Self::Error> {
            Ok(self.heads.clone())
        }

        fn publish_heads(
            &mut self,
            _expected: &BTreeSet<OperationId<test_support::TestModel>>,
            next: &BTreeSet<OperationId<test_support::TestModel>>,
        ) -> Result<PublishOutcome, Self::Error> {
            self.heads = next.clone();
            Ok(PublishOutcome::Published)
        }
    }
    type BackendIndex = provenance_indexing_backend::BackendIndex<test_support::TestModel>;

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
    fn incremental_update_matches_full_rebuild() {
        let base = test_support::operation_with_nodes(4);
        let mut next = test_support::operation_with_nodes(5);
        next.parents.insert(base.id.clone());
        let mut store = MemoryStore::default();
        store.set_head(base.clone());

        let mut incremental = in_memory();
        incremental
            .rebuild_from(&store)
            .expect("build incremental baseline");

        store.set_head(next.clone());
        incremental
            .update_from(&store)
            .expect("apply incremental operation");

        let mut full = in_memory();
        full.rebuild(&[base, next]).expect("build full reference");
        assert_eq!(
            incremental
                .execute(&test_support::explain_query())
                .expect("query incrementally updated index"),
            full.execute(&test_support::explain_query())
                .expect("query fully rebuilt index")
        );
        assert_eq!(incremental.operation_count().expect("incremental count"), 2);
    }

    #[test]
    fn incremental_duplicate_events_match_full_rebuild() {
        let base = test_support::operation_with_nodes(3);
        let mut duplicate = test_support::operation_with_nodes(3);
        duplicate.id =
            OperationId::<test_support::TestModel>::new("operation-duplicate".to_owned());
        duplicate.parents.insert(base.id.clone());

        let mut store = MemoryStore::default();
        store.set_head(base.clone());
        let mut incremental = in_memory();
        incremental
            .rebuild_from(&store)
            .expect("build duplicate-event baseline");

        store.set_head(duplicate.clone());
        incremental
            .update_from(&store)
            .expect("apply duplicate-event operation");

        let mut full = in_memory();
        full.rebuild(&[base, duplicate])
            .expect("build duplicate-event reference");
        for query in [
            test_support::explain_query(),
            test_support::history_query(),
            test_support::traverse_query(),
        ] {
            assert_eq!(
                incremental.execute(&query).expect("incremental query"),
                full.execute(&query).expect("full rebuild query")
            );
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
        } else if cfg!(feature = "redb") {
            "projection.redb"
        } else if cfg!(feature = "heed") {
            "projection.lmdb"
        } else if cfg!(feature = "mnestic") {
            "projection.mnestic"
        } else {
            "projection.lbug"
        };
        let path = directory.path().join(filename);
        let operations = vec![test_support::operation_with_nodes(4)];

        let mut index = open(&path);
        index.rebuild(&operations).expect("rebuild projection file");
        assert_eq!(index.operation_count().expect("operation count"), 1);
        drop(index);

        let mut index = open(&path);
        let mut store = MemoryStore::default();
        store.set_head(operations[0].clone());
        index
            .update_from(&store)
            .expect("hydrate and update reloaded projection file");
        assert_eq!(
            index.operation_count().expect("reloaded operation count"),
            1
        );
        let result = index
            .execute(&test_support::explain_query())
            .expect("query reloaded projection file");
        assert_eq!(result.entities.len(), 4);
    }
}
