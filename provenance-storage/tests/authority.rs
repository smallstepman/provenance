use std::collections::BTreeSet;

use provenance_core::{
    Attributes, FieldName, Model, Object, ObjectId, Operation, OperationId, ProvenanceStore, Value,
};
use provenance_storage::{DirectoryProvenanceStore, MemoryProvenanceStore};

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct TestModel;

impl Model for TestModel {
    type Id = String;
    type Seed = String;
    type ExternalId = String;
    type Payload = Vec<u8>;
}

fn empty_operation(id: &str) -> Operation<TestModel> {
    Operation {
        id: OperationId::<TestModel>::new(id.to_owned()),
        parents: BTreeSet::new(),
        source: None,
        facts: Vec::new(),
        attributes: Attributes::new(),
    }
}

#[test]
fn immutable_objects_round_trip_through_memory_authority() {
    let mut store = MemoryProvenanceStore::<TestModel>::new();
    let object = Object {
        id: ObjectId::<TestModel>::new("object-1".to_owned()),
        payload: vec![1, 2, 3],
        attributes: Attributes::new(),
    };
    store.put_object(&object).expect("store object");
    assert_eq!(
        store.get_object(&object.id).expect("load object"),
        Some(object)
    );
}

#[test]
fn immutable_operation_collision_is_rejected() {
    let mut store = MemoryProvenanceStore::<TestModel>::new();
    let operation = empty_operation("operation-1");
    store.put_operation(&operation).expect("store operation");
    let mut conflicting = operation.clone();
    conflicting.attributes.insert(
        FieldName::from("different"),
        Value::<TestModel>::String("payload".into()),
    );
    let error = store
        .put_operation(&conflicting)
        .expect_err("collision must be rejected");
    assert!(error.to_string().contains("different immutable contents"));
}

#[test]
fn filesystem_operations_and_heads_round_trip() {
    let directory = tempfile::tempdir().expect("temporary store");
    let mut store = DirectoryProvenanceStore::<TestModel>::open(directory.path())
        .expect("open filesystem store");
    let operation = empty_operation("operation-1");
    store.put_operation(&operation).expect("store operation");
    assert_eq!(
        store
            .publish_heads(&BTreeSet::new(), &BTreeSet::from([operation.id.clone()]))
            .expect("publish head"),
        provenance_core::PublishOutcome::Published
    );

    let reopened = DirectoryProvenanceStore::<TestModel>::open(directory.path())
        .expect("reopen filesystem store");
    assert_eq!(reopened.root(), directory.path());
    assert_eq!(
        reopened.reachable_operations().expect("load operations"),
        vec![operation]
    );
}
