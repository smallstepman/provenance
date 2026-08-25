use std::collections::BTreeSet;

use provenance_core::{Attributes, Object, ObjectId, Operation, OperationId, ProvenanceStore};
use provenance_jj::{DirectoryProvenanceStore, JjModel};

fn empty_operation(id: &str) -> Operation<JjModel> {
    Operation {
        id: OperationId::<JjModel>::new(id.to_owned()),
        parents: BTreeSet::new(),
        source: None,
        facts: Vec::new(),
        attributes: Attributes::new(),
    }
}

#[test]
fn immutable_objects_round_trip_through_memory_authority() {
    let mut store = DirectoryProvenanceStore::in_memory();
    let object = Object {
        id: ObjectId::<JjModel>::new("object-1".to_owned()),
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
    let mut store = DirectoryProvenanceStore::in_memory();
    let operation = empty_operation("operation-1");
    store.put_operation(&operation).expect("store operation");
    let mut conflicting = operation.clone();
    conflicting.attributes.insert(
        provenance_core::FieldName::from("different"),
        provenance_jj::string_value("payload"),
    );
    let error = store
        .put_operation(&conflicting)
        .expect_err("collision must be rejected");
    assert!(error.to_string().contains("different immutable contents"));
}
