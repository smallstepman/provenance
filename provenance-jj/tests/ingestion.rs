mod support;

use provenance_core::EntityRef;
use provenance_jj::{
    ingest_repository, jj_commit_address, jj_operation, observe_repository, open_service,
    why_jj_commit,
};

#[test]
fn single_commit_creates_operation_commit_and_workspace_entities() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "first\n");
    fixture.commit("first");

    let repository = fixture.repository();
    let operation_id = repository.operation_id();
    let commit_id = fixture.head_commit();
    let mut service = fixture.service();

    fixture.ingest(&mut service);

    let operation_address = jj_operation(operation_id.clone());
    let commit_address = jj_commit_address(commit_id.clone());
    assert!(
        service
            .state
            .operation_for_source(&operation_address)
            .is_some()
    );
    assert!(service.state.projection.entity(&commit_address).is_some());
    assert!(
        service
            .state
            .projection
            .entity(&operation_address)
            .is_some()
    );
    assert!(
        service
            .state
            .projection
            .outgoing
            .get(&EntityRef::External(operation_address.clone()))
            .is_some_and(|edges| {
                edges.iter().any(|edge| {
                    edge.relation.relation_type.name.as_str() == "produced"
                        && edge.relation.to == EntityRef::External(commit_address.clone())
                })
            })
    );
    assert!(
        service
            .state
            .projection
            .entities
            .iter()
            .any(|(address, _)| address.kind.as_str() == "workspace")
    );
    let explanation = service
        .state
        .query(&why_jj_commit(&commit_id))
        .expect("explain first commit");
    assert!(
        explanation
            .entities
            .contains(&EntityRef::External(operation_address))
    );
}

#[test]
fn multiple_jj_operations_follow_in_provenance() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "first\n");
    fixture.commit("first");
    let first = fixture.repository();
    let first_operation = first.operation_id();
    let mut service = fixture.service();
    fixture.ingest(&mut service);

    fixture.write_file("README", "second\n");
    fixture.commit("second");
    let second = fixture.repository();
    let second_operation = second.operation_id();
    fixture.ingest(&mut service);

    assert!(
        service
            .state
            .operation_for_source(&jj_operation(&second_operation))
            .is_some()
    );
    let explanation = service
        .state
        .query(&provenance_core::Query::Explain {
            root: provenance_jj::external(jj_operation(&second_operation)),
            max_depth: None,
            roles: std::collections::BTreeSet::from([
                provenance_core::ExplanationRole::Primary,
                provenance_core::ExplanationRole::Supporting,
            ]),
        })
        .expect("explain current operation");
    assert!(
        explanation
            .entities
            .contains(&provenance_jj::external(jj_operation(&first_operation)))
    );
}

#[test]
fn repeated_observation_is_idempotent() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "first\n");
    fixture.commit("first");
    let mut service = fixture.service();

    fixture.ingest(&mut service);
    let state_after_first = service.state.clone();
    let published_after_first = service
        .runtime
        .published_len()
        .expect("published operation count");

    fixture.ingest(&mut service);

    assert_eq!(service.state, state_after_first);
    assert_eq!(
        service
            .runtime
            .published_len()
            .expect("published operation count"),
        published_after_first
    );
    assert_eq!(
        service.state.operations.len(),
        state_after_first.operations.len()
    );
}

#[test]
fn observation_transaction_is_deterministic() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "first\n");
    fixture.commit("first");
    let repository = fixture.repository();

    let first = observe_repository(&repository).expect("first transaction");
    let second = observe_repository(&repository).expect("second transaction");
    assert_eq!(format!("{first:?}"), format!("{second:?}"));

    assert!(first.intents.iter().any(|intent| {
        matches!(intent, provenance_core::Intent::ObserveEntity(observation)
            if matches!(&observation.entity, address if address.kind.as_str() == "commit"))
    }));
    assert!(first.intents.iter().any(|intent| {
        matches!(intent, provenance_core::Intent::RecordEvent(event)

            if event.subjects.iter().any(|entity| matches!(entity, EntityRef::External(address) if address.kind.as_str() == "commit")))
    }));
}
#[test]
fn durable_ingestion_survives_restart_and_remains_idempotent() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "durable\n");
    fixture.commit("durable");
    let store_path = fixture.dir.path().join(".jj").join("provenance");

    let repository = fixture.repository();
    let mut first = open_service(&repository, &store_path).expect("open durable service");
    ingest_repository(&mut first, &repository).expect("first durable ingestion");
    let first_state = first.state.clone();
    let first_count = first
        .runtime
        .published_len()
        .expect("published operation count");
    assert!(first_count > 0);
    drop(first);

    let repository = fixture.repository();
    let mut restarted = open_service(&repository, &store_path).expect("reopen durable service");
    assert_eq!(restarted.state, first_state);
    ingest_repository(&mut restarted, &repository).expect("repeat durable ingestion");
    assert_eq!(restarted.state, first_state);
    assert_eq!(
        restarted
            .runtime
            .published_len()
            .expect("published operation count"),
        first_count
    );
}
