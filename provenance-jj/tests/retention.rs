mod support;

use std::collections::BTreeSet;

use provenance_core::{
    Attributes, EventIntent, Intent, Resource, ResourceRequirement, RetentionStrength, Runtime,
    Transaction,
};
use provenance_jj::{JjRuntimeError, jj_commit};

fn retention_transaction(
    commit_id: &str,
    strength: RetentionStrength,
) -> Transaction<provenance_jj::JjModel> {
    let entity = jj_commit(commit_id);
    Transaction {
        seed: format!("retention:{commit_id}:{strength:?}"),
        source: None,
        intents: vec![Intent::RecordEvent(EventIntent {
            session: None,
            actor: None,
            additional_parents: BTreeSet::new(),
            subjects: BTreeSet::from([entity.clone()]),
            relations: BTreeSet::new(),
            requires: BTreeSet::from([ResourceRequirement {
                resource: Resource::from(entity),
                strength,
            }]),
            attributes: Attributes::new(),
        })],
        attributes: Attributes::new(),
    }
}

fn commit_plan(
    service: &mut support::JjService,
    transaction: Transaction<provenance_jj::JjModel>,
) -> Result<(), JjRuntimeError> {
    let plan = service
        .kernel
        .transact(&service.state, transaction)
        .expect("retention transaction plan");
    for requirement in &plan.prepare {
        service.runtime.prepare(requirement)?;
    }
    service.runtime.publish(&plan.operation)?;
    service.state = plan.next_state;
    for action in &plan.finalize {
        service.runtime.finalize(action)?;
    }
    Ok(())
}

#[test]
fn pinned_commit_creates_and_verifies_jj_protection_ref() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "retained\n");
    fixture.commit("retained");
    let commit_id = fixture.head_commit();
    let mut service = fixture.service();
    fixture.ingest(&mut service);

    commit_plan(
        &mut service,
        retention_transaction(&commit_id, RetentionStrength::Pinned),
    )
    .expect("pinned retention");

    let resource = provenance_core::Resource::from(jj_commit(commit_id.clone()));
    service
        .runtime
        .retention()
        .verify_pinned(&resource)
        .expect("keep ref exists and resolves to commit");
    assert_eq!(
        provenance_jj::keep_ref_name(&commit_id),
        format!("refs/jj-prov/keep/{commit_id}")
    );
}

#[test]
fn escrowed_retention_is_explicitly_unsupported() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "escrow\n");
    fixture.commit("escrow");
    let commit_id = fixture.head_commit();
    let mut service = fixture.service();
    fixture.ingest(&mut service);

    let error = commit_plan(
        &mut service,
        retention_transaction(&commit_id, RetentionStrength::Escrowed),
    )
    .expect_err("escrow should be rejected");
    assert!(matches!(error, JjRuntimeError::Retention(_)));
    assert!(error.to_string().contains("Escrowed"));
}
