mod support;

use provenance_core::{
    Attributes, EventIntent, Intent, Operation, OperationId, ProvenanceStore, Resource,
    ResourceRequirement, RetentionStrength, Runtime, Transaction,
};
use provenance_jj::{DirectoryProvenanceStore, JjRuntime, JjRuntimeError, jj_commit, open_service};
use std::collections::BTreeSet;
use std::thread;

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

fn release_transaction(
    claim: provenance_core::ClaimId<provenance_jj::JjModel>,
) -> Transaction<provenance_jj::JjModel> {
    Transaction {
        seed: format!("release:{claim:?}"),
        source: None,
        intents: vec![Intent::ReleaseRetention { claim }],
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

#[test]
fn durable_release_reconciles_after_crash_between_publish_and_finalize() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "crash\n");
    fixture.commit("crash");
    let commit_id = fixture.head_commit();
    let store_path = fixture.dir.path().join(".jj").join("provenance");
    let repository = fixture.repository();
    let mut service = open_service(&repository, &store_path).expect("open durable service");
    fixture.ingest(&mut service);

    commit_plan(
        &mut service,
        retention_transaction(&commit_id, RetentionStrength::Pinned),
    )
    .expect("initial pinned retention");
    let claim = service
        .state
        .projection
        .active_retention
        .iter()
        .next()
        .map(|(claim, _)| claim.clone())
        .expect("pinned claim");
    let resource = Resource::from(jj_commit(commit_id.clone()));

    let plan = service
        .kernel
        .transact(&service.state, release_transaction(claim))
        .expect("release plan");
    for requirement in &plan.prepare {
        service
            .runtime
            .prepare(requirement)
            .expect("release prepare");
    }
    service
        .runtime
        .publish(&plan.operation)
        .expect("release publish");
    service.state = plan.next_state;
    // Simulate a process exit before finalize. The release operation is already
    // authoritative; startup must derive and reconcile the missing finalization.
    drop(service);

    let repository = fixture.repository();
    let mut recovered = open_service(&repository, &store_path).expect("recover runtime");
    recovered.runtime.reconcile().expect("reconcile retention");
    let error = recovered
        .runtime
        .retention()
        .verify_pinned(&resource)
        .expect_err("crash recovery should release the pin");
    assert!(error.to_string().contains("does not exist"));
}

#[test]
fn concurrent_store_publish_of_same_operation_is_idempotent() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "concurrent\n");
    fixture.commit("concurrent");
    let mut service = fixture.service();
    fixture.ingest(&mut service);
    let operation = service
        .state
        .operations
        .iter()
        .find(|(_, operation)| operation.parents.is_empty())
        .map(|(_, operation)| operation.clone())
        .expect("ingested operation");
    let store_path = fixture.dir.path().join(".jj").join("concurrent");
    let repository = fixture.repository();

    let handles = (0..2)
        .map(|_| {
            let operation = operation.clone();
            let store_path = store_path.clone();
            let repo = repository.repo().clone();
            thread::spawn(move || {
                let mut runtime = JjRuntime::open(repo, &store_path).expect("open runtime");
                runtime
                    .publish(&operation)
                    .expect("publish concurrent operation");
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().expect("concurrent publisher");
    }

    let repository = fixture.repository();
    let runtime = JjRuntime::open(repository.repo().clone(), &store_path).expect("reopen runtime");
    assert_eq!(
        runtime.published_len().expect("published operation count"),
        1
    );
    assert_eq!(
        runtime.store().heads().expect("authoritative heads").len(),
        1
    );
}

#[test]
fn crash_before_head_publication_does_not_make_orphan_authoritative() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "orphan\n");
    fixture.commit("orphan");
    let mut service = fixture.service();
    fixture.ingest(&mut service);
    let operation = service
        .state
        .operations
        .iter()
        .find(|(_, operation)| operation.parents.is_empty())
        .map(|(_, operation)| operation.clone())
        .expect("observed operation");
    let store_path = fixture.dir.path().join(".jj").join("crash-before-head");

    let mut store = DirectoryProvenanceStore::open(&store_path).expect("open authority");
    store
        .put_operation(&operation)
        .expect("persist immutable operation");
    drop(store);

    let mut reopened = DirectoryProvenanceStore::open(&store_path).expect("reopen authority");
    assert!(reopened.heads().expect("read heads").is_empty());
    assert!(
        reopened
            .get_operation(&operation.id)
            .expect("read orphan")
            .is_some()
    );
    assert_eq!(
        reopened
            .reachable_operation_count()
            .expect("reachable count"),
        0
    );
    let next = BTreeSet::from([operation.id.clone()]);
    reopened
        .publish_heads(&BTreeSet::new(), &next)
        .expect("publish recovered operation");
    assert_eq!(
        reopened
            .reachable_operation_count()
            .expect("reachable count after recovery"),
        1
    );
}

#[test]
fn concurrent_divergent_operations_preserve_both_heads() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "branches\n");
    fixture.commit("branches");
    let mut service = fixture.service();
    fixture.ingest(&mut service);
    let root = service
        .state
        .operations
        .iter()
        .find(|(_, operation)| operation.parents.is_empty())
        .map(|(_, operation)| operation.clone())
        .expect("root operation");
    let operation_a = Operation {
        id: OperationId::<provenance_jj::JjModel>::new("branch-a".to_owned()),
        parents: BTreeSet::from([root.id.clone()]),
        source: None,
        facts: Vec::new(),
        attributes: Attributes::new(),
    };
    let operation_b = Operation {
        id: OperationId::<provenance_jj::JjModel>::new("branch-b".to_owned()),
        parents: BTreeSet::from([root.id.clone()]),
        source: None,
        facts: Vec::new(),
        attributes: Attributes::new(),
    };
    let store_path = fixture.dir.path().join(".jj").join("divergent");
    let mut store = DirectoryProvenanceStore::open(&store_path).expect("open authority");
    store.put_operation(&root).expect("persist root");
    store
        .publish_heads(&BTreeSet::new(), &BTreeSet::from([root.id.clone()]))
        .expect("publish root");
    drop(store);

    let handles = [operation_a.clone(), operation_b.clone()]
        .into_iter()
        .map(|operation| {
            let store_path = store_path.clone();
            thread::spawn(move || {
                let mut store =
                    DirectoryProvenanceStore::open(store_path).expect("open branch authority");
                store.put_operation(&operation).expect("persist branch");
                let expected = store.heads().expect("read branch heads");
                let mut next = expected.clone();
                for parent in &operation.parents {
                    next.remove(parent);
                }
                next.insert(operation.id.clone());
                store
                    .publish_heads(&expected, &next)
                    .expect("publish branch");
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().expect("branch publisher");
    }

    let store = DirectoryProvenanceStore::open(&store_path).expect("reopen divergent authority");
    assert_eq!(
        store.heads().expect("divergent heads"),
        BTreeSet::from([operation_a.id, operation_b.id])
    );
    assert_eq!(
        store
            .reachable_operation_count()
            .expect("reachable divergent operations"),
        3
    );
}

#[test]
fn pinned_historical_commit_survives_jj_rewrite() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "before\n");
    fixture.commit("before");
    let historical_commit = fixture.head_commit();
    let mut service = fixture.service();
    fixture.ingest(&mut service);

    fixture.write_file("README", "after\n");
    fixture.squash();
    fixture.ingest(&mut service);
    assert_ne!(historical_commit, fixture.head_commit());

    commit_plan(
        &mut service,
        retention_transaction(&historical_commit, RetentionStrength::Pinned),
    )
    .expect("pin historical commit");
    let resource = Resource::from(jj_commit(historical_commit));
    service
        .runtime
        .retention()
        .verify_pinned(&resource)
        .expect("historical commit pin resolves");
}

#[test]
fn pinned_commit_survives_jj_gc_and_aggressive_git_pruning() {
    let fixture = support::Fixture::new();
    let root_operation = fixture.root_operation_id();

    fixture.write_file("README", "retained\n");
    fixture.commit("retained");
    let retained_commit = fixture.head_commit();
    let mut service = fixture.service();
    fixture.ingest(&mut service);
    commit_plan(
        &mut service,
        retention_transaction(&retained_commit, RetentionStrength::Pinned),
    )
    .expect("pin historical commit");
    let retained_resource = Resource::from(jj_commit(retained_commit.clone()));

    fixture.write_file("README", "unretained one\n");
    fixture.squash();
    let first_unretained_commit = fixture.head_commit();
    fixture.write_file("README", "unretained two\n");
    fixture.squash();
    let second_unretained_commit = fixture.head_commit();
    assert_ne!(retained_commit, first_unretained_commit);
    assert_ne!(first_unretained_commit, second_unretained_commit);

    // Restore the initial view and discard every operation that could retain
    // either rewritten commit before running the real JJ garbage collector.
    drop(service);
    fixture.restore_operation(&root_operation);
    fixture.abandon_operations_since(&root_operation);
    fixture.gc();
    fixture.git_prune();

    assert!(
        fixture.commit_exists(&retained_commit),
        "pinned commit was collected"
    );
    assert!(
        !fixture.commit_exists(&first_unretained_commit),
        "first rewritten commit remained reachable"
    );
    assert!(
        !fixture.commit_exists(&second_unretained_commit),
        "second rewritten commit remained reachable"
    );

    let recovered = fixture.service();
    recovered
        .runtime
        .retention()
        .verify_pinned(&retained_resource)
        .expect("packed provenance ref still resolves to retained commit");
}

#[test]
fn released_commit_is_collectable_after_jj_gc_and_aggressive_git_pruning() {
    let fixture = support::Fixture::new();
    let root_operation = fixture.root_operation_id();

    fixture.write_file("README", "released\n");
    fixture.commit("released");
    let released_commit = fixture.head_commit();
    let mut service = fixture.service();
    fixture.ingest(&mut service);
    commit_plan(
        &mut service,
        retention_transaction(&released_commit, RetentionStrength::Pinned),
    )
    .expect("pin commit before release");
    let released_resource = Resource::from(jj_commit(released_commit.clone()));
    // Exercise deletion after Git has packed the provenance ref.
    fixture.git_prune();
    let claim = service
        .state
        .projection
        .active_retention
        .iter()
        .next()
        .map(|(claim, _)| claim.clone())
        .expect("pinned claim");

    commit_plan(&mut service, release_transaction(claim)).expect("release commit pin");
    assert!(
        service
            .runtime
            .retention()
            .verify_pinned(&released_resource)
            .is_err(),
        "released commit still has a provenance ref"
    );

    fixture.write_file("README", "replacement\n");
    fixture.squash();
    let replacement_commit = fixture.head_commit();
    assert_ne!(released_commit, replacement_commit);

    drop(service);
    fixture.restore_operation(&root_operation);
    fixture.abandon_operations_since(&root_operation);
    fixture.gc();
    fixture.git_prune();

    assert!(
        !fixture.commit_exists(&released_commit),
        "released commit remained reachable after pruning"
    );
    assert!(
        !fixture.commit_exists(&replacement_commit),
        "replacement commit remained reachable after pruning"
    );
}
