mod support;

use provenance_core::EntityRef;
use provenance_output_tty::{render_operation, render_operation_verbose};
use support::ProvenanceFixture;

#[test]
fn compact_output_matches_golden_text() {
    let fixture = ProvenanceFixture::new();
    assert_golden("compact.txt", render_fixture(&fixture, false));
}

#[test]
fn verbose_output_matches_golden_text() {
    let fixture = ProvenanceFixture::new();
    assert_golden("verbose.txt", render_fixture(&fixture, true));
}

#[test]
fn fixture_rebuilds_cross_namespace_graph_edges() {
    let fixture = ProvenanceFixture::new();
    let issue = EntityRef::External(support::beads_issue());
    let commit = EntityRef::External(support::jj_commit_address());
    let event = EntityRef::External(support::agent_event_address());

    assert_eq!(fixture.state.operations.len(), 4);
    assert!(
        fixture
            .state
            .projection
            .entities
            .get(&support::beads_issue())
            .is_some()
    );
    assert!(
        fixture
            .state
            .projection
            .outgoing
            .get(&issue)
            .expect("issue has outgoing edges")
            .iter()
            .any(|edge| edge.relation.to == commit)
    );
    assert!(
        fixture
            .state
            .projection
            .outgoing
            .get(&event)
            .expect("agent event has outgoing edges")
            .iter()
            .any(|edge| edge.relation.to == issue)
    );
    assert!(
        fixture
            .state
            .projection
            .outgoing
            .get(&event)
            .expect("agent event has outgoing edges")
            .iter()
            .any(|edge| edge.relation.to == commit)
    );
}

fn render_fixture(fixture: &ProvenanceFixture, verbose: bool) -> String {
    fixture
        .operations()
        .map(|operation| {
            if verbose {
                render_operation_verbose(operation)
            } else {
                render_operation(operation)
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn assert_golden(name: &str, actual: String) {
    let expected = match name {
        "compact.txt" => include_str!("fixtures/compact.txt"),
        "verbose.txt" => include_str!("fixtures/verbose.txt"),
        _ => unreachable!("golden fixture is declared by this test module"),
    }
    .trim_end_matches('\n');

    if actual != expected {
        eprintln!("--- actual {name} ---\n{actual}\n--- end actual ---");
    }
    assert_eq!(actual, expected, "renderer output differs from {name}");
}
