mod support;

use provenance_core::EntityRef;
use provenance_output_tty::{
    ChronicleQuery, render_chronicle, render_operation, render_operation_verbose,
};
use support::ProvenanceFixture;

#[test]
fn compact_output_matches_golden_text() {
    let fixture = ProvenanceFixture::new();
    assert_golden("compact.txt", render_fixture(&fixture, false));
}

#[test]
fn compact_output_exposes_lineage_and_evidence() {
    let fixture = ProvenanceFixture::new();
    let output = render_fixture(&fixture, false);

    for fragment in [
        "Entity beads/issue/bd-42",
        "title=\"Connect agent turn to JJ implementation\"",
        "status=\"in_progress\"",
        "agent/triggered-by [agent@1]: agent/event/agent-event-0001/start -> beads/issue/bd-42",
        "agent/implemented-by [agent@1]: beads/issue/bd-42 -> jj/commit/deadbeef0001",
        "commit_id=\"deadbeef0001\"",
        "timestamp=\"2026-08-31T11:59:58Z@+00:00\"",
        "conversation-ref=\"~/.omp/agents/session-0001.jsonl\"",
        "evidence-ref=\"atof/events.jsonl#offset=777\"",
        "outcome=\"success\"",
    ] {
        assert!(
            output.contains(fragment),
            "compact output missing {fragment:?}"
        );
    }
    assert!(!output.contains("private prompt text"));
}

#[test]
fn verbose_output_matches_golden_text() {
    let fixture = ProvenanceFixture::new();
    assert_golden("verbose.txt", render_fixture(&fixture, true));
}

#[test]
fn chronicle_output_matches_golden_text() {
    let fixture = ProvenanceFixture::new();
    let target = EntityRef::External(support::beads_issue());
    let query = ChronicleQuery::why(target, 3);
    assert_golden(
        "chronicle.txt",
        render_chronicle(&fixture.state, &query).expect("chronicle query succeeds"),
    );
}

#[test]
fn chronicle_output_keeps_handles_and_excludes_payloads() {
    let fixture = ProvenanceFixture::new();
    let target = EntityRef::External(support::beads_issue());
    let output = render_chronicle(&fixture.state, &ChronicleQuery::why(target, 3))
        .expect("chronicle query succeeds");

    for fragment in [
        "@chronicle 1",
        "query: why bd://bd-42",
        "target: bd://bd-42 \"Connect agent turn to JJ implementation\"",
        "scope: causal depth=3",
        "folded: sessions=1 subagents=1",
        "detail: prov://bd/bd-42",
        "FEATURE",
        "AGENT agent://event/agent-event-0001/start",
        "conversation-ref: \"~/.omp/agents/session-0001.jsonl\"",
        "evidence-ref: \"atof/events.jsonl#offset=777\"",
        "telemetry phoenix trace trace-000000000001",
        "relation: bd://bd-42 -[agent/implemented-by agent@1]-> jj://deadbeef0001",
        "@end",
        "result: bd://bd-42 records=3 operations=3",
        "open: bd://bd-17 status=\"open\"",
    ] {
        assert!(
            output.contains(fragment),
            "chronicle output missing {fragment:?}"
        );
    }
    assert!(!output.contains("private prompt text"));
}

#[test]
fn chronicle_depth_zero_keeps_target_but_not_edges() {
    let fixture = ProvenanceFixture::new();
    let target = EntityRef::External(support::beads_issue());
    let output = render_chronicle(&fixture.state, &ChronicleQuery::why(target, 0))
        .expect("bounded chronicle query succeeds");

    assert!(output.contains("scope: causal depth=0"));
    assert!(output.contains("path: bd://bd-42"));
    assert!(output.contains("folded: sessions=0 subagents=0 operations=1"));
    assert!(!output.contains("agent/implemented-by"));
    assert!(output.contains("result: bd://bd-42 records=1 operations=1 entities=1 edges=0"));
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
        "chronicle.txt" => include_str!("fixtures/chronicle.txt"),
        _ => unreachable!("golden fixture is declared by this test module"),
    }
    .trim_end_matches('\n');

    if actual != expected {
        eprintln!("--- actual {name} ---\n{actual}\n--- end actual ---");
    }
    assert_eq!(actual, expected, "renderer output differs from {name}");
}
