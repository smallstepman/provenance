# Canonical Model vs. Multiple Graphs

## Decision context

The provenance core is generic over a `Model`, while integrations currently use the
same primitive representation:

- `Id = String`
- `Seed = String`
- `ExternalId = String`
- `Payload = Vec<u8>`

The current direction is implemented: `ProvenanceModel` is the canonical
primitive universe and integration-specific names such as `JjModel` alias it.
The plugin host also targets this concrete model, avoiding model markers without
a real semantic distinction.

A canonical model does **not** imply one graph, one store, or one runtime.

## Model and graph are different concepts

`Model` describes the typed provenance vocabulary:

- entity and operation identifiers,
- external addresses,
- values and payloads,
- schemas,
- relations,
- sessions, actors, events, and resources.

A graph instance describes operational ownership:

- which store is authoritative,
- which runtime publishes it,
- which retention rules apply,
- which permissions control it,
- which integrations may write to it.

Several independent graphs can use the same model:

```rust
type RepositoryGraph =
    Service<ProvenanceModel, JjIdentity, JjRules, JjAdapter, JjRuntime>;

type AgentAuditGraph =
    Service<ProvenanceModel, AgentIdentity, AgentRules, AgentAdapter, AgentRuntime>;
```

The first graph may use JJ-backed persistence and repository retention. The
second may use a separate audit store, redact sensitive prompts, and retain data
for a shorter period. They remain separate graphs even though their data model is
shared.

## Who chooses the destination graph?

The plugin does not choose the destination. A plugin translates a source
observation into a typed transaction and declares its supported semantic surface.
The host or application orchestration layer decides where the transaction goes,
based on:

- routing configuration,
- event type,
- execution context,
- permissions,
- retention policy,
- available runtime and store.

A conceptual routing policy could be:

```text
beads.issue.updated -> repository
beads.issue.updated -> agent-audit
beads.issue.updated -> repository + agent-audit
```

The routing layer may receive context such as:

```text
agent session: sess-42
agent turn: turn-7
JJ operation: abc123
Beads issue: demo-17
```

The same Beads observation can then be sent to one or more graph-specific
bridges.

## What is a bridge?

A bridge is integration code between the plugin host and a concrete provenance
service. It is not part of the WASM component and it is not the generic plugin
manager.

The existing JJ bridge is:

```text
provenance-jj/src/plugins.rs::ingest_plugin_with_causal_parents
```

Its responsibilities are:

1. invoke `PluginManager::observe`,
2. receive the converted transaction,
3. attach causal parents known by the host,
4. submit the transaction through `JjService::process_transaction`.

A future agent bridge would follow the same shape, but target an
`AgentService` and attach agent-specific parents such as an agent turn or tool
call.

The bridge must not infer causal parents from plugin presentation data. The host
already knows the execution context and should pass those parents explicitly.

## Concrete future use case for multiple graphs

Consider an OMP agent running:

```text
bd update demo-17 --status in_progress
```

The Beads hook produces an issue snapshot. The agent host knows that the command
was executed during `turn-7`, in `session sess-42`, while the repository was at
JJ operation `abc123`.

The repository graph could record:

```text
JJ operation abc123
└── Beads issue observation demo-17
```

The agent-audit graph could record:

```text
Agent session sess-42
└── Agent turn turn-7
    └── Beads issue observation demo-17
```

The facts have different owners:

- JJ owns the repository operation and its object-retention requirements.
- The agent runtime owns the session, turn, and tool-execution audit.
- The Beads plugin owns only the translation of the Beads snapshot.

Separate graphs are useful here only if those ownership, security, retention, or
runtime boundaries are real requirements.

## Connecting separate graphs

`Relation<M>` and `EntityRef<M>` are parameterized by one model. They do not
provide a cross-model edge automatically. If separate graphs must be connected,
the connection must be explicit.

A minimal federation reference could contain the graph identity as well as the
source address:

```rust
struct GraphRef {
    graph_id: String,
    namespace: String,
    kind: String,
    id: String,
}

struct CrossGraphLink {
    from: GraphRef,
    relation: String,
    to: GraphRef,
}
```

For example:

```text
agent-audit / agent / turn / turn-7
    --caused-->
repository / jj / operation / abc123
```

The link can be represented in one of three ways:

1. **Shared graph:** use one `ProvenanceModel` and normal core relations. This
   is the simplest option when cross-graph queries are a primary requirement.
2. **External references:** each graph records an opaque reference to the other
   graph, and a derived resolver joins the references.
3. **Federation registry:** an independent link store records immutable
   cross-graph links, while the source graphs remain authoritative for their own
   facts.

The repository does not currently define a federation contract. It should not be
added until separate authoritative graphs are an actual requirement.

## When should model markers be split?

Separate markers such as `RepositoryModel` and `AgentModel` become justified when
there is a concrete problem that a shared model cannot solve:

- a transaction from one graph must be rejected by another graph at compile time;
- separate graphs have genuinely different model associated types;
- different identity policies must be tied to distinct APIs;
- the same process owns multiple authoritative stores and cross-wiring would be
  dangerous;
- tests or production code repeatedly demonstrate accidental cross-graph
  publication.

A concrete type-level boundary would look like:

```rust
fn publish_to_repository(
    graph: &mut RepositoryService,
    transaction: Transaction<RepositoryModel>,
) {
    // ...
}

fn publish_to_agent_audit(
    graph: &mut AgentAuditService,
    transaction: Transaction<AgentModel>,
) {
    // ...
}
```

Passing `Transaction<AgentModel>` to the repository function would then fail at
compile time.

If the only differences are store, runtime, routing, retention, or permissions,
separate model markers conflate two concepts. Use one model and separate graph
wrappers instead.

## Migration path if multiple models become necessary

The core already uses `M: Model`, so introducing markers later is possible:

1. define `RepositoryModel` and `AgentModel`;
2. implement `Model` for both;
3. split graph-specific services and bridge APIs;
4. make the plugin host generic over the models if the WASM representation is
   compatible;
5. add explicit cross-graph references if queries must span the graphs.

In the current shaping phase, this is primarily an API and type migration. It
should be driven by a real second graph rather than by speculation.

## Payload boundary

The checked-in plugin WIT ABI represents payloads as `list<u8>`, and the host
projects those observations into the canonical `ProvenanceModel` (also exported
as `PluginModel`). This is a deliberate concrete boundary, not a generic
compatibility trait.

If a future integration needs a distinct model marker, the host can become
generic again only after an explicit conversion contract is defined for that
model's associated types, including payload conversion. Different payload types
would then be a reason to add such a boundary; they are not a reason to retain
one speculatively.

## Current recommendation

Use one canonical `ProvenanceModel` and keep graph separation at the service,
store, runtime, and routing layers. Add distinct model markers only when a real
compile-time or data-model boundary appears.

Keep bridge responsibilities explicit and host-owned. If future requirements
need cross-graph explanations, design the federation reference contract before
splitting the model types.
