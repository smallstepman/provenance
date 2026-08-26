//! provenance_core.rs
//!
//! Pure provenance kernel.
//!
//! The kernel knows:
//!
//!   identity
//!   source causality
//!   immutable provenance operations
//!   sessions / actors / events / evidence
//!   typed heterogeneous entities
//!   versioned schemas
//!   schema-checked relations
//!   retention semantics
//!   retention aggregation
//!   publication protocol
//!   graph projections
//!   declarative queries
//!   explanation semantics
//!   deterministic merge / replay
//!
//! The kernel knows NOTHING about:
//!
//!   files
//!   paths
//!   VCSes
//!   databases
//!   terminals
//!   clocks
//!   randomness
//!   sockets
//!   async runtimes
//!   WASM
//!   WIT
//!   plugin loading
//!   serialization
//!
//! -----------------------------------------------------------------------------
//!
//! ```text
//!                        EXTERNAL UNIVERSES
//!
//!     source A        source B        source C
//!        │               │               │
//!        └───────────────┼───────────────┘
//!                        │
//!                  typed observations
//!                        │
//!                        ▼
//!                  provenance-core
//!
//!           schema checked heterogeneous graph
//!                        │
//!          ┌─────────────┼──────────────┐
//!          │             │              │
//!      operations      queries       retention
//!          │             │              │
//!          ▼             ▼              ▼
//!      persistence    executors      runtimes
//! ```
//!
//! -----------------------------------------------------------------------------
//!
//! The authoritative state is ONLY the immutable provenance Operation DAG.
//!
//! Projection is disposable and rebuildable.
//!
//! State + Transaction -> CommitPlan
//!
//! Commit protocol:
//!
//!   prepare
//!      │
//!      │  acquire/strengthen retention
//!      │  materialize immutable objects
//!      ▼
//!   publish ONE immutable operation
//!      │
//!      ▼
//!   operation is authoritative
//!      │
//!      ▼
//!   finalize
//!      │
//!      │  weaken/release retention
//!      ▼
//!   done
//!
//! A crash before publish => operation did not happen.
//! A crash after publish => operation DID happen; finalize can be reconciled.
//!
//! =============================================================================

pub use provenance_data_model::{
    Actor, ActorId, Attributes, ClaimId, EntityAddress, EntityKind, EntityObservation, EntityRef,
    EntitySchema, EntityType, EntityTypePattern, EventId, EventTag, ExplanationDirection,
    ExplanationRole, ExplanationSemantics, FieldName, FieldSchema, Id, IdentityKind,
    IdentityScheme, InternalEntityKind, InternalEntityRef, Key, Model, Namespace, Object, ObjectId,
    ObjectTag, OperationId, OperationTag, QueryName, Relation, RelationName, RelationSchema,
    RelationType, Replica, ReplicaId, ReplicaTag, SchemaDefinition, SchemaKey, SchemaVersion,
    Session, SessionId, SessionTag, SourceAnchor, SourceOperation, Value, ValueType,
};

use serde::{Deserialize, Serialize};
use std::fmt;

// ERRORS
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    Missing,
    IdentityCollision,
    MissingSourceParent,
    SourceAlreadyMappedDifferently,
    MissingOperationParent,
    OperationCycle,
    MissingSchema,
    MissingSchemaDependency,
    InvalidSchemaNamespace,
    MissingEntitySchema,
    MissingRelationSchema,
    InvalidEntityType,
    InvalidRelationEndpoint,
    MissingRequiredField,
    UnknownField,
    InvalidFieldType,
    MissingSession,
    SessionAlreadyEnded,
    MissingActor,
    ActorSessionMismatch,
    MissingEvent,
    MissingObject,
    UnknownRetentionClaim,
    InvalidSelfRelation,
    InvalidNamedQuery,
    MergeConflict,
    PolicyRejected,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>; // =============================================================================

mod integration;
mod protocol;
mod query;
mod retention;
mod state;

pub use integration::*;
pub use protocol::*;
pub use query::*;
pub use retention::*;
pub use state::*;

// =============================================================================
// WHAT PLUGINS ACTUALLY CONTRIBUTE
// =============================================================================
//
// A task-tracker plugin might register:
//
// SchemaDefinition {
//     key: tracker@1,
//     entities: {
//         task: {
//             title: string required,
//             status: string required,
//             ...
//         },
//         epic: ...
//     },
//     relations: {
//         belongs-to: task -> epic,
//         blocked-by: task -> task,
//         motivated-by: task -> docs:rfc,
//         implemented-by: task -> vcs:change,
//     },
// }
//
// It then emits:
//
// EntityObservation {
//     entity: tracker://task/PROJ-42,
//     schema: tracker@1,
//     attributes: ...
// }
//
// and:
//
// Relation {
//     tracker://task/PROJ-42
//          -- motivated-by -->
//     docs://rfc/0001
// }
//
//
// A docs plugin might register:
//
//     docs:rfc
//     docs:adr
//
//     supersedes
//     implements
//     motivated-by
//
//
// An agent plugin might register:
//
//     agent:run
//     agent:task
//     agent:checkpoint
//
//     worked-on
//     produced
//     validated
//
//
// A source-control adapter might register:
//
//     vcs:commit
//     vcs:change
//     vcs:operation
//
//     replaces
//     derived-from
//
//
// None of these names exist in provenance-core itself.
//
// =============================================================================
//
// QUERY EXAMPLE
//
// URI parsing happens outside:
//
//     "jj://commit/abc123"
//
// becomes:
//
//     EntityRef::External(
//         EntityAddress {
//             namespace: "jj",
//             kind: "commit",
//             id: "abc123",
//         }
//     )
//
// `why` becomes:
//
//     Query::Explain {
//         root,
//         max_depth: None,
//         roles: {
//             Primary,
//             Supporting,
//         },
//     }
//
// The query engine follows only relationships whose versioned schemas declare:
//
//     explanation: Some(...)
//
// So a plugin automatically integrates into `why` by supplying proper semantic
// relation definitions.
//
// No `why` code needs to know what:
//     JJ
//     task tracker
//     RFC
//     agent run
//     CI system
// actually are.
//
// =============================================================================
//
// WIT/WASM BOUNDARY
//
// A plugin-host crate should expose WIT representations of:
//
//     SchemaDefinition
//     EntityObservation
//     Relation
//     EventIntent
//     Transaction
//     QueryDefinition
//
// and translate them into these Rust types.
//
// WASM plugins SHOULD NOT receive direct access to:
//
//     persistence
//     retention roots
//     operation heads
//     derived query state
//     authoritative state mutation
//
// They propose typed semantic Transactions.
//
// The kernel validates them.
//
// =============================================================================
//
// STORAGE BOUNDARY
//
// Authoritative persistence only needs:
//
//     immutable Operation objects
//     immutable Object payloads
//     operation head publication
//
// Everything else:
//
//     entities
//     graph edges
//     schemas
//     active retention
//     derived query state
//     full-text search state
//
// can be rebuilt from the Operation DAG.
//
// =============================================================================
//
// RETENTION BOUNDARY
//
// Kernel:
//
//     task X requires commit C @ Pinned
//     event Y requires commit C @ Escrowed
//
// derives:
//
//     effective(C) = Escrowed
//
// If Y disappears:
//
//     effective(C): Escrowed -> Pinned
//
// emits AFTER publication:
//
//     RetentionTransition::Weakened
//
// If X also disappears:
//
//     effective(C): Pinned -> None
//
// emits AFTER publication:
//
//     RetentionTransition::BecameUnrequired
//
// Implementation decides:
//
//     Git ref
//     object lock
//     remote replica
//     independent object copy
//     etc.
//
// =============================================================================
//
// DISTRIBUTED MODEL
//
// Host A:
//
//     P1 -> P2
//
// Host B:
//
//     P1 -> P3
//
// merge:
//
//          P1
//         /  \
//       P2    P3
//
// is simply:
//
//     union immutable operations
//     +
//     deterministic projection rebuild
//
// =============================================================================
