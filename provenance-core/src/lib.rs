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
use thiserror::Error;

// ERRORS
// =============================================================================

#[derive(Clone, Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    #[error("required value is missing")]
    Missing,
    #[error("immutable identity collision")]
    IdentityCollision,
    #[error("source parent is missing")]
    MissingSourceParent,
    #[error("source is already mapped to a different operation")]
    SourceAlreadyMappedDifferently,
    #[error("operation parent is missing")]
    MissingOperationParent,
    #[error("operation graph contains a cycle")]
    OperationCycle,
    #[error("schema is missing")]
    MissingSchema,
    #[error("schema dependency is missing")]
    MissingSchemaDependency,
    #[error("schema namespace is invalid")]
    InvalidSchemaNamespace,
    #[error("entity schema is missing")]
    MissingEntitySchema,
    #[error("relation schema is missing")]
    MissingRelationSchema,
    #[error("entity type is invalid")]
    InvalidEntityType,
    #[error("relation endpoint is invalid")]
    InvalidRelationEndpoint,
    #[error("required field is missing")]
    MissingRequiredField,
    #[error("field is not declared by the schema")]
    UnknownField,
    #[error("field value has an invalid type")]
    InvalidFieldType,
    #[error("session is missing")]
    MissingSession,
    #[error("session has already ended")]
    SessionAlreadyEnded,
    #[error("actor is missing")]
    MissingActor,
    #[error("actor does not belong to the specified session")]
    ActorSessionMismatch,
    #[error("event is missing")]
    MissingEvent,
    #[error("object is missing")]
    MissingObject,
    #[error("retention claim is unknown")]
    UnknownRetentionClaim,
    #[error("self-relations are not allowed")]
    InvalidSelfRelation,
    #[error("named query is invalid")]
    InvalidNamedQuery,
    #[error("provenance merge conflict")]
    MergeConflict,
    #[error("provenance policy rejected the operation")]
    PolicyRejected,
}

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
