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

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt::{self, Debug},
    marker::PhantomData,
};

// =============================================================================
// MODEL
// =============================================================================

pub trait Key: Clone + Debug + Eq + Ord + Send + Sync + 'static {}

impl<T> Key for T where T: Clone + Debug + Eq + Ord + Send + Sync + 'static {}

/// Consumer-supplied primitive universe.
///
/// Typed graph containers are deterministically ordered, so the model marker
/// itself carries the same bounds as the kernel's `Key` universe.
pub trait Model: Key {
    /// Raw internal provenance ID.
    type Id: Key;

    /// Deterministic seed used for internal identity derivation.
    type Seed: Key;

    /// ID component belonging to an external system.
    ///
    /// Namespace and kind are owned by the kernel's EntityAddress.
    ///
    /// ```text
    /// "abc123"
    /// "PROJ-42"
    /// "0001"
    /// ```
    type ExternalId: Key;

    /// Opaque immutable evidence payload.
    ///
    /// Typed SDLC entities should normally use Attributes instead.
    type Payload: Clone + Debug + PartialEq + Send + Sync + 'static;
}

// =============================================================================
// SMALL VALUE TYPES
// =============================================================================

macro_rules! text_newtype {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub Box<str>);

        impl $name {
            pub fn new(value: impl Into<Box<str>>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }
    };
}

text_newtype!(Namespace);
text_newtype!(EntityKind);
text_newtype!(RelationName);
text_newtype!(FieldName);
text_newtype!(SchemaVersion);
text_newtype!(QueryName);

// =============================================================================
// STRONGLY TYPED INTERNAL IDS
// =============================================================================

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Id<R, T> {
    raw: R,
    _tag: PhantomData<fn() -> T>,
}

impl<R: Clone, T> Clone for Id<R, T> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            _tag: PhantomData,
        }
    }
}

impl<R, T> Id<R, T> {
    pub fn new(raw: R) -> Self {
        Self {
            raw,
            _tag: PhantomData,
        }
    }

    pub fn raw(&self) -> &R {
        &self.raw
    }

    pub fn into_raw(self) -> R {
        self.raw
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum OperationTag {}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum EventTag {}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SessionTag {}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum ActorTag {}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum ObjectTag {}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum ClaimTag {}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum ReplicaTag {}

pub type OperationId<M> = Id<<M as Model>::Id, OperationTag>;

pub type EventId<M> = Id<<M as Model>::Id, EventTag>;

pub type SessionId<M> = Id<<M as Model>::Id, SessionTag>;

pub type ActorId<M> = Id<<M as Model>::Id, ActorTag>;

pub type ObjectId<M> = Id<<M as Model>::Id, ObjectTag>;

pub type ClaimId<M> = Id<<M as Model>::Id, ClaimTag>;

pub type ReplicaId<M> = Id<<M as Model>::Id, ReplicaTag>;

// =============================================================================
// GENERIC COLLECTIONS
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeclareOutcome {
    Inserted,
    AlreadyPresent,
}

/// Append-only identity -> immutable value.
///
/// absent + value           => insert
/// same identity + same     => idempotent
/// same identity + different=> corruption/collision
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de> + Ord, V: Deserialize<'de>",
))]
pub struct Ledger<K, V> {
    inner: BTreeMap<K, V>,
}

impl<K, V> Default for Ledger<K, V> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl<K, V> Ledger<K, V>
where
    K: Key,
    V: Clone + PartialEq,
{
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    pub fn require(&self, key: &K) -> Result<&V> {
        self.get(key).ok_or(Error::Missing)
    }

    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    pub fn declare(&mut self, key: K, value: V) -> Result<DeclareOutcome> {
        match self.inner.get(&key) {
            None => {
                self.inner.insert(key, value);
                Ok(DeclareOutcome::Inserted)
            }

            Some(existing) if existing == &value => Ok(DeclareOutcome::AlreadyPresent),

            Some(_) => Err(Error::IdentityCollision),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner.iter()
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.inner.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.inner.values()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Replaceable / derived mapping.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de> + Ord, V: Deserialize<'de>",
))]
pub struct Index<K, V> {
    inner: BTreeMap<K, V>,
}

impl<K, V> Default for Index<K, V> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl<K, V> Index<K, V>
where
    K: Key,
{
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.inner.get_mut(key)
    }

    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    pub fn set(&mut self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner.iter()
    }
}

/// Derived set.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "K: Serialize", deserialize = "K: Deserialize<'de> + Ord",))]
pub struct Set<K> {
    inner: BTreeSet<K>,
}

impl<K> Default for Set<K> {
    fn default() -> Self {
        Self {
            inner: BTreeSet::new(),
        }
    }
}

impl<K> Set<K>
where
    K: Key,
{
    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains(key)
    }

    pub fn insert(&mut self, key: K) -> bool {
        self.inner.insert(key)
    }

    pub fn remove(&mut self, key: &K) -> bool {
        self.inner.remove(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = &K> {
        self.inner.iter()
    }

    pub fn cloned(&self) -> BTreeSet<K> {
        self.inner.clone()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Derived K -> `set<V>`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de> + Ord, V: Deserialize<'de> + Ord",
))]
pub struct MultiIndex<K, V> {
    inner: BTreeMap<K, BTreeSet<V>>,
}

impl<K, V> Default for MultiIndex<K, V> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl<K, V> MultiIndex<K, V>
where
    K: Key,
    V: Key,
{
    pub fn get(&self, key: &K) -> Option<&BTreeSet<V>> {
        self.inner.get(key)
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.inner.entry(key).or_default().insert(value);
    }

    pub fn remove(&mut self, key: &K, value: &V) {
        if let Some(values) = self.inner.get_mut(key) {
            values.remove(value);

            if values.is_empty() {
                self.inner.remove(key);
            }
        }
    }
}

// =============================================================================
// IDENTITY DERIVATION
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdentityKind {
    Operation,
    Event,
    Session,
    Actor,
    Object,
    Claim,
    Replica,
}

/// Only one primitive must be implemented.
pub trait IdentityScheme<M: Model>: Send + Sync {
    fn derive(&self, kind: IdentityKind, seed: &M::Seed, discriminator: &str) -> M::Id;

    fn operation(&self, seed: &M::Seed) -> OperationId<M> {
        OperationId::<M>::new(self.derive(IdentityKind::Operation, seed, "operation"))
    }

    fn event(&self, seed: &M::Seed, index: usize) -> EventId<M> {
        EventId::<M>::new(self.derive(IdentityKind::Event, seed, &format!("event:{index}")))
    }

    fn session(&self, seed: &M::Seed) -> SessionId<M> {
        SessionId::<M>::new(self.derive(IdentityKind::Session, seed, "session"))
    }

    fn actor(&self, seed: &M::Seed) -> ActorId<M> {
        ActorId::<M>::new(self.derive(IdentityKind::Actor, seed, "actor"))
    }

    fn object(&self, seed: &M::Seed, index: usize) -> ObjectId<M> {
        ObjectId::<M>::new(self.derive(IdentityKind::Object, seed, &format!("object:{index}")))
    }

    fn claim(&self, seed: &M::Seed, event_index: usize, requirement_index: usize) -> ClaimId<M> {
        ClaimId::<M>::new(self.derive(
            IdentityKind::Claim,
            seed,
            &format!("claim:{event_index}:{requirement_index}"),
        ))
    }

    fn replica(&self, seed: &M::Seed) -> ReplicaId<M> {
        ReplicaId::<M>::new(self.derive(IdentityKind::Replica, seed, "replica"))
    }
}

// =============================================================================
// EXTERNAL ENTITY ADDRESSING
// =============================================================================

/// Canonical structured external identity.
///
/// ```text
/// jj://commit/abc
/// tracker://task/PROJ-42
/// docs://rfc/0001
/// ```
/// belongs outside the kernel.
///
/// Internally we never need to parse strings.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct EntityAddress<M: Model> {
    pub namespace: Namespace,
    pub kind: EntityKind,
    pub id: M::ExternalId,
}

impl<M: Model> EntityAddress<M> {
    pub fn entity_type(&self) -> EntityType {
        EntityType {
            namespace: self.namespace.clone(),
            kind: self.kind.clone(),
        }
    }
}

// =============================================================================
// ENTITY TYPES
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntityType {
    pub namespace: Namespace,
    pub kind: EntityKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum InternalEntityKind {
    Operation,
    Event,
    Session,
    Actor,
    Object,
    Replica,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EntityTypePattern {
    Internal(InternalEntityKind),
    External(EntityType),
    Any,
}

// =============================================================================
// ENTITY REFERENCES
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum InternalEntityRef<M: Model> {
    Operation(OperationId<M>),
    Event(EventId<M>),
    Session(SessionId<M>),
    Actor(ActorId<M>),
    Object(ObjectId<M>),
    Replica(ReplicaId<M>),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum EntityRef<M: Model> {
    Internal(InternalEntityRef<M>),
    External(EntityAddress<M>),
}

impl<M: Model> EntityRef<M> {
    pub fn type_pattern(&self) -> EntityTypePattern {
        match self {
            Self::External(address) => EntityTypePattern::External(address.entity_type()),

            Self::Internal(internal) => {
                let kind = match internal {
                    InternalEntityRef::Operation(_) => InternalEntityKind::Operation,
                    InternalEntityRef::Event(_) => InternalEntityKind::Event,
                    InternalEntityRef::Session(_) => InternalEntityKind::Session,
                    InternalEntityRef::Actor(_) => InternalEntityKind::Actor,
                    InternalEntityRef::Object(_) => InternalEntityKind::Object,
                    InternalEntityRef::Replica(_) => InternalEntityKind::Replica,
                };
                EntityTypePattern::Internal(kind)
            }
        }
    }
}

// =============================================================================
// STRICT STRUCTURED VALUES
// =============================================================================

/// Kernel-owned value representation.
///
/// Plugin DTOs should translate into this before entering the kernel.
///
/// No arbitrary JSON blob required for typed SDLC data.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum Value<M: Model> {
    Null,
    Bool(bool),
    Integer(i128),
    Decimal(Box<str>),
    String(Box<str>),
    Bytes(Vec<u8>),
    Entity(EntityRef<M>),
    List(Vec<Value<M>>),
    Map(BTreeMap<FieldName, Value<M>>),
}

pub type Attributes<M> = BTreeMap<FieldName, Value<M>>;

// =============================================================================
// SCHEMAS
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SchemaKey {
    pub namespace: Namespace,
    pub version: SchemaVersion,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueType {
    Null,
    Bool,
    Integer,
    Decimal,
    String,
    Bytes,
    Entity(EntityTypePattern),
    List(Box<ValueType>),
    Map {
        fields: BTreeMap<FieldName, FieldSchema>,
        allow_unknown: bool,
    },
    Any,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldSchema {
    pub value_type: ValueType,
    pub required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntitySchema {
    pub entity_type: EntityType,
    pub fields: BTreeMap<FieldName, FieldSchema>,
    pub allow_unknown_fields: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RelationType {
    pub namespace: Namespace,
    pub name: RelationName,
}

// =============================================================================
// EXPLANATION SEMANTICS
// =============================================================================

/// Plugins can tell the kernel whether a relation participates in `why`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExplanationRole {
    /// A direct causal dependency.
    Primary,
    /// Supporting evidence/context.
    Supporting,
    /// Useful but non-essential contextual relation.
    Contextual,
}

/// Determines which endpoint explains the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExplanationDirection {
    /// `from` depends on / is explained by `to`.
    FromExplainedByTo,
    /// `to` depends on / is explained by `from`.
    ToExplainedByFrom,
    /// Either endpoint can explain the other.
    Symmetric,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplanationSemantics {
    pub role: ExplanationRole,
    pub direction: ExplanationDirection,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationSchema {
    pub relation_type: RelationType,
    pub from: EntityTypePattern,
    pub to: EntityTypePattern,
    /// None => not followed by generic explanation queries.
    pub explanation: Option<ExplanationSemantics>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub key: SchemaKey,
    /// Other schemas which must already be known.
    pub requires: BTreeSet<SchemaKey>,
    pub entities: BTreeMap<EntityKind, EntitySchema>,
    pub relations: BTreeMap<RelationName, RelationSchema>,
}

// =============================================================================
// NAMED DECLARATIVE QUERIES
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct QueryKey {
    pub namespace: Namespace,
    pub version: SchemaVersion,
    pub name: QueryName,
}

/// Named queries supplied by an ontology/plugin.
///
/// They still compile to generic kernel queries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedQueryDefinition {
    pub key: QueryKey,
    pub input: EntityTypePattern,
    pub template: QueryTemplate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryTemplate {
    History,
    Explain {
        max_depth: Option<usize>,
        roles: BTreeSet<ExplanationRole>,
    },
    Traverse {
        direction: Direction,
        relations: RelationSelector,
        max_depth: Option<usize>,
    },
}

// =============================================================================
// SOURCE OPERATION DAG
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct SourceOperation<M: Model> {
    /// ```text
    /// vcs://operation/...
    /// tracker://event/...
    /// orchestrator://run-transition/...
    /// ```
    pub id: EntityAddress<M>,
    pub parents: BTreeSet<EntityAddress<M>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct SourceAnchor<M: Model> {
    pub source: EntityAddress<M>,
    pub operation: OperationId<M>,
}

// =============================================================================
// EXTERNAL ENTITY OBSERVATIONS
// =============================================================================

/// Immutable observation of a typed external entity.
///
/// The entity itself may evolve over time.
///
/// Every observation remains historical provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct EntityObservation<M: Model> {
    pub entity: EntityAddress<M>,
    /// Exact schema version under which this observation was validated.
    pub schema: SchemaKey,
    pub attributes: Attributes<M>,
}

// =============================================================================
// RELATIONS
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Relation<M: Model> {
    /// Exact schema version defining this relation.
    pub schema: SchemaKey,
    pub relation_type: RelationType,
    pub from: EntityRef<M>,
    pub to: EntityRef<M>,
    pub attributes: Attributes<M>,
}

// =============================================================================
// CORE ENTITIES
// =============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Session<M: Model> {
    pub id: SessionId<M>,
    pub parent: Option<SessionId<M>>,
    pub attributes: Attributes<M>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Actor<M: Model> {
    pub id: ActorId<M>,
    pub session: Option<SessionId<M>>,
    pub attributes: Attributes<M>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Object<M: Model> {
    pub id: ObjectId<M>,
    pub payload: M::Payload,
    pub attributes: Attributes<M>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Replica<M: Model> {
    pub id: ReplicaId<M>,
    pub attributes: Attributes<M>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Event<M: Model> {
    pub id: EventId<M>,
    pub session: Option<SessionId<M>>,
    pub actor: Option<ActorId<M>>,
    pub parents: BTreeSet<EventId<M>>,
    pub subjects: BTreeSet<EntityRef<M>>,
    pub relations: BTreeSet<Relation<M>>,
    pub requires: BTreeSet<ResourceRequirement<M>>,
    pub attributes: Attributes<M>,
}

// =============================================================================
// RETENTION
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RetentionStrength {
    /// Identity/reference only.
    Referenced,
    /// External owner must guarantee the resource survives collection.
    Pinned,
    /// Provenance must own an independently recoverable representation.
    Escrowed,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Resource<M: Model> {
    pub entity: EntityRef<M>,
}

impl<M: Model> From<EntityRef<M>> for Resource<M> {
    fn from(entity: EntityRef<M>) -> Self {
        Self { entity }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct ResourceRequirement<M: Model> {
    pub resource: Resource<M>,
    pub strength: RetentionStrength,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum RetentionOwner<M: Model> {
    Event(EventId<M>),
    Session(SessionId<M>),
    Actor(ActorId<M>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct RetentionClaim<M: Model> {
    pub id: ClaimId<M>,
    pub owner: RetentionOwner<M>,
    pub resource: Resource<M>,
    pub strength: RetentionStrength,
}

/// Aggregate physical transition.
///
/// This is what runtimes hydrate.
///
/// They never need to reason about individual claim counting.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum RetentionTransition<M: Model> {
    BecameRequired {
        resource: Resource<M>,
        strength: RetentionStrength,
    },
    Strengthened {
        resource: Resource<M>,
        from: RetentionStrength,
        to: RetentionStrength,
    },
    Weakened {
        resource: Resource<M>,
        from: RetentionStrength,
        to: RetentionStrength,
    },
    BecameUnrequired {
        resource: Resource<M>,
        previous: RetentionStrength,
    },
}

// =============================================================================
// OBSERVED RESOURCE STATE
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum Availability<M: Model> {
    Unknown,
    Local,
    Remote(BTreeSet<ReplicaId<M>>),
    Missing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Integrity {
    Unknown,
    Unverified,
    Verified,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct ResourceObservation<M: Model> {
    pub availability: Availability<M>,
    pub integrity: Integrity,
    /// What the outside runtime reports it currently guarantees.
    pub observed_retention: Option<RetentionStrength>,
}

// =============================================================================
// AUTHORITATIVE FACTS
// =============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum Fact<M: Model> {
    SchemaRegistered(SchemaDefinition),
    NamedQueryRegistered(NamedQueryDefinition),
    SourceAnchored(SourceAnchor<M>),
    SessionOpened(Session<M>),
    SessionEnded {
        session: SessionId<M>,
    },
    ActorDeclared(Actor<M>),
    ObjectDeclared(Object<M>),
    EntityObserved(EntityObservation<M>),
    EventRecorded(Event<M>),
    ReplicaDeclared(Replica<M>),
    RetentionClaimed(RetentionClaim<M>),
    RetentionReleased {
        claim: ClaimId<M>,
    },
    ResourceObserved {
        resource: Resource<M>,
        observation: ResourceObservation<M>,
    },
}

// =============================================================================
// AUTHORITATIVE OPERATION
// =============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Operation<M: Model> {
    pub id: OperationId<M>,
    pub parents: BTreeSet<OperationId<M>>,
    pub source: Option<EntityAddress<M>>,
    pub facts: Vec<Fact<M>>,
    pub attributes: Attributes<M>,
}

// =============================================================================
// GRAPH EDGE PROJECTION
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct GraphEdge<M: Model> {
    pub relation: Relation<M>,
    pub event: EventId<M>,
    pub operation: OperationId<M>,
}

// =============================================================================
// PROJECTION
// =============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Projection<M: Model> {
    // -------------------------------------------------------------------------
    // ontology
    // -------------------------------------------------------------------------
    pub schemas: Ledger<SchemaKey, SchemaDefinition>,
    pub named_queries: Ledger<QueryKey, NamedQueryDefinition>,

    // -------------------------------------------------------------------------
    // source mapping
    // -------------------------------------------------------------------------
    pub source_to_operation: Index<EntityAddress<M>, OperationId<M>>,
    pub source_to_primary_event: Index<EntityAddress<M>, EventId<M>>,

    // -------------------------------------------------------------------------
    // internal entities
    // -------------------------------------------------------------------------
    pub sessions: Ledger<SessionId<M>, Session<M>>,
    pub ended_sessions: Set<SessionId<M>>,
    pub actors: Ledger<ActorId<M>, Actor<M>>,
    pub objects: Ledger<ObjectId<M>, Object<M>>,
    pub events: Ledger<EventId<M>, Event<M>>,
    pub event_heads: Set<EventId<M>>,
    pub replicas: Ledger<ReplicaId<M>, Replica<M>>,

    // -------------------------------------------------------------------------
    // heterogeneous SDLC graph
    // -------------------------------------------------------------------------
    /// Latest external entity state.
    pub entities: Index<EntityAddress<M>, EntityObservation<M>>,

    /// Complete operation history for each external entity.
    pub entity_history: MultiIndex<EntityAddress<M>, OperationId<M>>,
    pub outgoing: MultiIndex<EntityRef<M>, GraphEdge<M>>,
    pub incoming: MultiIndex<EntityRef<M>, GraphEdge<M>>,

    // -------------------------------------------------------------------------
    // retention
    // -------------------------------------------------------------------------
    pub active_retention: Index<ClaimId<M>, RetentionClaim<M>>,
    /// Purely derived from active claims.
    pub required_retention: Index<Resource<M>, RetentionStrength>,
    /// Observed from runtime/backends.
    pub resource_observations: Index<Resource<M>, ResourceObservation<M>>,
}

impl<M: Model> Default for Projection<M> {
    fn default() -> Self {
        Self {
            schemas: Ledger::default(),
            named_queries: Ledger::default(),
            source_to_operation: Index::default(),
            source_to_primary_event: Index::default(),
            sessions: Ledger::default(),
            ended_sessions: Set::default(),
            actors: Ledger::default(),
            objects: Ledger::default(),
            events: Ledger::default(),
            event_heads: Set::default(),
            replicas: Ledger::default(),
            entities: Index::default(),
            entity_history: MultiIndex::default(),
            outgoing: MultiIndex::default(),
            incoming: MultiIndex::default(),
            active_retention: Index::default(),
            required_retention: Index::default(),
            resource_observations: Index::default(),
        }
    }
}

// =============================================================================
// PROJECTION ACCESSORS
// =============================================================================

impl<M: Model> Projection<M> {
    pub fn schema(&self, key: &SchemaKey) -> Option<&SchemaDefinition> {
        self.schemas.get(key)
    }

    pub fn require_schema(&self, key: &SchemaKey) -> Result<&SchemaDefinition> {
        self.schema(key).ok_or(Error::MissingSchema)
    }

    pub fn session(&self, id: &SessionId<M>) -> Option<&Session<M>> {
        self.sessions.get(id)
    }

    pub fn require_session(&self, id: &SessionId<M>) -> Result<&Session<M>> {
        self.sessions.get(id).ok_or(Error::MissingSession)
    }

    pub fn session_active(&self, id: &SessionId<M>) -> bool {
        self.sessions.contains(id) && !self.ended_sessions.contains(id)
    }

    pub fn actor(&self, id: &ActorId<M>) -> Option<&Actor<M>> {
        self.actors.get(id)
    }

    pub fn require_actor(&self, id: &ActorId<M>) -> Result<&Actor<M>> {
        self.actors.get(id).ok_or(Error::MissingActor)
    }

    pub fn event(&self, id: &EventId<M>) -> Option<&Event<M>> {
        self.events.get(id)
    }

    pub fn require_event(&self, id: &EventId<M>) -> Result<&Event<M>> {
        self.events.get(id).ok_or(Error::MissingEvent)
    }

    pub fn entity(&self, id: &EntityAddress<M>) -> Option<&EntityObservation<M>> {
        self.entities.get(id)
    }

    pub fn effective_retention(&self, resource: &Resource<M>) -> Option<RetentionStrength> {
        self.required_retention.get(resource).copied()
    }

    pub fn observed_retention(&self, resource: &Resource<M>) -> Option<RetentionStrength> {
        self.resource_observations
            .get(resource)
            .and_then(|observation| observation.observed_retention)
    }

    pub fn retention_satisfied(&self, resource: &Resource<M>) -> bool {
        match (
            self.effective_retention(resource),
            self.observed_retention(resource),
        ) {
            (None, _) => true,

            (Some(required), Some(observed)) => observed >= required,

            (Some(_), None) => false,
        }
    }

    fn recompute_retention_for(&mut self, resource: &Resource<M>) -> Option<RetentionStrength> {
        let strength = self
            .active_retention
            .iter()
            .filter(|(_, claim)| &claim.resource == resource)
            .map(|(_, claim)| claim.strength)
            .max();

        match strength {
            Some(strength) => {
                self.required_retention.set(resource.clone(), strength);
            }

            None => {
                self.required_retention.remove(resource);
            }
        }

        strength
    }
}

// =============================================================================
// STATE
// =============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct State<M: Model> {
    /// AUTHORITATIVE.
    pub operations: Ledger<OperationId<M>, Operation<M>>,

    /// DERIVED.
    pub operation_heads: Set<OperationId<M>>,

    /// DERIVED.
    pub projection: Projection<M>,
}

impl<M: Model> Default for State<M> {
    fn default() -> Self {
        Self {
            operations: Ledger::default(),

            operation_heads: Set::default(),

            projection: Projection::default(),
        }
    }
}

impl<M: Model> State<M> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn operation(&self, id: &OperationId<M>) -> Option<&Operation<M>> {
        self.operations.get(id)
    }

    pub fn operation_for_source(&self, source: &EntityAddress<M>) -> Option<&OperationId<M>> {
        self.projection.source_to_operation.get(source)
    }

    pub fn has_source(&self, source: &EntityAddress<M>) -> bool {
        self.operation_for_source(source).is_some()
    }

    pub fn rebuild(&mut self) -> Result<()> {
        let mut projection = Projection::default();
        let ordered = topological_operations(&self.operations)?;

        for operation in ordered {
            apply_operation(&mut projection, operation)?;
        }

        let operation_heads = compute_operation_heads(&self.operations);
        self.projection = projection;
        self.operation_heads = operation_heads;

        Ok(())
    }
}

// =============================================================================
// TRANSACTION INTENTS
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct EventIntent<M: Model> {
    pub session: Option<SessionId<M>>,

    pub actor: Option<ActorId<M>>,

    /// Source causal parents are inherited automatically.
    pub additional_parents: BTreeSet<EventId<M>>,

    pub subjects: BTreeSet<EntityRef<M>>,

    pub relations: BTreeSet<Relation<M>>,

    pub requires: BTreeSet<ResourceRequirement<M>>,

    pub attributes: Attributes<M>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum Intent<M: Model> {
    RegisterSchema(SchemaDefinition),

    RegisterNamedQuery(NamedQueryDefinition),

    OpenSession {
        seed: M::Seed,

        parent: Option<SessionId<M>>,

        attributes: Attributes<M>,
    },

    EndSession {
        session: SessionId<M>,
    },

    DeclareActor {
        seed: M::Seed,

        session: Option<SessionId<M>>,

        attributes: Attributes<M>,
    },

    PutObject {
        seed: M::Seed,

        payload: M::Payload,

        attributes: Attributes<M>,
    },

    ObserveEntity(EntityObservation<M>),

    RecordEvent(EventIntent<M>),

    ReleaseRetention {
        claim: ClaimId<M>,
    },

    DeclareReplica {
        seed: M::Seed,

        attributes: Attributes<M>,
    },

    ObserveResource {
        resource: Resource<M>,

        observation: ResourceObservation<M>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Transaction<M: Model> {
    pub seed: M::Seed,

    pub source: Option<SourceOperation<M>>,

    pub intents: Vec<Intent<M>>,

    pub attributes: Attributes<M>,
}

// =============================================================================
// POLICY EXTENSION
// =============================================================================

pub trait Rules<M: Model>: Send + Sync {
    fn validate_schema(&self, _state: &State<M>, _schema: &SchemaDefinition) -> Result<()> {
        Ok(())
    }

    fn validate_entity_observation(
        &self,
        _state: &State<M>,
        _observation: &EntityObservation<M>,
    ) -> Result<()> {
        Ok(())
    }

    fn validate_event(&self, _state: &State<M>, _event: &Event<M>) -> Result<()> {
        Ok(())
    }

    fn validate_operation(&self, _state: &State<M>, _operation: &Operation<M>) -> Result<()> {
        Ok(())
    }

    fn require_source_parents(&self) -> bool {
        true
    }

    fn retain_event_requirements(&self) -> bool {
        true
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct DefaultRules;

impl<M: Model> Rules<M> for DefaultRules {}

// =============================================================================
// COMMIT PROTOCOL
// =============================================================================

/// Must become true BEFORE operation publication.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum PrepareRequirement<M: Model> {
    MaterializeObject(Object<M>),

    Retention(RetentionTransition<M>),
}

/// Safe to perform AFTER authoritative publication.
///
/// A crash here does not invalidate provenance.
/// Runtime can reconcile later.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum FinalizeAction<M: Model> {
    Retention(RetentionTransition<M>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct CommitPlan<M: Model> {
    pub prepare: Vec<PrepareRequirement<M>>,

    pub operation: Operation<M>,

    pub next_state: State<M>,

    pub finalize: Vec<FinalizeAction<M>>,

    pub idempotent: bool,
}

// =============================================================================
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

pub type Result<T> = std::result::Result<T, Error>;

// =============================================================================
// SCHEMA VALIDATION
// =============================================================================

fn matches_entity_type<M: Model>(entity: &EntityRef<M>, expected: &EntityTypePattern) -> bool {
    match expected {
        EntityTypePattern::Any => true,

        EntityTypePattern::External(expected) => {
            matches!(
                entity,
                EntityRef::External(address)
                    if &address.entity_type() == expected
            )
        }

        EntityTypePattern::Internal(expected) => {
            matches!(
                entity.type_pattern(),
                EntityTypePattern::Internal(actual)
                    if &actual == expected
            )
        }
    }
}

fn value_matches<M: Model>(value: &Value<M>, expected: &ValueType) -> bool {
    match (value, expected) {
        (_, ValueType::Any) => true,

        (Value::Null, ValueType::Null) => true,

        (Value::Bool(_), ValueType::Bool) => true,

        (Value::Integer(_), ValueType::Integer) => true,

        (Value::Decimal(_), ValueType::Decimal) => true,

        (Value::String(_), ValueType::String) => true,

        (Value::Bytes(_), ValueType::Bytes) => true,

        (Value::Entity(entity), ValueType::Entity(expected)) => {
            matches_entity_type(entity, expected)
        }

        (Value::List(values), ValueType::List(inner)) => {
            values.iter().all(|value| value_matches(value, inner))
        }

        (
            Value::Map(values),
            ValueType::Map {
                fields,
                allow_unknown,
            },
        ) => validate_attribute_map(values, fields, *allow_unknown).is_ok(),

        _ => false,
    }
}

fn validate_attribute_map<M: Model>(
    values: &Attributes<M>,
    fields: &BTreeMap<FieldName, FieldSchema>,
    allow_unknown: bool,
) -> Result<()> {
    for (field, schema) in fields {
        if schema.required && !values.contains_key(field) {
            return Err(Error::MissingRequiredField);
        }
    }

    for (field, value) in values {
        let Some(schema) = fields.get(field) else {
            if allow_unknown {
                continue;
            }

            return Err(Error::UnknownField);
        };

        if !value_matches(value, &schema.value_type) {
            return Err(Error::InvalidFieldType);
        }
    }

    Ok(())
}

fn find_entity_schema<'a, M: Model>(
    projection: &'a Projection<M>,
    schema_key: &SchemaKey,
    entity_type: &EntityType,
) -> Result<&'a EntitySchema> {
    let schema = projection.require_schema(schema_key)?;

    schema
        .entities
        .get(&entity_type.kind)
        .filter(|definition| &definition.entity_type == entity_type)
        .ok_or(Error::MissingEntitySchema)
}

fn find_relation_schema<'a, M: Model>(
    projection: &'a Projection<M>,
    relation: &Relation<M>,
) -> Result<&'a RelationSchema> {
    let schema = projection.require_schema(&relation.schema)?;

    schema
        .relations
        .get(&relation.relation_type.name)
        .filter(|definition| definition.relation_type == relation.relation_type)
        .ok_or(Error::MissingRelationSchema)
}

fn validate_entity_observation_schema<M: Model>(
    projection: &Projection<M>,
    observation: &EntityObservation<M>,
) -> Result<()> {
    let entity_type = observation.entity.entity_type();

    let schema = find_entity_schema(projection, &observation.schema, &entity_type)?;

    validate_attribute_map(
        &observation.attributes,
        &schema.fields,
        schema.allow_unknown_fields,
    )
}

fn validate_relation<M: Model>(projection: &Projection<M>, relation: &Relation<M>) -> Result<()> {
    if relation.from == relation.to {
        return Err(Error::InvalidSelfRelation);
    }

    let schema = find_relation_schema(projection, relation)?;

    if !matches_entity_type(&relation.from, &schema.from) {
        return Err(Error::InvalidRelationEndpoint);
    }

    if !matches_entity_type(&relation.to, &schema.to) {
        return Err(Error::InvalidRelationEndpoint);
    }

    Ok(())
}

// =============================================================================
// KERNEL
// =============================================================================

pub struct Kernel<M, I, R = DefaultRules>
where
    M: Model,
    I: IdentityScheme<M>,
    R: Rules<M>,
{
    ids: I,
    rules: R,
    _model: PhantomData<fn() -> M>,
}
struct SourceResolution<M: Model>(
    Option<EntityAddress<M>>,
    BTreeSet<OperationId<M>>,
    BTreeSet<EventId<M>>,
);

impl<M, I> Kernel<M, I, DefaultRules>
where
    M: Model,
    I: IdentityScheme<M>,
{
    pub fn new(ids: I) -> Self {
        Self {
            ids,
            rules: DefaultRules,
            _model: PhantomData,
        }
    }
}

impl<M, I, R> Kernel<M, I, R>
where
    M: Model,
    I: IdentityScheme<M>,
    R: Rules<M>,
{
    pub fn with_rules(ids: I, rules: R) -> Self {
        Self {
            ids,
            rules,
            _model: PhantomData,
        }
    }

    // =========================================================================
    // TRANSACTION
    // =========================================================================

    pub fn transact(&self, state: &State<M>, tx: Transaction<M>) -> Result<CommitPlan<M>> {
        let operation_id = self.ids.operation(&tx.seed);

        let SourceResolution(source, operation_parents, inherited_event_parents) =
            self.resolve_source(state, &tx, &operation_id)?;

        // ---------------------------------------------------------------------
        // Source replay fast path.
        // ---------------------------------------------------------------------

        if let Some(source) = &source
            && let Some(existing_id) = state.projection.source_to_operation.get(source)
        {
            let existing = state
                .operations
                .get(existing_id)
                .ok_or(Error::MissingOperationParent)?;

            if existing.id == operation_id {
                return Ok(CommitPlan {
                    prepare: Vec::new(),
                    operation: existing.clone(),
                    next_state: state.clone(),
                    finalize: Vec::new(),
                    idempotent: true,
                });
            }

            return Err(Error::SourceAlreadyMappedDifferently);
        }

        let mut projection = state.projection.clone();

        let mut facts = Vec::new();

        let mut prepare = Vec::new();

        let mut finalize = Vec::new();

        if let Some(source) = source.clone() {
            let fact = Fact::SourceAnchored(SourceAnchor {
                source,
                operation: operation_id.clone(),
            });

            apply_fact(&mut projection, &operation_id, &fact)?;

            facts.push(fact);
        }

        let mut event_index = 0usize;

        let mut object_index = 0usize;

        let mut primary_event = None;

        for intent in tx.intents {
            match intent {
                // =============================================================
                // SCHEMA
                // =============================================================
                Intent::RegisterSchema(schema) => {
                    for dependency in &schema.requires {
                        if !projection.schemas.contains(dependency) {
                            return Err(Error::MissingSchemaDependency);
                        }
                    }

                    self.rules.validate_schema(state, &schema)?;

                    match projection
                        .schemas
                        .declare(schema.key.clone(), schema.clone())?
                    {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    facts.push(Fact::SchemaRegistered(schema));
                }

                // =============================================================
                // NAMED QUERY
                // =============================================================
                Intent::RegisterNamedQuery(query) => {
                    // Namespace/schema ownership enforcement can be stricter
                    // in plugin host policy.

                    match projection
                        .named_queries
                        .declare(query.key.clone(), query.clone())?
                    {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    facts.push(Fact::NamedQueryRegistered(query));
                }

                // =============================================================
                // SESSION
                // =============================================================
                Intent::OpenSession {
                    seed,
                    parent,
                    attributes,
                } => {
                    if let Some(parent) = &parent {
                        projection.require_session(parent)?;
                    }

                    let session = Session {
                        id: self.ids.session(&seed),

                        parent,

                        attributes,
                    };

                    match projection
                        .sessions
                        .declare(session.id.clone(), session.clone())?
                    {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    facts.push(Fact::SessionOpened(session));
                }

                Intent::EndSession { session } => {
                    projection.require_session(&session)?;

                    if projection.ended_sessions.contains(&session) {
                        continue;
                    }

                    projection.ended_sessions.insert(session.clone());

                    facts.push(Fact::SessionEnded { session });
                }

                // =============================================================
                // ACTOR
                // =============================================================
                Intent::DeclareActor {
                    seed,
                    session,
                    attributes,
                } => {
                    if let Some(session) = &session {
                        projection.require_session(session)?;

                        if !projection.session_active(session) {
                            return Err(Error::SessionAlreadyEnded);
                        }
                    }

                    let actor = Actor {
                        id: self.ids.actor(&seed),

                        session,

                        attributes,
                    };

                    match projection.actors.declare(actor.id.clone(), actor.clone())? {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    facts.push(Fact::ActorDeclared(actor));
                }

                // =============================================================
                // OBJECT
                // =============================================================
                Intent::PutObject {
                    seed,
                    payload,
                    attributes,
                } => {
                    let object = Object {
                        id: self.ids.object(&seed, object_index),

                        payload,

                        attributes,
                    };

                    object_index += 1;

                    match projection
                        .objects
                        .declare(object.id.clone(), object.clone())?
                    {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    prepare.push(PrepareRequirement::MaterializeObject(object.clone()));

                    facts.push(Fact::ObjectDeclared(object));
                }

                // =============================================================
                // EXTERNAL TYPED ENTITY
                // =============================================================
                Intent::ObserveEntity(observation) => {
                    validate_entity_observation_schema(&projection, &observation)?;

                    self.rules
                        .validate_entity_observation(state, &observation)?;

                    projection
                        .entities
                        .set(observation.entity.clone(), observation.clone());

                    projection
                        .entity_history
                        .insert(observation.entity.clone(), operation_id.clone());

                    facts.push(Fact::EntityObserved(observation));
                }

                // =============================================================
                // EVENT
                // =============================================================
                Intent::RecordEvent(intent) => {
                    let event_id = self.ids.event(&tx.seed, event_index);

                    let mut parents = inherited_event_parents.clone();

                    parents.extend(intent.additional_parents.iter().cloned());

                    for parent in &parents {
                        projection.require_event(parent)?;
                    }

                    if let Some(session) = &intent.session {
                        projection.require_session(session)?;

                        if !projection.session_active(session) {
                            return Err(Error::SessionAlreadyEnded);
                        }
                    }

                    if let Some(actor_id) = &intent.actor {
                        let actor = projection.require_actor(actor_id)?;

                        if let (Some(event_session), Some(actor_session)) =
                            (&intent.session, &actor.session)
                            && event_session != actor_session
                        {
                            return Err(Error::ActorSessionMismatch);
                        }
                    }

                    for relation in &intent.relations {
                        validate_relation(&projection, relation)?;
                    }

                    let event = Event {
                        id: event_id.clone(),

                        session: intent.session,

                        actor: intent.actor,

                        parents,

                        subjects: intent.subjects,

                        relations: intent.relations,

                        requires: intent.requires,

                        attributes: intent.attributes,
                    };

                    self.rules.validate_event(state, &event)?;

                    match projection.events.declare(event.id.clone(), event.clone())? {
                        DeclareOutcome::AlreadyPresent => {
                            event_index += 1;
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    for parent in &event.parents {
                        projection.event_heads.remove(parent);
                    }

                    projection.event_heads.insert(event.id.clone());

                    for relation in &event.relations {
                        let edge = GraphEdge {
                            relation: relation.clone(),

                            event: event.id.clone(),

                            operation: operation_id.clone(),
                        };

                        projection
                            .outgoing
                            .insert(relation.from.clone(), edge.clone());

                        projection.incoming.insert(relation.to.clone(), edge);
                    }

                    facts.push(Fact::EventRecorded(event.clone()));

                    if primary_event.is_none() {
                        primary_event = Some(event.id.clone());
                    }

                    // ---------------------------------------------------------
                    // AUTOMATIC AGGREGATE RETENTION
                    // ---------------------------------------------------------

                    if self.rules.retain_event_requirements() {
                        for (requirement_index, requirement) in
                            event.requires.iter().cloned().enumerate()
                        {
                            let before = projection.effective_retention(&requirement.resource);

                            let claim = RetentionClaim {
                                id: self.ids.claim(&tx.seed, event_index, requirement_index),

                                owner: RetentionOwner::Event(event.id.clone()),

                                resource: requirement.resource.clone(),

                                strength: requirement.strength,
                            };

                            if let Some(existing) = projection.active_retention.get(&claim.id) {
                                if existing != &claim {
                                    return Err(Error::IdentityCollision);
                                }

                                continue;
                            }

                            projection
                                .active_retention
                                .set(claim.id.clone(), claim.clone());

                            let after = projection.recompute_retention_for(&claim.resource);

                            if let Some(transition) =
                                retention_transition(claim.resource.clone(), before, after)
                            {
                                if retention_is_prepare(&transition) {
                                    prepare.push(PrepareRequirement::Retention(transition));
                                } else {
                                    finalize.push(FinalizeAction::Retention(transition));
                                }
                            }

                            facts.push(Fact::RetentionClaimed(claim));
                        }
                    }

                    event_index += 1;
                }

                // =============================================================
                // RETENTION RELEASE
                // =============================================================
                Intent::ReleaseRetention { claim } => {
                    let existing = projection
                        .active_retention
                        .get(&claim)
                        .cloned()
                        .ok_or(Error::UnknownRetentionClaim)?;

                    let before = projection.effective_retention(&existing.resource);

                    projection.active_retention.remove(&claim);

                    let after = projection.recompute_retention_for(&existing.resource);

                    if let Some(transition) = retention_transition(existing.resource, before, after)
                    {
                        if retention_is_prepare(&transition) {
                            prepare.push(PrepareRequirement::Retention(transition));
                        } else {
                            finalize.push(FinalizeAction::Retention(transition));
                        }
                    }

                    facts.push(Fact::RetentionReleased { claim });
                }

                // =============================================================
                // REPLICA
                // =============================================================
                Intent::DeclareReplica { seed, attributes } => {
                    let replica = Replica {
                        id: self.ids.replica(&seed),

                        attributes,
                    };

                    match projection
                        .replicas
                        .declare(replica.id.clone(), replica.clone())?
                    {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    facts.push(Fact::ReplicaDeclared(replica));
                }

                // =============================================================
                // RESOURCE OBSERVATION
                // =============================================================
                Intent::ObserveResource {
                    resource,
                    observation,
                } => {
                    if projection.resource_observations.get(&resource) == Some(&observation) {
                        continue;
                    }

                    projection
                        .resource_observations
                        .set(resource.clone(), observation.clone());

                    facts.push(Fact::ResourceObserved {
                        resource,
                        observation,
                    });
                }
            }
        }

        if let (Some(source), Some(event)) = (source.clone(), primary_event) {
            projection.source_to_primary_event.set(source, event);
        }

        let operation = Operation {
            id: operation_id.clone(),

            parents: operation_parents,

            source,

            facts,

            attributes: tx.attributes,
        };

        self.rules.validate_operation(state, &operation)?;

        if let Some(existing) = state.operations.get(&operation.id) {
            if existing == &operation {
                return Ok(CommitPlan {
                    prepare: Vec::new(),

                    operation: existing.clone(),

                    next_state: state.clone(),

                    finalize: Vec::new(),

                    idempotent: true,
                });
            }

            return Err(Error::IdentityCollision);
        }

        let mut next = state.clone();

        for parent in &operation.parents {
            next.operation_heads.remove(parent);
        }

        next.operation_heads.insert(operation.id.clone());

        next.operations
            .declare(operation.id.clone(), operation.clone())?;

        next.projection = projection;

        Ok(CommitPlan {
            prepare,
            operation,
            next_state: next,
            finalize,
            idempotent: false,
        })
    }

    // =========================================================================
    // SOURCE CAUSALITY
    // =========================================================================

    fn resolve_source(
        &self,
        state: &State<M>,
        tx: &Transaction<M>,
        operation_id: &OperationId<M>,
    ) -> Result<SourceResolution<M>> {
        let Some(source) = &tx.source else {
            return Ok(SourceResolution(
                None,
                state.operation_heads.cloned(),
                state.projection.event_heads.cloned(),
            ));
        };

        if let Some(existing) = state.projection.source_to_operation.get(&source.id)
            && existing != operation_id
        {
            return Err(Error::SourceAlreadyMappedDifferently);
        }

        let mut operation_parents = BTreeSet::new();

        let mut event_parents = BTreeSet::new();

        for source_parent in &source.parents {
            match state.projection.source_to_operation.get(source_parent) {
                Some(parent) => {
                    operation_parents.insert(parent.clone());
                }

                None if self.rules.require_source_parents() => {
                    return Err(Error::MissingSourceParent);
                }

                None => {}
            }

            if let Some(event) = state.projection.source_to_primary_event.get(source_parent) {
                event_parents.insert(event.clone());
            }
        }

        Ok(SourceResolution(
            Some(source.id.clone()),
            operation_parents,
            event_parents,
        ))
    }

    // =========================================================================
    // DISTRIBUTED MERGE
    // =========================================================================

    pub fn merge(&self, left: &State<M>, right: &State<M>) -> Result<State<M>> {
        let mut merged = left.clone();

        for (id, operation) in right.operations.iter() {
            merged.operations.declare(id.clone(), operation.clone())?;
        }

        merged.rebuild()?;

        Ok(merged)
    }

    // =========================================================================
    // QUERY VALIDATION / NAMED QUERY EXPANSION
    // =========================================================================

    pub fn instantiate_named_query(
        &self,
        state: &State<M>,
        key: &QueryKey,
        input: EntityRef<M>,
    ) -> Result<Query<M>> {
        let definition = state
            .projection
            .named_queries
            .get(key)
            .ok_or(Error::InvalidNamedQuery)?;

        if !matches_entity_type(&input, &definition.input) {
            return Err(Error::InvalidNamedQuery);
        }

        Ok(match &definition.template {
            QueryTemplate::History => Query::History { entity: input },

            QueryTemplate::Explain { max_depth, roles } => Query::Explain {
                root: input,

                max_depth: *max_depth,

                roles: roles.clone(),
            },

            QueryTemplate::Traverse {
                direction,
                relations,
                max_depth,
            } => Query::Traverse {
                roots: BTreeSet::from([input]),

                direction: *direction,

                relations: relations.clone(),

                max_depth: *max_depth,

                predicate: Predicate::Any,
            },
        })
    }
}

// =============================================================================
// RETENTION AGGREGATION
// =============================================================================

fn retention_transition<M: Model>(
    resource: Resource<M>,
    before: Option<RetentionStrength>,
    after: Option<RetentionStrength>,
) -> Option<RetentionTransition<M>> {
    match (before, after) {
        (None, None) => None,

        (None, Some(strength)) => Some(RetentionTransition::BecameRequired { resource, strength }),

        (Some(previous), None) => {
            Some(RetentionTransition::BecameUnrequired { resource, previous })
        }

        (Some(from), Some(to)) if to > from => {
            Some(RetentionTransition::Strengthened { resource, from, to })
        }

        (Some(from), Some(to)) if to < from => {
            Some(RetentionTransition::Weakened { resource, from, to })
        }

        _ => None,
    }
}

fn retention_is_prepare<M: Model>(transition: &RetentionTransition<M>) -> bool {
    matches!(
        transition,
        RetentionTransition::BecameRequired { .. } | RetentionTransition::Strengthened { .. }
    )
}

// =============================================================================
// APPLY AUTHORITATIVE OPERATIONS
// =============================================================================

fn apply_operation<M: Model>(
    projection: &mut Projection<M>,

    operation: &Operation<M>,
) -> Result<()> {
    for fact in &operation.facts {
        apply_fact(projection, &operation.id, fact)?;
    }

    if let Some(source) = &operation.source {
        match projection.source_to_operation.get(source) {
            None => {
                projection
                    .source_to_operation
                    .set(source.clone(), operation.id.clone());
            }

            Some(existing) if existing == &operation.id => {}

            Some(_) => {
                return Err(Error::SourceAlreadyMappedDifferently);
            }
        }

        if let Some(event) = operation.facts.iter().find_map(|fact| match fact {
            Fact::EventRecorded(event) => Some(event.id.clone()),

            _ => None,
        }) {
            projection
                .source_to_primary_event
                .set(source.clone(), event);
        }
    }

    Ok(())
}

fn apply_fact<M: Model>(
    projection: &mut Projection<M>,

    operation_id: &OperationId<M>,

    fact: &Fact<M>,
) -> Result<()> {
    match fact {
        Fact::SchemaRegistered(schema) => {
            projection
                .schemas
                .declare(schema.key.clone(), schema.clone())?;
        }

        Fact::NamedQueryRegistered(query) => {
            projection
                .named_queries
                .declare(query.key.clone(), query.clone())?;
        }

        Fact::SourceAnchored(anchor) => match projection.source_to_operation.get(&anchor.source) {
            None => {
                projection
                    .source_to_operation
                    .set(anchor.source.clone(), anchor.operation.clone());
            }

            Some(existing) if existing == &anchor.operation => {}

            Some(_) => {
                return Err(Error::SourceAlreadyMappedDifferently);
            }
        },

        Fact::SessionOpened(session) => {
            projection
                .sessions
                .declare(session.id.clone(), session.clone())?;
        }

        Fact::SessionEnded { session } => {
            projection.require_session(session)?;

            projection.ended_sessions.insert(session.clone());
        }

        Fact::ActorDeclared(actor) => {
            projection.actors.declare(actor.id.clone(), actor.clone())?;
        }

        Fact::ObjectDeclared(object) => {
            projection
                .objects
                .declare(object.id.clone(), object.clone())?;
        }

        Fact::EntityObserved(observation) => {
            validate_entity_observation_schema(projection, observation)?;

            projection
                .entities
                .set(observation.entity.clone(), observation.clone());

            projection
                .entity_history
                .insert(observation.entity.clone(), operation_id.clone());
        }

        Fact::EventRecorded(event) => {
            match projection.events.declare(event.id.clone(), event.clone())? {
                DeclareOutcome::Inserted => {
                    for parent in &event.parents {
                        projection.event_heads.remove(parent);
                    }

                    projection.event_heads.insert(event.id.clone());

                    for relation in &event.relations {
                        validate_relation(projection, relation)?;

                        let edge = GraphEdge {
                            relation: relation.clone(),

                            event: event.id.clone(),

                            operation: operation_id.clone(),
                        };

                        projection
                            .outgoing
                            .insert(relation.from.clone(), edge.clone());

                        projection.incoming.insert(relation.to.clone(), edge);
                    }
                }

                DeclareOutcome::AlreadyPresent => {}
            }
        }

        Fact::ReplicaDeclared(replica) => {
            projection
                .replicas
                .declare(replica.id.clone(), replica.clone())?;
        }

        Fact::RetentionClaimed(claim) => {
            match projection.active_retention.get(&claim.id) {
                None => {
                    projection
                        .active_retention
                        .set(claim.id.clone(), claim.clone());
                }

                Some(existing) if existing == claim => {}

                Some(_) => {
                    return Err(Error::IdentityCollision);
                }
            }

            projection.recompute_retention_for(&claim.resource);
        }

        Fact::RetentionReleased { claim } => {
            if let Some(existing) = projection.active_retention.remove(claim) {
                projection.recompute_retention_for(&existing.resource);
            }
        }

        Fact::ResourceObserved {
            resource,
            observation,
        } => {
            projection
                .resource_observations
                .set(resource.clone(), observation.clone());
        }
    }

    Ok(())
}

// =============================================================================
// OPERATION DAG
// =============================================================================

fn topological_operations<M: Model>(
    operations: &Ledger<OperationId<M>, Operation<M>>,
) -> Result<Vec<&Operation<M>>> {
    let mut indegree = BTreeMap::<OperationId<M>, usize>::new();

    let mut children = BTreeMap::<OperationId<M>, BTreeSet<OperationId<M>>>::new();

    for id in operations.keys() {
        indegree.insert(id.clone(), 0);
    }

    for operation in operations.values() {
        for parent in &operation.parents {
            if !operations.contains(parent) {
                return Err(Error::MissingOperationParent);
            }

            *indegree.get_mut(&operation.id).unwrap() += 1;

            children
                .entry(parent.clone())
                .or_default()
                .insert(operation.id.clone());
        }
    }

    let mut queue = VecDeque::new();

    for (id, degree) in &indegree {
        if *degree == 0 {
            queue.push_back(id.clone());
        }
    }

    let mut ordered = Vec::new();

    while let Some(id) = queue.pop_front() {
        ordered.push(id.clone());

        if let Some(children) = children.get(&id) {
            for child in children {
                let degree = indegree.get_mut(child).unwrap();

                *degree -= 1;

                if *degree == 0 {
                    queue.push_back(child.clone());
                }
            }
        }
    }

    if ordered.len() != operations.len() {
        return Err(Error::OperationCycle);
    }

    Ok(ordered
        .iter()
        .map(|id| operations.get(id).unwrap())
        .collect())
}

fn compute_operation_heads<M: Model>(
    operations: &Ledger<OperationId<M>, Operation<M>>,
) -> Set<OperationId<M>> {
    let mut heads = Set::default();

    for id in operations.keys() {
        heads.insert(id.clone());
    }

    for operation in operations.values() {
        for parent in &operation.parents {
            heads.remove(parent);
        }
    }

    heads
}

// =============================================================================
// QUERY LANGUAGE
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Incoming,
    Outgoing,
    Both,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationSelector {
    Any,

    Types(BTreeSet<RelationType>),

    /// Follow only relations whose schemas declare explanation semantics.
    Explanatory,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum Predicate<M: Model> {
    Any,

    EntityType(EntityTypePattern),

    PropertyEquals { field: FieldName, value: Value<M> },

    And(Vec<Predicate<M>>),

    Or(Vec<Predicate<M>>),

    Not(Box<Predicate<M>>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum Query<M: Model> {
    Lookup {
        entity: EntityRef<M>,
    },

    History {
        entity: EntityRef<M>,
    },

    Traverse {
        roots: BTreeSet<EntityRef<M>>,

        direction: Direction,

        relations: RelationSelector,

        max_depth: Option<usize>,

        predicate: Predicate<M>,
    },

    /// Generic `why`.
    ///
    /// Its behavior is driven by schema-declared ExplanationSemantics.
    Explain {
        root: EntityRef<M>,

        max_depth: Option<usize>,

        roles: BTreeSet<ExplanationRole>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct QueryResult<M: Model> {
    pub entities: BTreeSet<EntityRef<M>>,

    pub edges: BTreeSet<GraphEdge<M>>,

    pub events: BTreeSet<EventId<M>>,

    pub operations: BTreeSet<OperationId<M>>,
}

impl<M: Model> Default for QueryResult<M> {
    fn default() -> Self {
        Self {
            entities: BTreeSet::new(),

            edges: BTreeSet::new(),

            events: BTreeSet::new(),

            operations: BTreeSet::new(),
        }
    }
}

// =============================================================================
// PURE QUERY ENGINE
// =============================================================================

impl<M: Model> State<M> {
    pub fn query(&self, query: &Query<M>) -> Result<QueryResult<M>> {
        match query {
            Query::Lookup { entity } => {
                let mut result = QueryResult::default();

                result.entities.insert(entity.clone());

                Ok(result)
            }

            Query::History { entity } => self.query_history(entity),

            Query::Traverse {
                roots,
                direction,
                relations,
                max_depth,
                predicate,
            } => self.query_traverse(roots, *direction, relations, *max_depth, predicate),

            Query::Explain {
                root,
                max_depth,
                roles,
            } => self.query_explain(root, *max_depth, roles),
        }
    }

    fn query_history(&self, entity: &EntityRef<M>) -> Result<QueryResult<M>> {
        let mut result = QueryResult::default();

        result.entities.insert(entity.clone());

        if let EntityRef::External(address) = entity
            && let Some(operations) = self.projection.entity_history.get(address)
        {
            result.operations.extend(operations.iter().cloned());
        }

        for event in self.projection.events.values() {
            let touches_subject = event.subjects.contains(entity);

            let touches_relation = event
                .relations
                .iter()
                .any(|relation| &relation.from == entity || &relation.to == entity);

            if touches_subject || touches_relation {
                result.events.insert(event.id.clone());
            }
        }
        for (operation_id, operation) in self.operations.iter() {
            if operation.facts.iter().any(|fact| {
                matches!(
                    fact,
                    Fact::EventRecorded(event) if result.events.contains(&event.id)
                )
            }) {
                result.operations.insert(operation_id.clone());
            }
        }

        Ok(result)
    }

    fn query_traverse(
        &self,
        roots: &BTreeSet<EntityRef<M>>,

        direction: Direction,

        relations: &RelationSelector,

        max_depth: Option<usize>,

        predicate: &Predicate<M>,
    ) -> Result<QueryResult<M>> {
        let mut result = QueryResult::default();

        let mut visited = BTreeSet::new();

        let mut queue = VecDeque::new();

        for root in roots {
            queue.push_back((root.clone(), 0usize));
        }

        while let Some((entity, depth)) = queue.pop_front() {
            if !visited.insert(entity.clone()) {
                continue;
            }

            if predicate_matches(&self.projection, &entity, predicate) {
                result.entities.insert(entity.clone());
            }

            if max_depth.is_some_and(|max| depth >= max) {
                continue;
            }

            let edges = self.edges_for(&entity, direction);

            for edge in edges {
                if !relation_selected(&self.projection, &edge.relation, relations)? {
                    continue;
                }

                let next = traversal_other_end(&entity, &edge.relation, direction);

                let Some(next) = next else {
                    continue;
                };

                result.edges.insert(edge.clone());

                result.events.insert(edge.event.clone());

                result.operations.insert(edge.operation.clone());

                queue.push_back((next, depth + 1));
            }
        }

        Ok(result)
    }

    fn query_explain(
        &self,
        root: &EntityRef<M>,

        max_depth: Option<usize>,

        roles: &BTreeSet<ExplanationRole>,
    ) -> Result<QueryResult<M>> {
        let mut result = QueryResult::default();

        let mut visited = BTreeSet::new();

        let mut queue = VecDeque::new();

        queue.push_back((root.clone(), 0usize));

        while let Some((entity, depth)) = queue.pop_front() {
            if !visited.insert(entity.clone()) {
                continue;
            }

            result.entities.insert(entity.clone());

            if max_depth.is_some_and(|max| depth >= max) {
                continue;
            }

            let mut candidate_edges = Vec::new();

            if let Some(edges) = self.projection.outgoing.get(&entity) {
                candidate_edges.extend(edges.iter());
            }

            if let Some(edges) = self.projection.incoming.get(&entity) {
                candidate_edges.extend(edges.iter());
            }

            for edge in candidate_edges {
                let relation_schema = find_relation_schema(&self.projection, &edge.relation)?;

                let Some(explanation) = relation_schema.explanation else {
                    continue;
                };

                if !roles.is_empty() && !roles.contains(&explanation.role) {
                    continue;
                }

                let next = explanation_predecessor(&entity, &edge.relation, explanation.direction);

                let Some(next) = next else {
                    continue;
                };

                result.edges.insert(edge.clone());

                result.events.insert(edge.event.clone());

                result.operations.insert(edge.operation.clone());

                queue.push_back((next, depth + 1));
            }
        }

        Ok(result)
    }

    fn edges_for(&self, entity: &EntityRef<M>, direction: Direction) -> Vec<GraphEdge<M>> {
        let mut result = BTreeSet::new();

        if matches!(direction, Direction::Outgoing | Direction::Both)
            && let Some(edges) = self.projection.outgoing.get(entity)
        {
            result.extend(edges.iter().cloned());
        }

        if matches!(direction, Direction::Incoming | Direction::Both)
            && let Some(edges) = self.projection.incoming.get(entity)
        {
            result.extend(edges.iter().cloned());
        }

        result.into_iter().collect()
    }
}

fn relation_selected<M: Model>(
    projection: &Projection<M>,

    relation: &Relation<M>,

    selector: &RelationSelector,
) -> Result<bool> {
    match selector {
        RelationSelector::Any => Ok(true),

        RelationSelector::Types(types) => Ok(types.contains(&relation.relation_type)),

        RelationSelector::Explanatory => Ok(find_relation_schema(projection, relation)?
            .explanation
            .is_some()),
    }
}

fn traversal_other_end<M: Model>(
    current: &EntityRef<M>,

    relation: &Relation<M>,

    direction: Direction,
) -> Option<EntityRef<M>> {
    match direction {
        Direction::Outgoing if &relation.from == current => Some(relation.to.clone()),

        Direction::Incoming if &relation.to == current => Some(relation.from.clone()),

        Direction::Both if &relation.from == current => Some(relation.to.clone()),

        Direction::Both if &relation.to == current => Some(relation.from.clone()),

        _ => None,
    }
}

fn explanation_predecessor<M: Model>(
    current: &EntityRef<M>,

    relation: &Relation<M>,

    direction: ExplanationDirection,
) -> Option<EntityRef<M>> {
    match direction {
        ExplanationDirection::FromExplainedByTo if &relation.from == current => {
            Some(relation.to.clone())
        }

        ExplanationDirection::ToExplainedByFrom if &relation.to == current => {
            Some(relation.from.clone())
        }

        ExplanationDirection::Symmetric if &relation.from == current => Some(relation.to.clone()),

        ExplanationDirection::Symmetric if &relation.to == current => Some(relation.from.clone()),

        _ => None,
    }
}

fn predicate_matches<M: Model>(
    projection: &Projection<M>,

    entity: &EntityRef<M>,

    predicate: &Predicate<M>,
) -> bool {
    match predicate {
        Predicate::Any => true,

        Predicate::EntityType(expected) => matches_entity_type(entity, expected),

        Predicate::PropertyEquals { field, value } => {
            let EntityRef::External(address) = entity else {
                return false;
            };

            projection
                .entities
                .get(address)
                .and_then(|observation| observation.attributes.get(field))
                == Some(value)
        }

        Predicate::And(predicates) => predicates
            .iter()
            .all(|predicate| predicate_matches(projection, entity, predicate)),

        Predicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| predicate_matches(projection, entity, predicate)),

        Predicate::Not(predicate) => !predicate_matches(projection, entity, predicate),
    }
}

// =============================================================================
// ADAPTER
// =============================================================================

/// Pure plugin/integration boundary.
///
/// JJ adapter, issue adapter, docs adapter, agent adapter, etc. implement this.
///
/// A WASM host would convert WIT values into Adapter-native input or directly
/// into Transaction.
///
/// The kernel does not know WIT exists.
pub trait Adapter<M: Model> {
    type Input;
    type Error;

    fn transaction(&self, input: Self::Input) -> std::result::Result<Transaction<M>, Self::Error>;

    fn core_error(&self, error: Error) -> Self::Error;
}

pub trait AdapterExt<M: Model>: Adapter<M> {
    fn plan<I, R>(
        &self,
        kernel: &Kernel<M, I, R>,

        state: &State<M>,

        input: Self::Input,
    ) -> std::result::Result<CommitPlan<M>, Self::Error>
    where
        I: IdentityScheme<M>,
        R: Rules<M>,
    {
        let transaction = self.transaction(input)?;

        kernel
            .transact(state, transaction)
            .map_err(|error| self.core_error(error))
    }
}

impl<M, A> AdapterExt<M> for A
where
    M: Model,
    A: Adapter<M>,
{
}

// =============================================================================
// RUNTIME
// =============================================================================

/// Effectful implementation contract.
///
/// This may be implemented by:
///   filesystem store
///   object store
///   VCS retention layer
///   distributed service
///   anything else
///
/// Core itself remains pure.
pub trait Runtime<M: Model> {
    type Error;

    /// Idempotently establish prerequisite.
    fn prepare(
        &mut self,
        requirement: &PrepareRequirement<M>,
    ) -> std::result::Result<(), Self::Error>;

    /// Atomically publish immutable provenance Operation.
    ///
    /// Same operation twice must be harmless.
    fn publish(&mut self, operation: &Operation<M>) -> std::result::Result<(), Self::Error>;

    /// Idempotent post-publication reconciliation.
    fn finalize(&mut self, action: &FinalizeAction<M>) -> std::result::Result<(), Self::Error>;
}

// =============================================================================
// SERVICE
// =============================================================================

pub struct Service<M, I, R, A, RT>
where
    M: Model,
    I: IdentityScheme<M>,
    R: Rules<M>,
    A: Adapter<M>,
    RT: Runtime<M>,
{
    pub kernel: Kernel<M, I, R>,

    pub adapter: A,

    pub runtime: RT,

    pub state: State<M>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProcessError<A, R> {
    Adapter(A),

    Prepare(R),

    Publish(R),

    /// IMPORTANT:
    ///
    /// Operation is already authoritative when this is returned.
    /// Caller state has already advanced.
    Finalize(R),
}

impl<M, I, R, A, RT> Service<M, I, R, A, RT>
where
    M: Model,
    I: IdentityScheme<M>,
    R: Rules<M>,
    A: Adapter<M>,
    RT: Runtime<M>,
{
    pub fn process(
        &mut self,
        input: A::Input,
    ) -> std::result::Result<(), ProcessError<A::Error, RT::Error>> {
        let plan = self
            .adapter
            .plan(&self.kernel, &self.state, input)
            .map_err(ProcessError::Adapter)?;

        if plan.idempotent {
            return Ok(());
        }

        // ---------------------------------------------------------------------
        // PREPARE
        //
        // State must NOT advance if this fails.
        // ---------------------------------------------------------------------

        for requirement in &plan.prepare {
            self.runtime
                .prepare(requirement)
                .map_err(ProcessError::Prepare)?;
        }

        // ---------------------------------------------------------------------
        // PUBLISH
        //
        // This is the commit point.
        // ---------------------------------------------------------------------

        self.runtime
            .publish(&plan.operation)
            .map_err(ProcessError::Publish)?;

        // ---------------------------------------------------------------------
        // Operation is authoritative from HERE onward.
        // ---------------------------------------------------------------------

        self.state = plan.next_state;

        // ---------------------------------------------------------------------
        // FINALIZE
        //
        // Failure does NOT roll back state.
        //
        // Reconciliation should retry these operations later.
        // ---------------------------------------------------------------------

        for action in &plan.finalize {
            self.runtime
                .finalize(action)
                .map_err(ProcessError::Finalize)?;
        }

        Ok(())
    }
}

// =============================================================================
// QUERY EXECUTION ABSTRACTION
// =============================================================================

/// The pure State implementation above is the reference semantics.
///
/// Production indexes can implement this trait using SQLite, DuckDB, remote
/// query services, etc.
///
/// They must return equivalent semantic results.
pub trait QueryEngine<M: Model> {
    type Error;

    fn execute(&self, query: &Query<M>) -> std::result::Result<QueryResult<M>, Self::Error>;
}

impl<M: Model> QueryEngine<M> for State<M> {
    type Error = Error;

    fn execute(&self, query: &Query<M>) -> Result<QueryResult<M>> {
        self.query(query)
    }
}

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
//     query indexes
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
//     schemas indexes
//     active retention
//     query indexes
//     full text indexes
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
