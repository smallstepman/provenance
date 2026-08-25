//! Storage-agnostic provenance data model.
//!
//! This crate owns the typed vocabulary shared by provenance adapters and the
//! provenance kernel: identities, structured values, external addresses,
//! schemas, observations, relations, and portable graph records. It does not
//! own transactions, publication, retention effects, query execution, or
//! persistence.

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
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

// Compatibility aliases for the vocabulary-oriented API.
pub type Kind = EntityKind;
pub type Predicate = RelationName;
text_newtype!(VocabularyName);

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

/// Only one primitive must be implemented by a provenance model.
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
    pub fn new(
        namespace: impl Into<Namespace>,
        kind: impl Into<EntityKind>,
        id: M::ExternalId,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            kind: kind.into(),
            id,
        }
    }

    pub fn entity_type(&self) -> EntityType {
        EntityType {
            namespace: self.namespace.clone(),
            kind: self.kind.clone(),
        }
    }

    pub fn ty(&self) -> EntityType {
        self.entity_type()
    }
}

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

    pub fn external(
        namespace: impl Into<Namespace>,
        kind: impl Into<EntityKind>,
        id: M::ExternalId,
    ) -> Self {
        Self::External(EntityAddress::new(namespace, kind, id))
    }
}

// =============================================================================
// STRICT STRUCTURED VALUES
// =============================================================================

/// Kernel-compatible structured values. `Ref` is retained for the portable
/// vocabulary API; `Entity` is the operation/query representation.
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
    Ref(NodeRef<M>),
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

impl SchemaKey {
    pub fn new(namespace: impl Into<Namespace>, version: impl Into<SchemaVersion>) -> Self {
        Self {
            namespace: namespace.into(),
            version: version.into(),
        }
    }
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
    Ref(NodeType),
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

impl FieldSchema {
    pub fn required(value_type: ValueType) -> Self {
        Self {
            value_type,
            required: true,
        }
    }

    pub fn optional(value_type: ValueType) -> Self {
        Self {
            value_type,
            required: false,
        }
    }
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

impl RelationType {
    pub fn new(namespace: impl Into<Namespace>, name: impl Into<RelationName>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    pub fn predicate(&self) -> &RelationName {
        &self.name
    }
}

// =============================================================================
// EXPLANATION SEMANTICS
// =============================================================================

/// Plugins can tell the kernel whether a relation participates in `why`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExplanationRole {
    Primary,
    Supporting,
    Contextual,
    Causal,
    Contradicting,
    Invalidating,
    Attribution,
    Influence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExplanationDirection {
    FromExplainedByTo,
    ToExplainedByFrom,
    Symmetric,
    SubjectExplainedByObject,
    ObjectExplainedBySubject,
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

// Compatibility aliases for the extracted vocabulary API.
pub type ExternalRef<M> = EntityAddress<M>;
pub type ExternalType = EntityType;
pub type SchemaId = SchemaKey;

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
// CORE DATA ENTITIES
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

// =============================================================================
// VOCABULARY NODE REFERENCES AND BUNDLES
// =============================================================================

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum EntityTag {}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum ActivityTag {}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum AgentTag {}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum AssertionTag {}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum BundleTag {}

pub type EntityId<M> = Id<<M as Model>::Id, EntityTag>;
pub type ActivityId<M> = Id<<M as Model>::Id, ActivityTag>;
pub type AgentId<M> = Id<<M as Model>::Id, AgentTag>;
pub type AssertionId<M> = Id<<M as Model>::Id, AssertionTag>;
pub type BundleId<M> = Id<<M as Model>::Id, BundleTag>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum NodeRef<M: Model> {
    Entity(EntityId<M>),
    Activity(ActivityId<M>),
    Agent(AgentId<M>),
    External(ExternalRef<M>),
    Assertion(AssertionId<M>),
}

impl<M: Model> NodeRef<M> {
    pub fn external(
        namespace: impl Into<Namespace>,
        kind: impl Into<Kind>,
        id: M::ExternalId,
    ) -> Self {
        Self::External(EntityAddress {
            namespace: namespace.into(),
            kind: kind.into(),
            id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Entity<M: Model> {
    pub id: EntityId<M>,
    pub kind: Kind,
    pub external: Option<ExternalRef<M>>,
    pub attributes: Attributes<M>,
}

impl<M: Model> Entity<M> {
    pub fn reference(&self) -> NodeRef<M> {
        NodeRef::Entity(self.id.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Activity<M: Model> {
    pub id: ActivityId<M>,
    pub kind: Kind,
    pub external: Option<ExternalRef<M>>,
    pub attributes: Attributes<M>,
}

impl<M: Model> Activity<M> {
    pub fn reference(&self) -> NodeRef<M> {
        NodeRef::Activity(self.id.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Agent<M: Model> {
    pub id: AgentId<M>,
    pub kind: Kind,
    pub external: Option<ExternalRef<M>>,
    pub attributes: Attributes<M>,
}

impl<M: Model> Agent<M> {
    pub fn reference(&self) -> NodeRef<M> {
        NodeRef::Agent(self.id.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Assertion<M: Model> {
    pub id: AssertionId<M>,
    pub subject: NodeRef<M>,
    pub predicate: RelationType,
    pub object: NodeRef<M>,
    pub generated_by: Option<ActivityId<M>>,
    pub validity: Validity<M>,
    pub attributes: Attributes<M>,
}

impl<M: Model> Assertion<M> {
    pub fn new(
        id: AssertionId<M>,
        subject: NodeRef<M>,
        predicate: RelationType,
        object: NodeRef<M>,
    ) -> Self {
        Self {
            id,
            subject,
            predicate,
            object,
            generated_by: None,
            validity: Validity::Unspecified,
            attributes: BTreeMap::new(),
        }
    }

    pub fn reference(&self) -> NodeRef<M> {
        NodeRef::Assertion(self.id.clone())
    }

    pub fn generated_by(mut self, activity: ActivityId<M>) -> Self {
        self.generated_by = Some(activity);
        self
    }

    pub fn with_validity(mut self, validity: Validity<M>) -> Self {
        self.validity = validity;
        self
    }

    pub fn with_attribute(mut self, field: impl Into<FieldName>, value: Value<M>) -> Self {
        self.attributes.insert(field.into(), value);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum Validity<M: Model> {
    Unspecified,
    Active,
    Superseded {
        by: AssertionId<M>,
    },
    Invalidated {
        by: AssertionId<M>,
    },
    Interval {
        from: BundleId<M>,
        until: Option<BundleId<M>>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSchema {
    pub kind: Kind,
    pub fields: BTreeMap<FieldName, FieldSchema>,
    pub allow_unknown_fields: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    Entity {
        namespace: Option<Namespace>,
        kind: Kind,
    },
    Activity {
        namespace: Option<Namespace>,
        kind: Kind,
    },
    Agent {
        namespace: Option<Namespace>,
        kind: Kind,
    },
    Assertion,
    External(ExternalType),
    Any,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub id: SchemaId,
    pub requires: BTreeSet<SchemaId>,
    pub entities: BTreeMap<Kind, NodeSchema>,
    pub activities: BTreeMap<Kind, NodeSchema>,
    pub agents: BTreeMap<Kind, NodeSchema>,
    pub relations: BTreeMap<Predicate, RelationSchema>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Bundle<M: Model> {
    pub id: BundleId<M>,
    pub source: Option<ExternalRef<M>>,
    pub parents: BTreeSet<BundleId<M>>,
    pub schemas: BTreeSet<SchemaId>,
    pub entities: Vec<Entity<M>>,
    pub activities: Vec<Activity<M>>,
    pub agents: Vec<Agent<M>>,
    pub assertions: Vec<Assertion<M>>,
    pub attributes: Attributes<M>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct ProvenanceGraph<M: Model> {
    pub schemas: Vec<Schema>,
    pub bundles: Vec<Bundle<M>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Classification<M: Model> {
    pub subject: NodeRef<M>,
    pub field: FieldName,
    pub value: Value<M>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct IdentityResolution<M: Model> {
    pub members: BTreeSet<NodeRef<M>>,
    pub canonical: NodeRef<M>,
    pub generated_by: Option<ActivityId<M>>,
    pub attributes: Attributes<M>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;

    #[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
    struct TestModel;

    impl Model for TestModel {
        type Id = String;
        type Seed = String;
        type ExternalId = String;
        type Payload = Vec<u8>;
    }

    fn assert_serde<T: Serialize + DeserializeOwned>() {}

    #[test]
    fn current_kernel_vocabulary_is_serde_capable() {
        assert_serde::<EntityAddress<TestModel>>();
        assert_serde::<EntityRef<TestModel>>();
        assert_serde::<Value<TestModel>>();
        assert_serde::<SourceOperation<TestModel>>();
        assert_serde::<Bundle<TestModel>>();
    }
}
