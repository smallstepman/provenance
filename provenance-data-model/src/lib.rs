//! provenance-data-model
//!
//! Pure, storage-agnostic provenance vocabulary.
//!
//! This crate represents:
//!
//!   - entities
//!   - activities
//!   - agents
//!   - assertions
//!   - semantic relationships
//!   - temporal validity
//!   - schemas
//!   - provenance bundles
//!
//! It deliberately does NOT contain:
//!
//!   - persistence
//!   - transactions
//!   - locks
//!   - guards
//!   - publication
//!   - retention / pinning / escrow
//!   - query execution
//!   - plugin loading
//!   - WIT / WASM
//!   - filesystem concepts
//!   - database concepts
//!   - JJ / Git concepts
//!
//! Domain concepts such as:
//!
//!   jj:commit
//!   tracker:task
//!   docs:rfc
//!   semantic:customer
//!   technical:column
//!   agent:run
//!
//! are schema-defined kinds, not Rust enums in this crate.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    marker::PhantomData,
};

// =============================================================================
// FOUNDATIONS
// =============================================================================

pub trait Key: Clone + Debug + Eq + Ord + Send + Sync + 'static {}

impl<T> Key for T where T: Clone + Debug + Eq + Ord + Send + Sync + 'static {}

/// Consumer-chosen scalar identity types.
///
/// The data model imposes no generation or persistence semantics.
pub trait Model: Send + Sync + 'static {
    /// Internal provenance identity representation.
    type Id: Key;

    /// Identity local to an external namespace.
    ///
    /// Examples:
    ///
    ///   "abc123"
    ///   "PROJ-42"
    ///   "0001"
    ///   "customer-182"
    ///
    type ExternalId: Key;
}

// =============================================================================
// SYMBOL TYPES
// =============================================================================

macro_rules! symbol {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

symbol!(Namespace);
symbol!(Kind);
symbol!(Predicate);
symbol!(FieldName);
symbol!(SchemaVersion);
symbol!(VocabularyName);

// =============================================================================
// TYPED INTERNAL IDS
// =============================================================================

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

pub enum EntityTag {}
pub enum ActivityTag {}
pub enum AgentTag {}
pub enum AssertionTag {}
pub enum BundleTag {}

pub type EntityId<M> = Id<<M as Model>::Id, EntityTag>;

pub type ActivityId<M> = Id<<M as Model>::Id, ActivityTag>;

pub type AgentId<M> = Id<<M as Model>::Id, AgentTag>;

pub type AssertionId<M> = Id<<M as Model>::Id, AssertionTag>;

pub type BundleId<M> = Id<<M as Model>::Id, BundleTag>;

// =============================================================================
// EXTERNAL ADDRESSING
// =============================================================================

/// Structured identity from some external semantic universe.
///
/// Examples, rendered elsewhere:
///
///   jj://commit/abc123
///   tracker://task/PROJ-42
///   docs://rfc/0001
///   agent://run/019...
///   semantic://customer/customer-182
///   technical://column/customer.f_name
///
/// URI parsing/rendering does not belong in this crate.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExternalRef<M: Model> {
    pub namespace: Namespace,
    pub kind: Kind,
    pub id: M::ExternalId,
}

impl<M: Model> ExternalRef<M> {
    pub fn new(namespace: impl Into<Namespace>, kind: impl Into<Kind>, id: M::ExternalId) -> Self {
        Self {
            namespace: namespace.into(),

            kind: kind.into(),

            id,
        }
    }

    pub fn ty(&self) -> ExternalType {
        ExternalType {
            namespace: self.namespace.clone(),

            kind: self.kind.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExternalType {
    pub namespace: Namespace,
    pub kind: Kind,
}

// =============================================================================
// UNIVERSAL NODE REFERENCES
// =============================================================================

/// Anything that may participate in a provenance relationship.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum NodeRef<M: Model> {
    Entity(EntityId<M>),
    Activity(ActivityId<M>),
    Agent(AgentId<M>),

    /// Identity whose authoritative namespace exists elsewhere.
    External(ExternalRef<M>),

    /// Assertions are first-class provenance artifacts and may themselves be
    /// supported, contradicted, superseded, invalidated, etc.
    Assertion(AssertionId<M>),
}

impl<M: Model> NodeRef<M> {
    pub fn external(
        namespace: impl Into<Namespace>,
        kind: impl Into<Kind>,
        id: M::ExternalId,
    ) -> Self {
        Self::External(ExternalRef::new(namespace, kind, id))
    }
}

// =============================================================================
// STRUCTURED VALUES
// =============================================================================

/// Strict structured values without requiring JSON.
///
/// Plugin/WIT DTOs can translate to/from this type.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value<M: Model> {
    Null,
    Bool(bool),
    Integer(i128),

    /// Decimal is textual so this crate does not impose a decimal implementation.
    Decimal(Box<str>),

    String(Box<str>),
    Bytes(Vec<u8>),

    Ref(NodeRef<M>),

    List(Vec<Value<M>>),

    Map(BTreeMap<FieldName, Value<M>>),
}

pub type Attributes<M> = BTreeMap<FieldName, Value<M>>;

// =============================================================================
// ENTITY
// =============================================================================

/// A thing that exists or existed.
///
/// Examples:
///
///   commit
///   change
///   issue
///   RFC
///   document
///   customer
///   table
///   column
///   dataset
///   binary
///   deployment
///   model response
///   derived fact
///
/// Domain-specific kinds are provided through schemas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entity<M: Model> {
    pub id: EntityId<M>,

    pub kind: Kind,

    /// Optional identity in an external universe.
    pub external: Option<ExternalRef<M>>,

    pub attributes: Attributes<M>,
}

impl<M: Model> Entity<M> {
    pub fn reference(&self) -> NodeRef<M> {
        NodeRef::Entity(self.id.clone())
    }
}

// =============================================================================
// AGENT
// =============================================================================

/// Something to which responsibility or agency may be attributed.
///
/// Examples:
///
///   human
///   LLM agent
///   CI service
///   workflow engine
///   bot
///   organization
#[derive(Clone, Debug, PartialEq, Eq)]
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

// =============================================================================
// ACTIVITY
// =============================================================================

/// Something that happened.
///
/// Examples:
///
///   JJ operation
///   agent run
///   human editing session
///   tool invocation
///   inference
///   database query
///   compilation
///   CI run
///   deployment
///   issue transition
///   extraction pipeline
///
/// Inputs, outputs, ownership and causality are represented through assertions,
/// rather than duplicated structurally here.
#[derive(Clone, Debug, PartialEq, Eq)]
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

// =============================================================================
// RELATION TYPES
// =============================================================================

/// A semantic predicate.
///
/// Examples:
///
///   prov:used
///   prov:generated-by
///   jj:belongs-to-change
///   tracker:blocked-by
///   docs:supersedes
///   semantic:represented-by
///   agent:selected-source
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelationType {
    pub namespace: Namespace,

    pub predicate: Predicate,
}

impl RelationType {
    pub fn new(namespace: impl Into<Namespace>, predicate: impl Into<Predicate>) -> Self {
        Self {
            namespace: namespace.into(),

            predicate: predicate.into(),
        }
    }
}

// =============================================================================
// CORE VOCABULARY
// =============================================================================

/// Optional conventional provenance vocabulary.
///
/// These are convenience names only.
///
/// Schemas and plugins remain free to introduce arbitrary relation types.
pub mod core_predicates {
    pub const USED: &str = "used";

    pub const GENERATED_BY: &str = "generated-by";

    pub const DERIVED_FROM: &str = "derived-from";

    pub const ASSOCIATED_WITH: &str = "associated-with";

    pub const ATTRIBUTED_TO: &str = "attributed-to";

    pub const INFORMED_BY: &str = "informed-by";

    pub const SUPPORTS: &str = "supports";

    pub const CONTRADICTS: &str = "contradicts";

    pub const INVALIDATES: &str = "invalidates";

    pub const SUPERSEDES: &str = "supersedes";

    pub const MERGED_INTO: &str = "merged-into";

    pub const INFLUENCED_BY: &str = "influenced-by";

    pub const REPRESENTED_BY: &str = "represented-by";

    pub const SELECTED: &str = "selected";

    pub const CONSIDERED: &str = "considered";

    pub const PRODUCED: &str = "produced";
}

// =============================================================================
// ASSERTION
// =============================================================================

/// A first-class claim about a relationship.
///
/// This is one of the most important choices in the model.
///
/// Instead of treating:
///
///     commit C --implements--> task T
///
/// as an anonymous graph edge, it becomes an identifiable provenance artifact.
///
/// That assertion can itself have provenance:
///
///     assertion A
///         generated-by
///     agent run R
///
/// and can later be:
///
///     supported
///     contradicted
///     superseded
///     invalidated
///
/// This is especially important for LLM-generated/synthesized knowledge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assertion<M: Model> {
    pub id: AssertionId<M>,

    pub subject: NodeRef<M>,

    pub predicate: RelationType,

    pub object: NodeRef<M>,

    /// Optional activity that produced this assertion.
    ///
    /// This is a convenience field representing the extremely common
    /// "assertion generated by activity" relationship.
    ///
    /// Consumers may alternatively model this purely through assertions.
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

// =============================================================================
// TEMPORAL / VALIDITY REPRESENTATION
// =============================================================================

/// Historical validity of an assertion.
///
/// This type REPRESENTS temporal semantics.
///
/// It does not enforce them.
///
/// Old assertions are never required to disappear merely because later
/// assertions supersede or invalidate them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Validity<M: Model> {
    /// No validity information was supplied.
    Unspecified,

    /// Assertion is represented as currently active.
    Active,

    /// Another assertion superseded this one.
    Superseded { by: AssertionId<M> },

    /// Another assertion explicitly invalidated this one.
    Invalidated { by: AssertionId<M> },

    /// Logical interval using provenance bundle boundaries.
    ///
    /// This avoids requiring wall-clock timestamps in the foundational model.
    Interval {
        from: BundleId<M>,

        until: Option<BundleId<M>>,
    },
}

// =============================================================================
// EXPLANATION SEMANTICS
// =============================================================================

/// Optional meaning for explanation-oriented consumers such as `why`.
///
/// This is schema metadata, not query behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ExplanationRole {
    /// Direct causal dependency.
    Causal,

    /// Evidence supporting a claim.
    Supporting,

    /// Evidence opposing a claim.
    Contradicting,

    /// Relation describing invalidation.
    Invalidating,

    /// Responsibility/ownership.
    Attribution,

    /// Influential but weaker than direct causality.
    Influence,

    /// Context useful to explanation but not itself causal.
    Contextual,
}

/// Defines which endpoint is explained by the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplanationDirection {
    SubjectExplainedByObject,
    ObjectExplainedBySubject,
    Symmetric,
}

// =============================================================================
// SCHEMA TYPES
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchemaId {
    pub namespace: Namespace,

    pub version: SchemaVersion,
}

impl SchemaId {
    pub fn new(namespace: impl Into<Namespace>, version: impl Into<SchemaVersion>) -> Self {
        Self {
            namespace: namespace.into(),

            version: version.into(),
        }
    }
}

/// Type-pattern for references appearing in schemas.
#[derive(Clone, Debug, PartialEq, Eq)]
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

// =============================================================================
// ATTRIBUTE SCHEMAS
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueType {
    Null,
    Bool,
    Integer,
    Decimal,
    String,
    Bytes,

    Ref(NodeType),

    List(Box<ValueType>),

    Map {
        fields: BTreeMap<FieldName, FieldSchema>,

        allow_unknown: bool,
    },

    Any,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

// =============================================================================
// NODE SCHEMAS
// =============================================================================

/// Schema for one domain-defined node kind.
///
/// Namespace ownership is expressed by the containing Schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSchema {
    pub kind: Kind,

    pub fields: BTreeMap<FieldName, FieldSchema>,

    pub allow_unknown_fields: bool,
}

// =============================================================================
// RELATION SCHEMA
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelationSchema {
    pub relation_type: RelationType,

    pub subject: NodeType,

    pub object: NodeType,

    pub fields: BTreeMap<FieldName, FieldSchema>,

    pub allow_unknown_fields: bool,

    /// Optional semantics for explanation consumers.
    pub explanation_role: Option<ExplanationRole>,

    pub explanation_direction: Option<ExplanationDirection>,
}

// =============================================================================
// SCHEMA
// =============================================================================

/// Versioned ontology contributed by a domain/plugin.
///
/// Examples:
///
///     jj@1
///     tracker@2
///     docs@1
///     agent@3
///     semantic@5
///     technical@2
///
/// Historical provenance can retain the exact SchemaId under which it was
/// interpreted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub id: SchemaId,

    /// Other schemas referenced by this schema.
    pub requires: BTreeSet<SchemaId>,

    pub entities: BTreeMap<Kind, NodeSchema>,

    pub activities: BTreeMap<Kind, NodeSchema>,

    pub agents: BTreeMap<Kind, NodeSchema>,

    pub relations: BTreeMap<Predicate, RelationSchema>,
}

// =============================================================================
// BUNDLE
// =============================================================================

/// Immutable logical grouping of provenance observations.
///
/// Examples:
///
///     one JJ operation
///     one issue transition
///     one agent run
///     one CI execution
///     one imported trace
///     one semantic extraction episode
///
/// IMPORTANT:
///
/// This is ONLY representation.
///
/// It does not imply:
///
///     transaction
///     atomicity
///     publication
///     locking
///     persistence
///     retention
///
/// Higher layers may assign those semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bundle<M: Model> {
    pub id: BundleId<M>,

    /// Optional external source represented by this bundle.
    pub source: Option<ExternalRef<M>>,

    /// Logical/causal bundle ancestry.
    pub parents: BTreeSet<BundleId<M>>,

    /// Exact schema versions used by this bundle.
    pub schemas: BTreeSet<SchemaId>,

    pub entities: Vec<Entity<M>>,

    pub activities: Vec<Activity<M>>,

    pub agents: Vec<Agent<M>>,

    pub assertions: Vec<Assertion<M>>,

    pub attributes: Attributes<M>,
}

// =============================================================================
// GRAPH DOCUMENT
// =============================================================================

/// Portable aggregate of provenance data.
///
/// Possible uses:
///
///     export/import
///     plugin payload
///     synchronization payload
///     query response
///     archive format
///     interchange format
///
/// Still no storage semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvenanceGraph<M: Model> {
    pub schemas: Vec<Schema>,

    pub bundles: Vec<Bundle<M>>,
}

// =============================================================================
// OPTIONAL SEMANTIC CONVENIENCE TYPES
// =============================================================================

/// A projection/classification attached to a node.
///
/// Example:
///
///     source.authoritative = true
///     environment = production
///     classification = pii
///
/// Whether/how these propagate is NOT defined here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Classification<M: Model> {
    pub subject: NodeRef<M>,

    pub field: FieldName,

    pub value: Value<M>,
}

/// Represents entity reconciliation without destroying the original identities.
///
/// Consumers would normally encode this as an Assertion using `merged-into` or
/// a domain-specific predicate. This helper merely gives such tooling a common
/// shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityResolution<M: Model> {
    pub members: BTreeSet<NodeRef<M>>,

    pub canonical: NodeRef<M>,

    pub generated_by: Option<ActivityId<M>>,

    pub attributes: Attributes<M>,
}

// =============================================================================
// EXAMPLE DOMAIN SHAPES
// =============================================================================
//
// None of the following become Rust enums here.
//
// -----------------------------------------------------------------------------
// SOFTWARE DEVELOPMENT
// -----------------------------------------------------------------------------
//
// Entity:
//
//     jj://commit/abc
//     tracker://task/PROJ-42
//     docs://rfc/0001
//
// Activity:
//
//     agent://run/R1
//     jj://operation/O42
//
// Agent:
//
//     agent://coding-agent/codex
//     identity://human/alice
//
// Assertions:
//
//     commit abc
//         implements
//     task PROJ-42
//
//     commit abc
//         generated-by
//     agent run R1
//
//     agent run R1
//         used
//     RFC 0001
//
//     commit abc
//         derived-from
//     commit def
//
//
// -----------------------------------------------------------------------------
// APPLICATION-LEVEL AGENT
// -----------------------------------------------------------------------------
//
// Entity:
//
//     semantic://concept/customer.credit-limit
//     technical://column/crm.customer.credit_limit
//     technical://api/customer-service/get-customer
//     answer://artifact/A1
//
// Activity:
//
//     agent://run/R99
//     tool://query/Q11
//
// Agent:
//
//     agent://support-assistant/v3
//
// Assertions:
//
//     semantic customer.credit-limit
//         represented-by
//     technical crm.customer.credit_limit
//
//     agent run R99
//         considered
//     API A
//
//     agent run R99
//         considered
//     database B
//
//     agent run R99
//         selected
//     database B
//
//     agent run R99
//         used
//     crm.customer.credit_limit
//
//     answer A1
//         generated-by
//     agent run R99
//
//
// -----------------------------------------------------------------------------
// SYNTHESIZED KNOWLEDGE
// -----------------------------------------------------------------------------
//
// Sources:
//
//     E1
//     E2
//     E3
//
// Activity:
//
//     agent inference R
//
// Assertion A:
//
//     customer X
//         risk-level
//     high
//
// A.generated_by = R
//
// Assertions:
//
//     R used E1
//     R used E2
//     R used E3
//
// Later:
//
// Assertion B
//     contradicts
// Assertion A
//
// or:
//
// Assertion C
//     invalidates
// Assertion A
//
//
// The old assertion remains part of history.
//
// =============================================================================
// DESIGN INVARIANTS OF THIS CRATE
// =============================================================================
//
// 1. Domain-specific concepts are schema-defined, not hard-coded.
//
// 2. Assertions are first-class objects.
//
// 3. Assertions may themselves participate in provenance.
//
// 4. Activities represent causal/action units.
//
// 5. Agents represent responsibility/agency.
//
// 6. Entities represent things.
//
// 7. External identities are structured as:
//
//        namespace + kind + external ID
//
//    rather than opaque URI strings.
//
// 8. Historical invalidation/supersession is represented rather than erased.
//
// 9. Multi-source synthesis is naturally represented through:
//
//        source entities
//             ↓
//        used by activity
//             ↓
//        generated assertion/entity
//
// 10. Semantic ontology and technical ontology use exactly the same graph
//     primitives.
//
// 11. Software provenance and application-agent provenance use exactly the
//     same graph primitives.
//
// 12. This crate does not decide what is true, trusted, retained, queryable,
//     stored, committed, visible, or permitted.
//
// =============================================================================
