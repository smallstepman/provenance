//! Retention requirements, observations, and aggregation transitions.

use provenance_data_model::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

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

// RETENTION AGGREGATION
// =============================================================================

pub(crate) fn retention_transition<M: Model>(
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

pub(crate) fn retention_is_prepare<M: Model>(transition: &RetentionTransition<M>) -> bool {
    matches!(
        transition,
        RetentionTransition::BecameRequired { .. } | RetentionTransition::Strengthened { .. }
    )
}

// =============================================================================
