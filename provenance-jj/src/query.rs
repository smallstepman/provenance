use std::collections::BTreeSet;

use provenance_core::{EntityAddress, EntityRef, ExplanationRole, Query};
use thiserror::Error;

use crate::model::{JJ_NAMESPACE, external, jj_address, jj_commit_address};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum JjQueryError {
    #[error("invalid entity URI: {0}")]
    InvalidUri(String),
}

/// Constructs an external JJ commit reference for the generic query engine.
pub fn jj_commit(id: impl AsRef<str>) -> EntityRef<crate::JjModel> {
    let id = id.as_ref();
    let id = id.strip_prefix("jj://commit/").unwrap_or(id);
    external(jj_commit_address(id))
}

pub fn jj_entity(uri: impl AsRef<str>) -> Result<EntityRef<crate::JjModel>, JjQueryError> {
    let address = parse_jj_uri(uri.as_ref())?;
    Ok(external(address))
}

/// Compiles `why jj://...` input into the generic `Query::Explain` variant.
pub fn compile_why(uri: impl AsRef<str>) -> Result<Query<crate::JjModel>, JjQueryError> {
    Ok(why_entity(jj_entity(uri)?))
}

pub fn why_jj_commit(id: impl AsRef<str>) -> Query<crate::JjModel> {
    why_entity(jj_commit(id))
}

fn why_entity(root: EntityRef<crate::JjModel>) -> Query<crate::JjModel> {
    Query::Explain {
        root,
        max_depth: None,
        roles: BTreeSet::from([ExplanationRole::Primary, ExplanationRole::Supporting]),
    }
}

fn parse_jj_uri(uri: &str) -> Result<EntityAddress<crate::JjModel>, JjQueryError> {
    let Some((namespace, remainder)) = uri.split_once("://") else {
        return Err(JjQueryError::InvalidUri(uri.to_owned()));
    };
    let Some((kind, id)) = remainder.split_once('/') else {
        return Err(JjQueryError::InvalidUri(uri.to_owned()));
    };
    if namespace != JJ_NAMESPACE || kind.is_empty() || id.is_empty() || id.contains('/') {
        return Err(JjQueryError::InvalidUri(uri.to_owned()));
    }
    Ok(jj_address(kind, id))
}
