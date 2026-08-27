use postcard::Error;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum CodecError {
    #[error("could not encode provenance record: {0}")]
    Encode(#[source] Error),
    #[error("could not decode provenance record: {0}")]
    Decode(#[source] Error),
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    postcard::to_allocvec(value).map_err(CodecError::Encode)
}

pub(crate) fn decode<T: DeserializeOwned>(payload: &[u8]) -> Result<T, CodecError> {
    postcard::from_bytes(payload).map_err(CodecError::Decode)
}
