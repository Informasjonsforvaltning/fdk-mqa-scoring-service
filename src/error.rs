use oxigraph::{
    model::IriParseError,
    sparql::{EvaluationError, QueryError},
    store::{LoaderError, SerializerError, StorageError},
};
use thiserror::Error;

use crate::database;

#[derive(Error, Debug)]
pub enum MqaError {
    #[error(transparent)]
    LoaderError(#[from] LoaderError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    QueryError(#[from] QueryError),
    #[error(transparent)]
    IriParseError(#[from] IriParseError),
    #[error(transparent)]
    EvaluationError(#[from] EvaluationError),
    #[error(transparent)]
    SerializerError(#[from] SerializerError),
    #[error(transparent)]
    RegexError(#[from] regex::Error),
    #[error(transparent)]
    KafkaError(#[from] rdkafka::error::KafkaError),
    #[error(transparent)]
    SRCError(#[from] schema_registry_converter::error::SRCError),
    #[error(transparent)]
    DatabaseError(#[from] database::DatabaseError),
    #[error(transparent)]
    PoolError(#[from] deadpool_postgres::PoolError),
    #[error("{0}")]
    String(String),
}

impl From<&str> for MqaError {
    fn from(e: &str) -> Self {
        Self::String(e.to_string())
    }
}

impl From<String> for MqaError {
    fn from(e: String) -> Self {
        Self::String(e)
    }
}
