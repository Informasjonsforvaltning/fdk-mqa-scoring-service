use oxigraph::{
    model::IriParseError,
    sparql::{EvaluationError, QueryError},
    store::{LoaderError, SerializerError, StorageError},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
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
    SerdeError(#[from] serde_json::Error),
    #[error(transparent)]
    RegexError(#[from] regex::Error),
    #[error(transparent)]
    KafkaError(#[from] rdkafka::error::KafkaError),
    #[error(transparent)]
    AvroError(#[from] avro_rs::Error),
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error(transparent)]
    SRCError(#[from] schema_registry_converter::error::SRCError),
    #[error("{0}")]
    String(String),
}

impl From<&str> for Error {
    fn from(e: &str) -> Self {
        Self::String(e.to_string())
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Self::String(e)
    }
}
