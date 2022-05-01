use oxigraph::{
    sparql::{EvaluationError, QueryError},
    store::{LoaderError, StorageError},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MqaError {
    #[error(transparent)]
    LoaderError(#[from] LoaderError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    QueryError(#[from] QueryError),
    #[error(transparent)]
    EvaluationError(#[from] EvaluationError),
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
