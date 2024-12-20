#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    LoaderError(#[from] oxigraph::store::LoaderError),
    #[error(transparent)]
    StorageError(#[from] oxigraph::store::StorageError),
    #[error(transparent)]
    VariableNameParseError(#[from] oxigraph::sparql::VariableNameParseError),
    #[error(transparent)]
    SparqlSyntaxError(#[from] oxigraph::sparql::SparqlSyntaxError),
    #[error(transparent)]
    IriParseError(#[from] oxigraph::model::IriParseError),
    #[error(transparent)]
    EvaluationError(#[from] oxigraph::sparql::EvaluationError),
    #[error(transparent)]
    SerializerError(#[from] oxigraph::store::SerializerError),
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
    #[error(transparent)]
    RegexError(#[from] regex::Error),
    #[error(transparent)]
    KafkaError(#[from] rdkafka::error::KafkaError),
    #[error(transparent)]
    AvroError(#[from] apache_avro::Error),
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
