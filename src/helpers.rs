use std::{fs, io};

use oxigraph::{
    io::GraphFormat,
    model::{GraphNameRef, NamedNode, NamedOrBlankNode, Quad, Subject, Term},
    sparql::{EvaluationError, QueryError, QueryResults, QuerySolution},
    store::{LoaderError, StorageError, Store},
};

#[derive(Debug)]
pub enum StoreError {
    LoaderError(LoaderError),
    StorageError(StorageError),
    QueryError(QueryError),
    EvaluationError(EvaluationError),
    String(String),
}

impl From<LoaderError> for StoreError {
    fn from(e: LoaderError) -> Self {
        Self::LoaderError(e)
    }
}
impl From<StorageError> for StoreError {
    fn from(e: StorageError) -> Self {
        Self::StorageError(e)
    }
}
impl From<QueryError> for StoreError {
    fn from(e: QueryError) -> Self {
        Self::QueryError(e)
    }
}
impl From<EvaluationError> for StoreError {
    fn from(e: EvaluationError) -> Self {
        Self::EvaluationError(e)
    }
}
impl From<String> for StoreError {
    fn from(e: String) -> Self {
        Self::String(e)
    }
}

// Executes SPARQL query on store.
pub fn execute_query(q: &str, store: &Store) -> Result<Vec<QuerySolution>, StoreError> {
    match store.query(q) {
        Ok(QueryResults::Solutions(solutions)) => match solutions.collect() {
            Ok(vec) => Ok(vec),
            Err(e) => Err(e.into()),
        },
        Err(e) => Err(e.into()),
        _ => Err("query error".to_string().into()),
    }
}

// Loads files from a list of filenames.
pub fn load_files(fnames: Vec<&str>) -> Result<Vec<String>, io::Error> {
    fnames
        .into_iter()
        .map(|fname| fs::read_to_string(fname))
        .collect()
}

// Parses list of turtle graph strings into a single store.
pub fn parse_graphs(graphs: Vec<String>) -> Result<Store, StoreError> {
    let store = oxigraph::store::Store::new()?;
    for graph in graphs {
        store.load_graph(
            graph.as_ref(),
            GraphFormat::Turtle,
            GraphNameRef::DefaultGraph,
            None,
        )?;
    }
    Ok(store)
}

// Attemts to extract quad subject as named node.
pub fn named_quad_subject(result: Result<Quad, StorageError>) -> Result<NamedNode, StoreError> {
    match result?.subject {
        Subject::NamedNode(node) => Ok(node),
        _ => Err(StoreError::String(
            "unable to get named quad object".to_string(),
        )),
    }
}

// Attemts to extract quad object as named or blank node.
pub fn named_or_blank_quad_object(
    result: Result<Quad, StorageError>,
) -> Result<NamedOrBlankNode, StoreError> {
    match result?.object {
        Term::NamedNode(node) => Ok(NamedOrBlankNode::NamedNode(node)),
        Term::BlankNode(node) => Ok(NamedOrBlankNode::BlankNode(node)),
        _ => Err(StoreError::String(
            "unable to get named or blank quad object".to_string(),
        )),
    }
}
