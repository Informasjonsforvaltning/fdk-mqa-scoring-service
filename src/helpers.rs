use std::{fs, io};

use oxigraph::{
    io::GraphFormat,
    model::{GraphNameRef, NamedNode, NamedOrBlankNode, Quad, Subject, Term},
    sparql::{EvaluationError, QueryResults, QuerySolution},
    store::{LoaderError, StorageError, Store},
};

#[derive(Debug)]
pub enum QueryError {
    EvalError(EvaluationError),
    Msg(String),
}

pub fn query(q: &str, store: &Store) -> Result<Vec<QuerySolution>, QueryError> {
    let result = store.query(q);
    match result {
        Ok(QueryResults::Solutions(solutions)) => match solutions.collect() {
            Ok(vec) => Ok(vec),
            Err(e) => Err(QueryError::EvalError(e)),
        },
        Err(e) => Err(QueryError::EvalError(e)),
        _ => Err(QueryError::Msg("query errro".to_string())),
    }
}

pub fn load_files(fnames: Vec<&str>) -> Result<Vec<String>, io::Error> {
    fnames
        .into_iter()
        .map(|fname| fs::read_to_string(fname))
        .collect()
}

pub fn parse_graphs(graphs: Vec<String>) -> Result<Store, LoaderError> {
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

pub fn named_or_blank_quad_subject(
    result: Result<Quad, StorageError>,
) -> Option<Result<NamedOrBlankNode, StorageError>> {
    match result {
        Ok(quad) => match quad.subject {
            Subject::NamedNode(node) => Some(Ok(NamedOrBlankNode::NamedNode(node))),
            Subject::BlankNode(node) => Some(Ok(NamedOrBlankNode::BlankNode(node))),
            _ => None,
        },
        Err(e) => Some(Err(e)),
    }
}

pub fn named_quad_subject(
    result: Result<Quad, StorageError>,
) -> Option<Result<NamedNode, StorageError>> {
    match result {
        Ok(quad) => match quad.subject {
            Subject::NamedNode(node) => Some(Ok(node)),
            _ => None,
        },
        Err(e) => Some(Err(e)),
    }
}

pub fn named_or_blank_quad_object(
    result: Result<Quad, StorageError>,
) -> Option<Result<NamedOrBlankNode, StorageError>> {
    match result {
        Ok(quad) => match quad.object {
            Term::NamedNode(node) => Some(Ok(NamedOrBlankNode::NamedNode(node))),
            Term::BlankNode(node) => Some(Ok(NamedOrBlankNode::BlankNode(node))),
            _ => None,
        },
        Err(e) => Some(Err(e)),
    }
}

pub fn named_quad_object(
    result: Result<Quad, StorageError>,
) -> Option<Result<NamedNode, StorageError>> {
    match result {
        Ok(quad) => match quad.object {
            Term::NamedNode(node) => Some(Ok(node)),
            _ => None,
        },
        Err(e) => Some(Err(e)),
    }
}
