use crate::error::MqaError;
use oxigraph::{
    io::GraphFormat,
    model::{GraphNameRef, NamedNode, NamedOrBlankNode, Quad, Subject, Term},
    sparql::{QueryResults, QuerySolution},
    store::{StorageError, Store},
};
use std::{fs, io};

// Executes SPARQL query on store.
pub fn execute_query(q: &str, store: &Store) -> Result<Vec<QuerySolution>, MqaError> {
    match store.query(q) {
        Ok(QueryResults::Solutions(solutions)) => match solutions.collect() {
            Ok(vec) => Ok(vec),
            Err(e) => Err(e.into()),
        },
        Err(e) => Err(e.into()),
        _ => Err("query error".into()),
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
pub fn parse_graphs(graphs: Vec<String>) -> Result<Store, MqaError> {
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
pub fn named_quad_subject(result: Result<Quad, StorageError>) -> Result<NamedNode, MqaError> {
    match result?.subject {
        Subject::NamedNode(node) => Ok(node),
        _ => Err(MqaError::String(
            "unable to get named quad object".to_string(),
        )),
    }
}

// Attemts to extract quad object as named or blank node.
pub fn named_or_blank_quad_object(
    result: Result<Quad, StorageError>,
) -> Result<NamedOrBlankNode, MqaError> {
    match result?.object {
        Term::NamedNode(node) => Ok(NamedOrBlankNode::NamedNode(node)),
        Term::BlankNode(node) => Ok(NamedOrBlankNode::BlankNode(node)),
        _ => Err(MqaError::String(
            "unable to get named or blank quad object".to_string(),
        )),
    }
}
