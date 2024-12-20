use std::fs;

use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{GraphNameRef, NamedNode, Quad, Subject, Term},
    sparql::{QueryResults, QuerySolution},
    store::{StorageError, Store},
};
use crate::error::Error;

// Executes SPARQL SELECT query on store.
pub fn execute_query(store: &Store, q: &str) -> Result<Vec<QuerySolution>, Error> {
    match store.query(q) {
        Ok(QueryResults::Solutions(solutions)) => Ok(solutions.collect::<Result<_, _>>()?),
        Ok(_) => Err("unable to execute query, not a SELECT query".into()),
        Err(e) => Err(e.into()),
    }
}

// Loads files from a list of filenames.
pub fn load_files(fnames: Vec<&str>) -> Result<Vec<String>, Error> {
    fnames
        .into_iter()
        .map(|fname| fs::read_to_string(fname).map_err(|e| StorageError::Io(e).into()))
        .collect()
}

// Parses list of turtle graph strings into a single store.
pub fn parse_graphs<G: ToString>(graphs: Vec<G>) -> Result<Store, Error> {
    let store = oxigraph::store::Store::new()?;
    for graph in graphs {
        store.load_from_reader(
            RdfParser::from_format(RdfFormat::Turtle)
                .without_named_graphs()
                .with_default_graph(GraphNameRef::DefaultGraph),
            graph.to_string().as_bytes().as_ref()
        )?;
    }
    Ok(store)
}

// Attemts to extract quad subject as named node.
pub fn named_quad_subject(result: Result<Quad, StorageError>) -> Result<NamedNode, Error> {
    match result?.subject {
        Subject::NamedNode(node) => Ok(node),
        _ => Err("unable to get named quad subject".into()),
    }
}

// Attemts to extract quad object as named node.
pub fn named_quad_object(result: Result<Quad, StorageError>) -> Result<NamedNode, Error> {
    match result?.object {
        Term::NamedNode(node) => Ok(node),
        _ => Err("unable to get named quad object".into()),
    }
}
