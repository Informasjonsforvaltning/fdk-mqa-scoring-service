use std::fs;

use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{GraphNameRef, Literal, NamedNode, NamedOrBlankNode, Quad, Term},
    sparql::{QueryResults, QuerySolution, SparqlEvaluator},
    store::{StorageError, Store},
};
use crate::error::Error;

// Executes SPARQL SELECT query on store.
pub fn execute_query(store: &Store, q: &str) -> Result<Vec<QuerySolution>, Error> {
    match SparqlEvaluator::new().parse_query(q)?.on_store(store).execute()? {
        QueryResults::Solutions(solutions) => Ok(solutions.collect::<Result<_, _>>()?),
        _ => Err("unable to execute query, not a SELECT query".into()),
    }
}

/// Extracts a named-node binding from a SPARQL solution.
pub fn named_binding(qs: &QuerySolution, name: &str) -> Result<NamedNode, Error> {
    match qs.get(name) {
        Some(Term::NamedNode(node)) => Ok(node.clone()),
        _ => Err(format!("unable to get named binding '{name}'").into()),
    }
}

/// Extracts a literal binding from a SPARQL solution.
pub fn literal_binding(qs: &QuerySolution, name: &str) -> Result<Literal, Error> {
    match qs.get(name) {
        Some(Term::Literal(literal)) => Ok(literal.clone()),
        _ => Err(format!("unable to get literal binding '{name}'").into()),
    }
}

/// Extracts a named- or blank-node binding from a SPARQL solution.
pub fn named_or_blank_binding(qs: &QuerySolution, name: &str) -> Result<NamedOrBlankNode, Error> {
    match qs.get(name) {
        Some(Term::NamedNode(node)) => Ok(NamedOrBlankNode::NamedNode(node.clone())),
        Some(Term::BlankNode(node)) => Ok(NamedOrBlankNode::BlankNode(node.clone())),
        Some(term) => Err(format!(
            "unable to get named or blank binding '{name}', found: '{term}'"
        )
        .into()),
        None => Err(format!("unable to get named or blank binding '{name}'").into()),
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
pub fn parse_graphs<G: AsRef<[u8]>>(graphs: Vec<G>) -> Result<Store, Error> {
    let store = oxigraph::store::Store::new()?;
    for graph in graphs {
        store.load_from_reader(
            RdfParser::from_format(RdfFormat::Turtle)
                .without_named_graphs()
                .with_default_graph(GraphNameRef::DefaultGraph),
            graph.as_ref(),
        )?;
    }
    Ok(store)
}

// Attemts to extract quad subject as named node.
pub fn named_quad_subject(result: Result<Quad, StorageError>) -> Result<NamedNode, Error> {
    match result?.subject {
        NamedOrBlankNode::NamedNode(node) => Ok(node),
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
