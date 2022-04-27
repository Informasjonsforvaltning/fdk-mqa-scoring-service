use log::{error, info};

use oxigraph::io::GraphFormat;
use oxigraph::model::vocab::xsd;
use oxigraph::model::{
    BlankNodeRef, GraphNameRef, Literal, NamedNode, NamedNodeRef, NamedOrBlankNodeRef, Term, LiteralRef, TermRef,
};
use oxigraph::sparql::{EvaluationError, QueryResults, QuerySolution};
use oxigraph::store::{QuadIter, StorageError, Store};
use rand::distributions;

use crate::vocab::{dcat, dqv};

#[derive(Debug, PartialEq)]
pub enum QualityMeasurementValue {
    Bool(bool),
    Int(i32),
    String(String),
}

// Parse Turtle RDF and load into store
pub fn parse_turtle(turtle: String) -> Result<Store, StorageError> {
    info!("Loading turtle graph");

    let store = Store::new()?;
    match store.load_graph(
        turtle.as_ref(),
        GraphFormat::Turtle,
        GraphNameRef::DefaultGraph,
        None,
    ) {
        Ok(_) => info!("Graph loaded successfully"),
        Err(e) => error!("Loading graph failed {}", e),
    }

    Ok(store)
}

// Retrieve distributions of a dataset
pub fn list_distributions(store: &Store) -> QuadIter {
    store.quads_for_pattern(None, Some(dcat::DISTRIBUTION.into()), None, None)
}

#[derive(Debug)]
enum QueryError {
    EvalError(EvaluationError),
    Msg(String),
}

fn query(
    q: &str,
    store: &Store,
) -> Result<Vec<QuerySolution>, QueryError> {
    let result = store.query(q);
    match result {
        Ok(QueryResults::Solutions(solutions)) => match solutions.collect() {
            Ok(vec) => Ok(vec),
            Err(e) => Err(QueryError::EvalError(e)),
        },
        Err(e) => Err(QueryError::EvalError(e)),
        _ => Err(QueryError::Msg("".to_string())),
    }
}

fn query_measurement_values(
    distribution: NamedOrBlankNodeRef,
    store: &Store,
) -> Result<Vec<QuerySolution>, QueryError> {
    let q = format!(
        "
            SELECT ?measurement ?value
            WHERE {{
                {distribution} {} ?m .
                ?m {} ?measurement .
                ?m {} ?value .
            }}
        ",
        dqv::HAS_QUALITY_MEASUREMENT,
        dqv::IS_MEASUREMENT_OF,
        dqv::VALUE
    );
    query(&q, store)
}

fn get_quality_measurement(
    measurement: NamedOrBlankNodeRef,
    store: &Store,
) -> Result<Vec<(NamedNode, Literal)>,QueryError>{
    let measurements_query = query_measurement_values(measurement, store)?;
    let measurements = measurements_query.into_iter().filter_map(|qs| {
        let measurement = qs.get("measurement");
        let value = qs.get("value");
        match (measurement, value) {
            (Some(Term::NamedNode(measurement)), Some(Term::Literal(value))) => {
                Some((measurement.clone(), value.clone()))
            }
            _ => None,
        }
    }).collect::<Vec<(NamedNode, Literal)>>();

    Ok(measurements)

    /*match (measurement_quad, value_quad) {
        (Some(Ok(Quad {object: Term::NamedNode(n), ..})),
        Some(Ok(Quad {object: Term::Literal(v), ..}))) => (n, convert_literal_to_quality_measurement_value(v)),
        _ => (),
    }*/
}

fn convert_literal_to_quality_measurement_value(value: Literal) -> Option<QualityMeasurementValue> {
    match value.datatype() {
        xsd::STRING => Some(QualityMeasurementValue::String(value.value().to_string())),
        xsd::BOOLEAN => Some(QualityMeasurementValue::Bool(
            value.value().to_string() == "true",
        )),
        xsd::INTEGER => Some(QualityMeasurementValue::Int(
            value.value().parse().unwrap_or(0),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use oxigraph::model::NamedOrBlankNode;

    use crate::store::{load_files, named_or_blank_quad_subject, parse_graphs};

    use super::*;

    fn measurement_graph() -> Store {
        let fnames = vec!["test/measurement_graph.ttl"];
        parse_graphs(load_files(fnames).unwrap()).unwrap()
    }

    #[test]
    fn test_store() {
        let graph = measurement_graph();
        let distributions = list_distributions(&graph)
            .filter_map(named_or_blank_quad_subject)
            .collect::<Result<Vec<NamedOrBlankNode>, StorageError>>()
            .unwrap();

        for dist in distributions {
            println!("{}", dist);
            let measurements = get_quality_measurement(dist.as_ref(), &graph).unwrap();
            for m in measurements {
                println!("{}, {}", m.0, m.1);
            }
        }
    }
}
