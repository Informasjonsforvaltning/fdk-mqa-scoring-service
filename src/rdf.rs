use log::{error, info};
use oxigraph::{
    io::GraphFormat,
    model::{
        vocab::xsd, GraphNameRef, Literal, NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef, Term,
    },
    store::{StorageError, Store},
};

use crate::{
    helpers::{named_or_blank_quad_subject, query, QueryError},
    vocab::{dcat, dqv},
};

#[derive(Debug, PartialEq)]
pub enum QualityMeasurementValue {
    Bool(bool),
    Int(i64),
    String(String),
    Unknown(String),
}

impl From<Literal> for QualityMeasurementValue {
    fn from(value: Literal) -> QualityMeasurementValue {
        match value.datatype() {
            xsd::STRING => QualityMeasurementValue::String(value.value().to_string()),
            xsd::BOOLEAN => QualityMeasurementValue::Bool(value.value().to_string() == "true"),
            xsd::INTEGER => QualityMeasurementValue::Int(value.value().parse().unwrap_or(0)),
            _ => QualityMeasurementValue::Unknown(value.value().to_string()),
        }
    }
}

/// Parses Turtle RDF and load into store.
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

/// Retrieves named or blank distributions.
pub fn distributions(store: &Store) -> Result<Vec<NamedOrBlankNode>, StorageError> {
    store
        .quads_for_pattern(None, Some(dcat::DISTRIBUTION.into()), None, None)
        .filter_map(named_or_blank_quad_subject)
        .collect()
}

fn get_quality_measurement(
    distribution: NamedOrBlankNodeRef,
    store: &Store,
) -> Result<Vec<(NamedNode, QualityMeasurementValue)>, QueryError> {
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
    let measurements = query(&q, store)?
        .into_iter()
        .filter_map(|qs| {
            let measurement = qs.get("measurement");
            let value = qs.get("value");
            match (measurement, value) {
                (Some(Term::NamedNode(measurement)), Some(Term::Literal(value))) => Some((
                    measurement.clone(),
                    QualityMeasurementValue::from(value.clone()),
                )),
                _ => None,
            }
        })
        .collect::<Vec<(NamedNode, QualityMeasurementValue)>>();

    Ok(measurements)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::{load_files, parse_graphs};

    fn measurement_graph() -> Store {
        let fnames = vec!["test/measurement_graph.ttl"];
        parse_graphs(load_files(fnames).unwrap()).unwrap()
    }

    #[test]
    fn test_get_measurements() {
        let graph = measurement_graph();
        let distributions = distributions(&graph).unwrap();

        for dist in distributions {
            let measurements = get_quality_measurement(dist.as_ref(), &graph).unwrap();
            assert!(measurements.len() > 0);
            /*for m in measurements {
                println!("{}, {:?}", m.0, m.1);
            }*/
        }
    }
}
