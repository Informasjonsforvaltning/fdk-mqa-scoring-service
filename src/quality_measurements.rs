use std::collections::HashMap;

use crate::{
    helpers::{named_or_blank_quad_subject, query, StoreError},
    vocab::{dcat, dqv},
};
use oxigraph::{
    model::{vocab::xsd, Literal, NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef, Term},
    store::{StorageError, Store},
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

/// Retrieves named or blank distributions.
pub fn distributions(store: &Store) -> Result<Vec<NamedOrBlankNode>, StoreError> {
    store
        .quads_for_pattern(None, Some(dcat::DISTRIBUTION.into()), None, None)
        .filter_map(named_or_blank_quad_subject)
        .collect::<Result<Vec<NamedOrBlankNode>, StorageError>>()
        .or_else(|e| Err(e.into()))
}

/// Retrieves pairs of quality measurements and their values.
pub fn quality_measurements(
    store: &Store,
    distribution: NamedOrBlankNodeRef,
) -> Result<HashMap<NamedNode, QualityMeasurementValue>, StoreError> {
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
    let measurements = query(&q, &store)?
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
        .collect::<HashMap<NamedNode, QualityMeasurementValue>>();

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
            let measurements = quality_measurements(&graph, dist.as_ref()).unwrap();
            assert!(measurements.len() > 0);
            /*for m in measurements {
                println!("{}, {:?}", m.0, m.1);
            }*/
        }
    }
}
