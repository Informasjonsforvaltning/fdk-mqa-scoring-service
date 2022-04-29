use crate::{
    helpers::{query, StoreError, named_or_blank_quad_object},
    vocab::{dcat, dqv},
};
use oxigraph::{
    model::{vocab::xsd, Literal, NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef, Term},
    store::{StorageError, Store},
};
use std::collections::HashMap;

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

/// Retrieves all named or blank distributions.
pub fn distributions(store: &Store) -> Result<Vec<NamedOrBlankNode>, StoreError> {
    store
        .quads_for_pattern(None, Some(dcat::DISTRIBUTION.into()), None, None)
        .filter_map(named_or_blank_quad_object)
        .collect::<Result<Vec<NamedOrBlankNode>, StorageError>>()
        .or_else(|e| Err(e.into()))
}

/// Retrieves all pairs of quality measurements and their values, within a distribution.
/// ```
/// <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:c0cc2452ef89d2b1343d07254497828e .
/// _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
/// _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
/// _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#keywordAvailability> .
/// _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
/// ```
pub fn quality_measurements(
    store: &Store,
    node: NamedOrBlankNodeRef,
) -> Result<HashMap<NamedNode, QualityMeasurementValue>, StoreError> {
    let q = format!(
        "
            SELECT ?metric ?value
            WHERE {{
                {node} {} ?measurement .
                ?measurement {} ?metric .
                ?measurement {} ?value .
            }}
        ",
        dqv::HAS_QUALITY_MEASUREMENT,
        dqv::IS_MEASUREMENT_OF,
        dqv::VALUE
    );
    let query_result = query(&q, &store)?;
    Ok(query_result
        .into_iter()
        .filter_map(|qs| {
            match (qs.get("measurement"), qs.get("value")) {
                (Some(Term::NamedNode(measurement)), Some(Term::Literal(value))) => Some((
                    measurement.clone(),
                    QualityMeasurementValue::from(value.clone()),
                )),
                _ => None,
            }
        })
        .collect::<HashMap<NamedNode, QualityMeasurementValue>>())
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
