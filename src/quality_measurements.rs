use crate::{
    error::MqaError,
    helpers::{execute_query, named_or_blank_quad_object, named_or_blank_quad_subject},
    vocab::{dcat, dqv},
};
use oxigraph::{
    model::{vocab::xsd, Literal, NamedNode, NamedOrBlankNode, Term},
    store::Store,
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

/// Retrieves all named or blank dataset nodes.
pub fn datasets(store: &Store) -> Result<Vec<NamedOrBlankNode>, MqaError> {
    store
        .quads_for_pattern(None, Some(dcat::DISTRIBUTION.into()), None, None)
        .map(named_or_blank_quad_subject)
        .collect()
}

/// Retrieves all named or blank distribution nodes.
pub fn distributions(store: &Store) -> Result<Vec<NamedOrBlankNode>, MqaError> {
    store
        .quads_for_pattern(None, Some(dcat::DISTRIBUTION.into()), None, None)
        .map(named_or_blank_quad_object)
        .collect()
}

/// Retrieves all quality measurements in a graph, as map: (node, metric) -> value.
pub fn quality_measurements(
    store: &Store,
) -> Result<HashMap<(NamedOrBlankNode, NamedNode), QualityMeasurementValue>, MqaError> {
    let query = format!(
        "
            SELECT ?node ?metric ?value
            WHERE {{
                ?node {} ?measurement .
                ?measurement {} ?metric .
                ?measurement {} ?value .
            }}
        ",
        dqv::HAS_QUALITY_MEASUREMENT,
        dqv::IS_MEASUREMENT_OF,
        dqv::VALUE
    );
    execute_query(&query, &store)?
        .into_iter()
        .map(|qs| {
            let node = match qs.get("node") {
                Some(Term::NamedNode(node)) => Ok(NamedOrBlankNode::NamedNode(node.clone())),
                Some(Term::BlankNode(node)) => Ok(NamedOrBlankNode::BlankNode(node.clone())),
                _ => Err("unable to get quality measurement node"),
            }?;
            let metric = match qs.get("metric") {
                Some(Term::NamedNode(node)) => Ok(node.clone()),
                _ => Err("unable to get quality measurement metric"),
            }?;
            let value = match qs.get("value") {
                Some(Term::Literal(value)) => Ok(QualityMeasurementValue::from(value.clone())),
                _ => Err("unable to get quality measurement value"),
            }?;
            Ok(((node, metric), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::{
        parse_graphs,
        tests::{nnode, node},
    };

    fn measurement_graph() -> Store {
        parse_graphs(vec![r#"
            <http://dataset.a> <http://www.w3.org/ns/dcat#distribution> _:f9b4fdb9378aa7013a762790b069eb7e .
            <http://dataset.a> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:2e0587e7a28b492755a38437372b2e05 .
            _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:38fc04f528a7eef5b4102f9fdd4b9ab6 .
            _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:972515fe91764948597fbb3beebedc5 .
            _:2e0587e7a28b492755a38437372b2e05 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:2e0587e7a28b492755a38437372b2e05 <http://www.w3.org/ns/dqv#isMeasurementOf> <http://metric.a> .
            _:38fc04f528a7eef5b4102f9fdd4b9ab6 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:38fc04f528a7eef5b4102f9fdd4b9ab6 <http://www.w3.org/ns/dqv#isMeasurementOf> <http://metric.b> .
            _:972515fe91764948597fbb3beebedc5 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:972515fe91764948597fbb3beebedc5 <http://www.w3.org/ns/dqv#isMeasurementOf> <http://metric.c> .
        "#.to_string()]).unwrap()
    }

    #[test]
    fn test_distributions() {
        let graph = measurement_graph();
        let distributions = distributions(&graph).unwrap();
        assert_eq!(distributions.len(), 1);
    }

    #[test]
    fn test_get_measurements() {
        let graph = measurement_graph();

        let measurements = quality_measurements(&graph).unwrap();
        let distribution = distributions(&graph).unwrap().into_iter().next().unwrap();

        assert_eq!(measurements.len(), 3);
        assert_eq!(
            measurements.get(&(node("http://dataset.a"), nnode("http://metric.a"))),
            Some(&QualityMeasurementValue::Bool(true))
        );
        assert_eq!(
            measurements.get(&(distribution.clone(), nnode("http://metric.b"))),
            Some(&QualityMeasurementValue::Bool(false))
        );
        assert_eq!(
            measurements.get(&(distribution.clone(), nnode("http://metric.c"))),
            Some(&QualityMeasurementValue::Bool(true))
        );
    }
}
