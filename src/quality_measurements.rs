use crate::{
    error::MqaError,
    helpers::{
        execute_query, named_or_blank_quad_object, named_or_blank_quad_subject, parse_graphs,
    },
    score::{DimensionScore, DistributionScore, MetricScore},
    vocab::{dcat, dcat_mqa, dqv},
};
use oxigraph::{
    io::GraphFormat,
    model::{vocab::xsd, GraphNameRef, Literal, NamedNode, NamedOrBlankNode, Quad, Subject, Term},
};
use regex::Regex;
use std::{collections::HashMap, io::Cursor};

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

pub struct MeasurementGraph(oxigraph::store::Store);

impl MeasurementGraph {
    // Loads graph from string.
    pub fn parse<G: ToString>(graph: G) -> Result<Self, MqaError> {
        let graph = Self::name_blank_nodes(graph.to_string())?;
        parse_graphs(vec![graph]).map(|store| Self(store))
    }

    fn name_blank_nodes(graph: String) -> Result<String, MqaError> {
        let re = Regex::new(r"_:(?P<id>[0-9a-f]+) ");
        match re {
            Ok(re) => Ok(re
                .replace_all(&graph, "<https://blank.node#${id}> ")
                .to_string()),
            Err(e) => Err(e.to_string().into()),
        }
    }

    /// Retrieves all named or blank dataset nodes.
    pub fn dataset(&self) -> Result<NamedOrBlankNode, MqaError> {
        self.0
            .quads_for_pattern(None, Some(dcat::DISTRIBUTION.into()), None, None)
            .map(named_or_blank_quad_subject)
            .next()
            .unwrap_or(Err(MqaError::from("store has no dataset")))
    }

    /// Retrieves all named or blank distribution nodes.
    pub fn distributions(&self) -> Result<Vec<NamedOrBlankNode>, MqaError> {
        self.0
            .quads_for_pattern(None, Some(dcat::DISTRIBUTION.into()), None, None)
            .map(named_or_blank_quad_object)
            .collect()
    }

    /// Retrieves all quality measurements in a graph, as map: (node, metric) -> value.
    pub fn quality_measurements(
        &self,
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
        execute_query(&self.0, &query)?
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

    pub fn insert_scores(
        &mut self,
        distributions: &Vec<DistributionScore>,
    ) -> Result<(), MqaError> {
        for DistributionScore(distribution, dimensions) in distributions {
            for DimensionScore(_, metrics) in dimensions {
                for MetricScore(metric, score) in metrics {
                    let value = score.unwrap_or(0);
                    let q = format!(
                        "
                            SELECT ?measurement
                            WHERE {{
                                {{
                                    ?measurement {} {metric} .
                                    ?measurement {} {distribution} .
                                }}
                            
                            }}
                        ",
                        dqv::IS_MEASUREMENT_OF,
                        dqv::COMPUTED_ON,
                    );
                    // Measurement of type metric might not exist in graph
                    if let Some(qs) = execute_query(&self.0, &q)?.first() {
                        let measurement = match qs.get("measurement") {
                            Some(Term::NamedNode(node)) => {
                                Ok(NamedOrBlankNode::NamedNode(node.clone()))
                            }
                            Some(Term::BlankNode(node)) => {
                                Ok(NamedOrBlankNode::BlankNode(node.clone()))
                            }
                            _ => Err(format!(
                                "unable to get measurement when inserting score: {}",
                                metric
                            )),
                        }?;

                        self.0.insert(
                            &Quad::new(
                                Subject::from(measurement),
                                NamedNode::from(dcat_mqa::TRUE_SCORE),
                                Term::Literal(Literal::new_typed_literal(
                                    format!("{}", value),
                                    xsd::INTEGER,
                                )),
                                GraphNameRef::DefaultGraph,
                            )
                            .into(),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn to_string(&self) -> Result<String, MqaError> {
        let mut buff = Cursor::new(Vec::new());
        self.0
            .dump_graph(&mut buff, GraphFormat::Turtle, GraphNameRef::DefaultGraph)?;

        match String::from_utf8(buff.into_inner()) {
            Ok(str) => Ok(str),
            Err(e) => Err(e.to_string().into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        helpers::tests::{mqa_node, node},
        test::MEASUREMENT_GRAPH,
    };

    fn measurement_graph() -> MeasurementGraph {
        MeasurementGraph::parse(MEASUREMENT_GRAPH).unwrap()
    }

    #[test]
    fn test_distributions() {
        let graph = measurement_graph();
        let distributions = graph.distributions().unwrap();
        assert_eq!(
            distributions,
            vec![
                node("https://distribution.b"),
                node("https://distribution.a")
            ]
        );
    }

    #[test]
    fn test_get_measurements() {
        let graph = measurement_graph();
        let measurements = graph.quality_measurements().unwrap();

        assert_eq!(measurements.len(), 4);
        assert_eq!(
            measurements.get(&(
                node("https://dataset.foo"),
                mqa_node("downloadUrlAvailability")
            )),
            Some(&QualityMeasurementValue::Bool(true))
        );
        assert_eq!(
            measurements.get(&(
                node("https://distribution.a"),
                mqa_node("accessUrlStatusCode")
            )),
            Some(&QualityMeasurementValue::Bool(true))
        );
        assert_eq!(
            measurements.get(&(
                node("https://distribution.a"),
                mqa_node("formatAvailability")
            )),
            Some(&QualityMeasurementValue::Bool(false))
        );
        assert_eq!(
            measurements.get(&(
                node("https://distribution.b"),
                mqa_node("formatAvailability")
            )),
            Some(&QualityMeasurementValue::Bool(true))
        );
    }
}
