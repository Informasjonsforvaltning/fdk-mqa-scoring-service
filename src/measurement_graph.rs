use crate::{
    error::MqaError,
    helpers::{
        execute_query, named_or_blank_quad_object, named_or_blank_quad_subject, parse_graphs,
    },
    measurement_value::MeasurementValue,
    score::{DimensionScore, DistributionScore, MetricScore},
    vocab::{dcat, dcat_mqa, dqv, rdf_syntax},
};
use oxigraph::{
    io::GraphFormat,
    model::{
        vocab::xsd, BlankNode, BlankNodeRef, GraphNameRef, Literal, NamedNode, NamedNodeRef,
        NamedOrBlankNode, NamedOrBlankNodeRef, Quad, Term,
    },
};
use rand::Rng;
use regex::Regex;
use std::{collections::HashMap, io::Cursor};

pub struct MeasurementGraph(oxigraph::store::Store);

impl MeasurementGraph {
    /// Loads graph from string.
    pub fn parse<G: ToString>(graph: G) -> Result<Self, MqaError> {
        let graph = Self::name_blank_nodes(graph.to_string())?;
        parse_graphs(vec![graph]).map(|store| Self(store))
    }

    /// Replaces all blank nodes with named nodes.
    /// Enables SPARQL query with (previously) blank nodes as identifiers.
    fn name_blank_nodes(graph: String) -> Result<String, MqaError> {
        let replaced = Regex::new(r"_:(?P<id>[0-9a-f]+) ")
            .map(|re| re.replace_all(&graph, "<https://blank.node#${id}> "))?;
        Ok(replaced.to_string())
    }

    // Undoes replacement of all blank nodes with named nodes.
    fn undo_name_blank_nodes(graph: String) -> Result<String, MqaError> {
        let replaced = Regex::new(r"<https://blank.node#(?P<id>[0-9a-f]+)> ")
            .map(|re| re.replace_all(&graph, "_:${id} "))?;
        Ok(replaced.to_string())
    }

    /// Retrieves all named or blank dataset nodes.
    pub fn dataset(&self) -> Result<NamedOrBlankNode, MqaError> {
        self.0
            .quads_for_pattern(None, Some(dcat::DISTRIBUTION.into()), None, None)
            .map(named_or_blank_quad_subject)
            .next()
            .unwrap_or(Err(MqaError::from("measurement graph has no datasets")))
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
    ) -> Result<HashMap<(NamedOrBlankNode, NamedNode), MeasurementValue>, MqaError> {
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
                    Some(Term::Literal(value)) => MeasurementValue::try_from(value.clone()),
                    _ => Err("unable to get quality measurement value".into()),
                }?;
                Ok(((node, metric), value))
            })
            .collect()
    }

    /// Inserts score into measurement graph.
    /// The node in DistributionScore may be a dataset node, when inserting scores of a dataset.
    pub fn insert_scores(
        &mut self,
        distributions: &Vec<DistributionScore>,
    ) -> Result<(), MqaError> {
        for DistributionScore(distribution, dimensions) in distributions {
            for DimensionScore(dimension, metrics) in dimensions {
                self.insert_dimension_score(distribution.as_ref(), dimension.as_ref(), metrics)?;
                self.insert_measurement_scores(distribution.as_ref(), metrics)?;
            }
        }
        Ok(())
    }

    /// Insert a distribution's dimension score into graph.
    fn insert_dimension_score(
        &mut self,
        distribution: NamedOrBlankNodeRef,
        dimension: NamedNodeRef,
        metrics: &Vec<MetricScore>,
    ) -> Result<(), MqaError> {
        let metric = NamedNode::new(
            format!(
                "{}Scoring",
                dimension.as_str().replace("<", "").replace(">", "")
            )
            .as_str(),
        )?;
        let measurement = self.get_or_insert_measurement(distribution, metric.as_ref())?;
        let sum = metrics
            .iter()
            .filter_map(|MetricScore(_, score)| score.clone())
            .sum::<u64>();

        let entry = Quad {
            subject: measurement.into(),
            predicate: dqv::VALUE.into(),
            object: Literal::new_typed_literal(format! {"{}", sum}, xsd::INTEGER).into(),
            graph_name: GraphNameRef::DefaultGraph.into(),
        };

        self.0.insert(&entry)?;
        Ok(())
    }

    /// Insert a distribution's measurement scores into graph.
    fn insert_measurement_scores(
        &mut self,
        distribution: NamedOrBlankNodeRef,
        metrics: &Vec<MetricScore>,
    ) -> Result<(), MqaError> {
        for MetricScore(metric, score) in metrics {
            let metric = NamedNode::new(
                format!(
                    "{}Scoring",
                    metric.as_str().replace("<", "").replace(">", "")
                )
                .as_str(),
            )?;

            let measurement = self.get_or_insert_measurement(distribution, metric.as_ref())?;

            let score =
                Literal::new_typed_literal(format! {"{}", score.unwrap_or_default()}, xsd::INTEGER);

            let entry = Quad {
                subject: measurement.into(),
                predicate: dqv::VALUE.into(),
                object: score.into(),
                graph_name: GraphNameRef::DefaultGraph.into(),
            };
            self.0.insert(&entry)?;
        }

        Ok(())
    }

    /// Retrieves measurement of metric for node.
    /// If no such measurement exists, one is created.
    fn get_or_insert_measurement(
        &mut self,
        node: NamedOrBlankNodeRef,
        metric: NamedNodeRef,
    ) -> Result<NamedOrBlankNode, MqaError> {
        let q = format!(
            "
                SELECT ?measurement
                WHERE {{
                    {node} {} ?measurement .
                    ?measurement {} {metric} .
                }}
            ",
            dqv::HAS_QUALITY_MEASUREMENT,
            dqv::IS_MEASUREMENT_OF,
        );
        let result = execute_query(&self.0, &q)?.into_iter().next();
        match result {
            Some(qs) => match qs.values().first() {
                Some(Some(Term::NamedNode(node))) => Ok(NamedOrBlankNode::NamedNode(node.clone())),
                Some(Some(Term::BlankNode(node))) => Ok(NamedOrBlankNode::BlankNode(node.clone())),
                Some(Some(term)) => {
                    Err(format!("unable to get measurement, found: '{}'", term).into())
                }
                _ => Err("unable to get measurement".into()),
            },
            _ => self.insert_measurement(node, metric),
        }
    }

    /// Inserts measurement of metric for node.
    fn insert_measurement(
        &mut self,
        node: NamedOrBlankNodeRef,
        metric: NamedNodeRef,
    ) -> Result<NamedOrBlankNode, MqaError> {
        let mut rng = rand::thread_rng();
        let id: u64 = rng.gen_range(100_000_000..1_000_000_000);
        let measurement = NamedNode::new(format!("https://blank.node#{:x}", id))?;

        let q = format!(
            "
                INSERT DATA {{
                    {measurement} {} {} ;
                                  {} {metric} ;
                                  {} {node} .
                    {node} {} {measurement} .
                }}
            ",
            rdf_syntax::TYPE,
            dqv::QUALITY_MEASUREMENT_CLASS,
            dqv::IS_MEASUREMENT_OF,
            dqv::COMPUTED_ON,
            dqv::HAS_QUALITY_MEASUREMENT,
        );
        self.0.update(&q)?;

        Ok(NamedOrBlankNode::NamedNode(measurement))
    }

    /// Dump graph to string.
    pub fn to_string(&self) -> Result<String, MqaError> {
        let mut buff = Cursor::new(Vec::new());
        self.0
            .dump_graph(&mut buff, GraphFormat::Turtle, GraphNameRef::DefaultGraph)?;

        match String::from_utf8(buff.into_inner()) {
            Ok(str) => MeasurementGraph::undo_name_blank_nodes(str),
            Err(e) => Err(e.to_string().into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{mqa_node, node, MEASUREMENT_GRAPH};

    pub fn measurement_graph() -> MeasurementGraph {
        MeasurementGraph::parse(MEASUREMENT_GRAPH).unwrap()
    }

    #[test]
    fn dataset() {
        let graph = measurement_graph();
        let dataset = graph.dataset().unwrap();
        assert_eq!(dataset, node("https://dataset.foo"));
    }

    #[test]
    fn distributions() {
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
    fn get_measurements() {
        let graph = measurement_graph();
        let measurements = graph.quality_measurements().unwrap();

        assert_eq!(measurements.len(), 4);
        assert_eq!(
            measurements.get(&(
                node("https://dataset.foo"),
                mqa_node("downloadUrlAvailability")
            )),
            Some(&MeasurementValue::Bool(true))
        );
        assert_eq!(
            measurements.get(&(
                node("https://distribution.a"),
                mqa_node("accessUrlStatusCode")
            )),
            Some(&MeasurementValue::Bool(true))
        );
        assert_eq!(
            measurements.get(&(
                node("https://distribution.a"),
                mqa_node("formatAvailability")
            )),
            Some(&MeasurementValue::Bool(false))
        );
        assert_eq!(
            measurements.get(&(
                node("https://distribution.b"),
                mqa_node("formatAvailability")
            )),
            Some(&MeasurementValue::Bool(true))
        );
    }
}
