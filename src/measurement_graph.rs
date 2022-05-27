use crate::{
    error::MqaError,
    helpers::{execute_query, named_or_blank_quad_object, named_or_blank_quad_subject},
    measurement_value::MeasurementValue,
    score::{DimensionScore, MetricScore, Score},
    vocab::{dcat, dcat_mqa, dqv, rdf_syntax},
};
use oxigraph::{
    io::GraphFormat,
    model::{
        vocab::xsd, BlankNode, GraphNameRef, Literal, NamedNode, NamedNodeRef, NamedOrBlankNode,
        NamedOrBlankNodeRef, Quad, Term,
    },
    store::Store,
};
use std::{collections::HashMap, io::Cursor};

pub struct MeasurementGraph(oxigraph::store::Store);

impl MeasurementGraph {
    /// Creates new measurement graph.
    pub fn new() -> Result<Self, MqaError> {
        let store = Store::new()?;
        Ok(Self(store))
    }

    /// Loads graph from string.
    pub fn load<G: ToString>(&mut self, graph: G) -> Result<(), MqaError> {
        self.0.load_graph(
            graph.to_string().as_ref(),
            GraphFormat::Turtle,
            GraphNameRef::DefaultGraph,
            None,
        )?;
        Ok(())
    }

    /// Retrieves all named or blank dataset nodes.
    pub fn dataset(&self) -> Result<NamedOrBlankNode, MqaError> {
        self.0
            .quads_for_pattern(
                None,
                Some(rdf_syntax::TYPE),
                Some(dcat::DATASET.into()),
                None,
            )
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
    pub fn insert_scores(&mut self, scores: &Vec<Score>) -> Result<(), MqaError> {
        for Score(node, dimensions) in scores {
            self.insert_node_score(node.as_ref(), dimensions)?;
            for DimensionScore(dimension, metrics) in dimensions {
                self.insert_dimension_score(node.as_ref(), dimension.as_ref(), metrics)?;
                for metric_score in metrics {
                    self.insert_measurement_score(node.as_ref(), metric_score)?;
                }
            }
        }
        Ok(())
    }

    /// Insert total score of a node into graph.
    fn insert_node_score(
        &mut self,
        node: NamedOrBlankNodeRef,
        dimensions: &Vec<DimensionScore>,
    ) -> Result<(), MqaError> {
        let sum = dimensions
            .iter()
            .map(|DimensionScore(_, metrics_scores)| {
                metrics_scores
                    .iter()
                    .filter_map(|MetricScore(_, score)| score.clone())
                    .sum::<u64>()
            })
            .sum::<u64>();

        self.insert_measurement_property(node, dcat_mqa::SCORING, dqv::VALUE, sum)
    }

    /// Insert dimension score of a node into graph.
    fn insert_dimension_score(
        &mut self,
        node: NamedOrBlankNodeRef,
        dimension: NamedNodeRef,
        metrics: &Vec<MetricScore>,
    ) -> Result<(), MqaError> {
        let metric = NamedNode::new(format!("{}Scoring", dimension.as_str()).as_str())?;
        let sum = metrics
            .iter()
            .filter_map(|MetricScore(_, score)| score.clone())
            .sum::<u64>();

        self.insert_measurement_property(node, metric.as_ref(), dqv::VALUE, sum)
    }

    /// Insert measurement score into graph.
    fn insert_measurement_score(
        &mut self,
        node: NamedOrBlankNodeRef,
        metric_score: &MetricScore,
    ) -> Result<(), MqaError> {
        let MetricScore(metric, score) = metric_score;

        self.insert_measurement_property(
            node,
            metric.as_ref(),
            dcat_mqa::SCORE,
            score.unwrap_or_default(),
        )
    }

    /// Insert the value of a metric measurement into graph.
    /// Creates the measurement if it does not exist.
    fn insert_measurement_property(
        &mut self,
        node: NamedOrBlankNodeRef,
        metric: NamedNodeRef,
        property: NamedNodeRef,
        value: u64,
    ) -> Result<(), MqaError> {
        let measurement = match self.get_measurement(node, metric)? {
            Some(node) => node,
            None => self.insert_measurement(node, metric)?,
        };

        let entry = Quad {
            subject: measurement.into(),
            predicate: property.into(),
            object: Literal::new_typed_literal(format! {"{}", value}, xsd::INTEGER).into(),
            graph_name: GraphNameRef::DefaultGraph.into(),
        };

        self.0.insert(&entry)?;
        Ok(())
    }

    /// Retrieves measurement of metric for node.
    fn get_measurement(
        &mut self,
        node: NamedOrBlankNodeRef,
        metric: NamedNodeRef,
    ) -> Result<Option<NamedOrBlankNode>, MqaError> {
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
                Some(Some(Term::NamedNode(node))) => {
                    Ok(Some(NamedOrBlankNode::NamedNode(node.clone())))
                }
                Some(Some(Term::BlankNode(node))) => {
                    Ok(Some(NamedOrBlankNode::BlankNode(node.clone())))
                }
                Some(Some(term)) => {
                    Err(format!("unable to get measurement, found: '{}'", term).into())
                }
                _ => Err("unable to get measurement".into()),
            },
            _ => Ok(None),
        }
    }

    /// Inserts measurement of metric for node.
    fn insert_measurement(
        &mut self,
        node: NamedOrBlankNodeRef,
        metric: NamedNodeRef,
    ) -> Result<NamedOrBlankNode, MqaError> {
        let measurement = BlankNode::default();
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

        Ok(NamedOrBlankNode::BlankNode(measurement))
    }

    /// Dump graph to string.
    pub fn to_string(&self) -> Result<String, MqaError> {
        let mut buff = Cursor::new(Vec::new());
        self.0
            .dump_graph(&mut buff, GraphFormat::Turtle, GraphNameRef::DefaultGraph)?;

        String::from_utf8(buff.into_inner()).map_err(|e| e.to_string().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{mqa_node, node, MEASUREMENT_GRAPH};

    pub fn measurement_graph() -> MeasurementGraph {
        let mut graph = MeasurementGraph::new().unwrap();
        graph.load(MEASUREMENT_GRAPH).unwrap();
        graph
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
