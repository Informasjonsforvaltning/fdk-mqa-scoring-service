use std::{collections::HashMap, io::Cursor};

use oxigraph::{
    io::GraphFormat,
    model::{
        vocab::xsd, BlankNode, GraphNameRef, Literal, NamedNode, NamedNodeRef, NamedOrBlankNode,
        Quad, Term,
    },
    store::Store,
};

use crate::{
    error::Error,
    helpers::{execute_query, named_quad_subject},
    measurement_value::MeasurementValue,
    score::{DimensionScore, MetricScore, Score},
    vocab::{dcat, dcat_mqa, dqv, rdf_syntax},
};

pub struct MeasurementGraph(oxigraph::store::Store);

impl MeasurementGraph {
    /// Creates new measurement graph.
    pub fn new() -> Result<Self, Error> {
        let store = Store::new()?;
        Ok(Self(store))
    }

    /// Loads graph from string.
    pub fn load<G: ToString>(&mut self, graph: G) -> Result<(), Error> {
        self.0.load_graph(
            graph.to_string().as_ref(),
            GraphFormat::Turtle,
            GraphNameRef::DefaultGraph,
            None,
        )?;
        Ok(())
    }

    /// Retrieves all named dataset nodes.
    pub fn dataset(&self) -> Result<NamedNode, Error> {
        self.0
            .quads_for_pattern(
                None,
                Some(rdf_syntax::TYPE),
                Some(dcat::DATASET.into()),
                None,
            )
            .map(named_quad_subject)
            .next()
            .unwrap_or(Err("measurement graph has no datasets".into()))
    }

    /// Retrieves all named distribution nodes.
    pub fn distributions(&self) -> Result<Vec<NamedNode>, Error> {
        let mut distributions = self
            .0
            .quads_for_pattern(
                None,
                Some(rdf_syntax::TYPE),
                Some(dcat::DISTRIBUTION_CLASS.into()),
                None,
            )
            .map(named_quad_subject)
            .collect::<Result<Vec<NamedNode>, Error>>()?;
        distributions.sort();
        Ok(distributions)
    }

    /// Retrieves all quality measurements in a graph, as map: (node, metric) -> value.
    pub fn quality_measurements(
        &self,
    ) -> Result<HashMap<(NamedNode, NamedNode), MeasurementValue>, Error> {
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
                    Some(Term::NamedNode(node)) => Ok(node.clone()),
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
    pub fn insert_scores(&mut self, scores: &Vec<Score>) -> Result<(), Error> {
        for Score {
            name: node,
            dimensions,
            score: total_score,
        } in scores
        {
            self.insert_node_score(node.as_ref(), total_score)?;
            for DimensionScore {
                name,
                metrics,
                score: total_score,
            } in dimensions
            {
                self.insert_dimension_score(node.as_ref(), name.as_ref(), total_score)?;
                for metric_score in metrics {
                    self.insert_measurement_score(node.as_ref(), metric_score)?;
                }
            }
        }
        Ok(())
    }

    /// Insert total score of a node into graph.
    fn insert_node_score(&mut self, node: NamedNodeRef, score: &u64) -> Result<(), Error> {
        self.insert_measurement_property(node, dcat_mqa::SCORING, dqv::VALUE, score)
    }

    /// Insert dimension score of a node into graph.
    fn insert_dimension_score(
        &mut self,
        node: NamedNodeRef,
        dimension: NamedNodeRef,
        score: &u64,
    ) -> Result<(), Error> {
        let metric = NamedNode::new(format!("{}Scoring", dimension.as_str()).as_str())?;
        self.insert_measurement_property(node, metric.as_ref(), dqv::VALUE, score)
    }

    /// Insert measurement score into graph.
    fn insert_measurement_score(
        &mut self,
        node: NamedNodeRef,
        metric: &MetricScore,
    ) -> Result<(), Error> {
        self.insert_measurement_property(
            node,
            metric.name.as_ref(),
            dcat_mqa::SCORE,
            &metric.score.unwrap_or_default(),
        )
    }

    /// Insert the value of a metric measurement into graph.
    /// Creates the measurement if it does not exist.
    fn insert_measurement_property(
        &mut self,
        node: NamedNodeRef,
        metric: NamedNodeRef,
        property: NamedNodeRef,
        value: &u64,
    ) -> Result<(), Error> {
        let measurement = match self.get_measurement(node, metric)? {
            Some(node) => node,
            None => self.insert_measurement(node, metric)?,
        };

        let entry = Quad {
            subject: measurement.into(),
            predicate: property.into(),
            object: Literal::new_typed_literal(format!("{}", value), xsd::INTEGER).into(),
            graph_name: GraphNameRef::DefaultGraph.into(),
        };

        self.0.insert(&entry)?;
        Ok(())
    }

    /// Retrieves measurement of metric for node.
    fn get_measurement(
        &mut self,
        node: NamedNodeRef,
        metric: NamedNodeRef,
    ) -> Result<Option<NamedOrBlankNode>, Error> {
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
        node: NamedNodeRef,
        metric: NamedNodeRef,
    ) -> Result<NamedOrBlankNode, Error> {
        let measurement = BlankNode::default();

        self.0.insert(&Quad {
            subject: measurement.clone().into(),
            predicate: rdf_syntax::TYPE.into(),
            object: dqv::QUALITY_MEASUREMENT_CLASS.into(),
            graph_name: GraphNameRef::DefaultGraph.into(),
        })?;
        self.0.insert(&Quad {
            subject: measurement.clone().into(),
            predicate: dqv::IS_MEASUREMENT_OF.into(),
            object: metric.into(),
            graph_name: GraphNameRef::DefaultGraph.into(),
        })?;
        self.0.insert(&Quad {
            subject: measurement.clone().into(),
            predicate: dqv::COMPUTED_ON.into(),
            object: node.into(),
            graph_name: GraphNameRef::DefaultGraph.into(),
        })?;
        self.0.insert(&Quad {
            subject: node.into(),
            predicate: dqv::HAS_QUALITY_MEASUREMENT.into(),
            object: measurement.clone().into(),
            graph_name: GraphNameRef::DefaultGraph.into(),
        })?;

        Ok(NamedOrBlankNode::BlankNode(measurement))
    }

    /// Dump graph to string.
    pub fn to_string(&self) -> Result<String, Error> {
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
                node("https://distribution.a"),
                node("https://distribution.b"),
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
