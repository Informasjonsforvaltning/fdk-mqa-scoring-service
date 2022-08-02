use std::{collections::HashMap, io::Cursor};

use chrono::{DateTime, NaiveDateTime, Utc};
use oxigraph::{
    io::GraphFormat,
    model::{
        vocab::xsd, BlankNode, GraphNameRef, Literal, NamedNode, NamedNodeRef, NamedOrBlankNode,
        Quad, Term,
    },
    store::Store,
};
use sophia::{
    graph::{inmem::LightGraph, Graph},
    parser::turtle,
    serializer::{QuadSerializer, Stringifier},
    triple::stream::TripleSource,
};
use sophia_jsonld::JsonLdStringifier;

use crate::{
    error::Error,
    helpers::{execute_query, named_quad_object, named_quad_subject},
    measurement_value::MeasurementValue,
    score::{DimensionScore, MetricScore, Score},
    vocab::{dcat_mqa, dcat_terms, dqv, rdf_syntax},
};

#[derive(Debug, PartialEq)]
pub struct AssessmentNode {
    pub assessment: NamedNode,
    pub resource: NamedNode,
}

pub struct AssessmentGraph(oxigraph::store::Store);

impl AssessmentGraph {
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
    pub fn dataset(&self) -> Result<AssessmentNode, Error> {
        let assessment = self
            .0
            .quads_for_pattern(
                None,
                Some(rdf_syntax::TYPE),
                Some(dcat_mqa::DATASET_ASSESSMENT_CLASS.into()),
                None,
            )
            .map(named_quad_subject)
            .next()
            .unwrap_or(Err("assessment graph has no dataset assessments".into()))?;
        let resource = self.assessment_resource(assessment.as_ref())?;
        Ok(AssessmentNode {
            assessment,
            resource,
        })
    }

    pub fn assessment_resource(&self, assessment: NamedNodeRef) -> Result<NamedNode, Error> {
        self.0
            .quads_for_pattern(
                Some(assessment.into()),
                Some(dcat_mqa::ASSESSMENT_OF),
                None,
                None,
            )
            .map(named_quad_object)
            .next()
            .unwrap_or(Err(format!(
                "assessment graph has no resource that '{}' is assessment of",
                assessment
            )
            .into()))
    }

    /// Retrieves all named distribution assessment nodes.
    pub fn distributions(&self) -> Result<Vec<AssessmentNode>, Error> {
        let distributions = self
            .0
            .quads_for_pattern(
                None,
                Some(rdf_syntax::TYPE),
                Some(dcat_mqa::DISTRIBUTION_ASSESSMENT_CLASS.into()),
                None,
            )
            .map(named_quad_subject)
            .collect::<Result<Vec<NamedNode>, Error>>()?
            .into_iter()
            .map(|assessment| {
                let resource = self.assessment_resource(assessment.as_ref())?;
                Ok(AssessmentNode {
                    assessment,
                    resource,
                })
            })
            .collect::<Result<Vec<AssessmentNode>, Error>>()?;
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
            dcat_mqa::CONTAINS_QUALITY_MEASUREMENT,
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

    /// Inserts modification timestamp.
    pub fn insert_modified_timestmap(&self, timestamp: i64) -> Result<(), Error> {
        let timestamp = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(
                timestamp / 1000,
                ((timestamp % 1000) * 1_000_000) as u32,
            ),
            Utc,
        )
        .format("%Y-%m-%d %H:%M:%S%.f %z")
        .to_string();

        let dataset_assessment = self.dataset()?.assessment;
        self.0.insert(&Quad::new(
            dataset_assessment.as_ref(),
            dcat_terms::MODIFIED,
            Literal::new_typed_literal(timestamp, xsd::DATE_TIME),
            GraphNameRef::DefaultGraph,
        ))?;
        Ok(())
    }

    /// Get modification timestamp.
    pub fn get_modified_timestmap(&self) -> Result<i64, Error> {
        let dataset_assessment = self.dataset()?.assessment;
        let term = match self
            .0
            .quads_for_pattern(
                Some(dataset_assessment.as_ref().into()),
                Some(dcat_terms::MODIFIED),
                None,
                None,
            )
            .next()
        {
            Some(Ok(quad)) => Ok(Some(quad.object)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }?;

        if let Some(Term::Literal(literal)) = term {
            let timestamp = DateTime::parse_from_str(literal.value(), "%Y-%m-%d %H:%M:%S%.f %z")
                .map_err(|e| e.to_string())?
                .timestamp_millis();
            Ok(timestamp)
        } else {
            Err("measurement graph has no modified timestamp".into())
        }
    }

    /// Inserts score into measurement graph.
    pub fn insert_scores(&mut self, scores: &Vec<Score>) -> Result<(), Error> {
        for Score {
            assessment,
            resource,
            dimensions,
            score: total_score,
        } in scores
        {
            self.insert_node_score(assessment.as_ref(), resource.as_ref(), total_score)?;
            for DimensionScore {
                id: name,
                metrics,
                score: total_score,
            } in dimensions
            {
                self.insert_dimension_score(
                    assessment.as_ref(),
                    resource.as_ref(),
                    name.as_ref(),
                    total_score,
                )?;
                for metric_score in metrics {
                    self.insert_measurement_score(
                        assessment.as_ref(),
                        resource.as_ref(),
                        metric_score,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Insert total score of a node into graph.
    fn insert_node_score(
        &mut self,
        assessment: NamedNodeRef,
        computed_on: NamedNodeRef,
        score: &u64,
    ) -> Result<(), Error> {
        self.insert_measurement_property(
            assessment,
            computed_on,
            dcat_mqa::SCORING,
            dqv::VALUE,
            score,
        )
    }

    /// Insert dimension score of a node into graph.
    fn insert_dimension_score(
        &mut self,
        assessment: NamedNodeRef,
        computed_on: NamedNodeRef,
        dimension: NamedNodeRef,
        score: &u64,
    ) -> Result<(), Error> {
        let metric = NamedNode::new(format!("{}Scoring", dimension.as_str()).as_str())?;
        self.insert_measurement_property(
            assessment,
            computed_on,
            metric.as_ref(),
            dqv::VALUE,
            score,
        )
    }

    /// Insert measurement score into graph.
    fn insert_measurement_score(
        &mut self,
        assessment: NamedNodeRef,
        computed_on: NamedNodeRef,
        metric: &MetricScore,
    ) -> Result<(), Error> {
        if let Some(score) = metric.score {
            self.insert_measurement_property(
                assessment,
                computed_on,
                metric.id.as_ref(),
                dcat_mqa::SCORE,
                &score,
            )?;
        }
        Ok(())
    }

    /// Insert the value of a metric measurement into graph.
    /// Creates the measurement if it does not exist.
    fn insert_measurement_property(
        &mut self,
        assessment: NamedNodeRef,
        computed_on: NamedNodeRef,
        metric: NamedNodeRef,
        property: NamedNodeRef,
        value: &u64,
    ) -> Result<(), Error> {
        let measurement = match self.get_measurement(assessment, metric)? {
            Some(node) => node,
            None => self.insert_measurement(assessment, computed_on, metric)?,
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
            dcat_mqa::CONTAINS_QUALITY_MEASUREMENT,
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
        assessment: NamedNodeRef,
        computed_on: NamedNodeRef,
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
            object: computed_on.into(),
            graph_name: GraphNameRef::DefaultGraph.into(),
        })?;
        self.0.insert(&Quad {
            subject: assessment.into(),
            predicate: dcat_mqa::CONTAINS_QUALITY_MEASUREMENT.into(),
            object: measurement.clone().into(),
            graph_name: GraphNameRef::DefaultGraph.into(),
        })?;

        Ok(NamedOrBlankNode::BlankNode(measurement))
    }

    /// Clean content of graph.
    pub fn clear(&mut self) -> Result<(), Error> {
        self.0.clear()?;
        Ok(())
    }

    /// Dump graph to string.
    pub fn to_turtle(&self) -> Result<String, Error> {
        let mut buff = Cursor::new(Vec::new());
        self.0
            .dump_graph(&mut buff, GraphFormat::Turtle, GraphNameRef::DefaultGraph)?;

        String::from_utf8(buff.into_inner()).map_err(|e| e.to_string().into())
    }

    /// Dump graph to json.
    pub fn to_jsonld(&self) -> Result<String, Error> {
        let graph: LightGraph = turtle::parse_str(&self.to_turtle()?)
            .collect_triples()
            .map_err(|e| Error::String(e.to_string()))?;

        let mut serializer = JsonLdStringifier::new_stringifier();
        serializer
            .serialize_dataset(&graph.as_dataset())
            .map_err(|e| Error::String(e.to_string()))?;

        String::from_utf8(serializer.as_utf8().iter().map(|b| b.clone()).collect())
            .map_err(|e| e.to_string().into())
    }
}

#[cfg(test)]
mod tests {
    use tracing::info;

    use super::*;
    use crate::test::{mqa_node, node, MEASUREMENT_GRAPH};

    pub fn measurement_graph() -> AssessmentGraph {
        let mut graph = AssessmentGraph::new().unwrap();
        graph.load(MEASUREMENT_GRAPH).unwrap();
        graph
    }

    #[test]
    fn dataset() {
        let graph = measurement_graph();
        let dataset = graph.dataset().unwrap();
        assert_eq!(
            dataset,
            AssessmentNode {
                assessment: node("https://dataset.assessment.foo"),
                resource: node("https://dataset.foo"),
            }
        );
    }

    #[test]
    fn distributions() {
        let graph = measurement_graph();
        let distributions = graph.distributions().unwrap();
        assert_eq!(
            distributions,
            vec![
                AssessmentNode {
                    assessment: node("https://distribution.assessment.a"),
                    resource: node("https://distribution.a"),
                },
                AssessmentNode {
                    assessment: node("https://distribution.assessment.b"),
                    resource: node("https://distribution.b"),
                },
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
                node("https://dataset.assessment.foo"),
                mqa_node("downloadUrlAvailability")
            )),
            Some(&MeasurementValue::Bool(true))
        );
        assert_eq!(
            measurements.get(&(
                node("https://distribution.assessment.a"),
                mqa_node("accessUrlStatusCode")
            )),
            Some(&MeasurementValue::Int(200))
        );
        assert_eq!(
            measurements.get(&(
                node("https://distribution.assessment.a"),
                mqa_node("formatAvailability")
            )),
            Some(&MeasurementValue::Bool(false))
        );
        assert_eq!(
            measurements.get(&(
                node("https://distribution.assessment.b"),
                mqa_node("formatAvailability")
            )),
            Some(&MeasurementValue::Bool(true))
        );
    }

    #[test]
    fn modification_timestamp() {
        let graph = measurement_graph();
        assert!(graph.get_modified_timestmap().is_err());
        graph.insert_modified_timestmap(1656316912123).unwrap();
        assert!(graph.to_turtle().unwrap().contains("<https://dataset.assessment.foo> <http://purl.org/dc/terms/modified> \"2022-06-27 08:01:52.123 +0000\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."));
        assert_eq!(graph.get_modified_timestmap().unwrap(), 1656316912123);
    }
}
