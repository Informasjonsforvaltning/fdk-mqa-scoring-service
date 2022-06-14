use oxigraph::model::{vocab::rdf, NamedNode, NamedNodeRef, Term};

use crate::{
    error::Error,
    helpers::execute_query,
    helpers::{named_quad_subject, parse_graphs},
    measurement_value::MeasurementValue,
    vocab::{dcat_mqa, dqv},
};

pub static VOCAB_GRAPH: &str = include_str!("../graphs/dcatno-mqa-vocabulary.ttl");
pub static SCORE_GRAPH: &str =
    include_str!("../graphs/dcatno-mqa-vocabulary-default-score-values.ttl");

pub struct ScoreGraph(pub oxigraph::store::Store);

#[derive(Debug, PartialEq)]
pub struct ScoreDefinitions {
    pub dimensions: Vec<ScoreDimension>,
    pub total_score: u64,
}

#[derive(Debug, PartialEq)]
pub struct ScoreDimension {
    pub name: NamedNode,
    pub metrics: Vec<ScoreMetric>,
    pub total_score: u64,
}

#[derive(Debug, PartialEq)]
pub struct ScoreMetric {
    pub name: NamedNode,
    pub score: u64,
}

impl ScoreGraph {
    // Loads score graph from files.
    pub fn new() -> Result<Self, Error> {
        parse_graphs(vec![VOCAB_GRAPH, SCORE_GRAPH]).map(|store| Self(store))
    }

    // Retrieves the metrics and values of each score dimension.
    pub fn scores(&self) -> Result<ScoreDefinitions, Error> {
        let dimensions = self
            .dimensions()?
            .into_iter()
            .map(|name| {
                let metrics = self.metrics(name.as_ref())?;
                let total_score = metrics.iter().map(|metric| metric.score).sum();
                Ok(ScoreDimension {
                    name,
                    metrics,
                    total_score,
                })
            })
            .collect::<Result<Vec<ScoreDimension>, Error>>()?;
        Ok(ScoreDefinitions {
            total_score: dimensions
                .iter()
                .map(|dimension| dimension.total_score)
                .sum(),
            dimensions,
        })
    }

    /// Retrieves all named dimensions.
    fn dimensions(&self) -> Result<Vec<NamedNode>, Error> {
        let mut dimensions = self
            .0
            .quads_for_pattern(
                None,
                Some(rdf::TYPE),
                Some(dqv::DIMENSION_CLASS.into()),
                None,
            )
            .map(named_quad_subject)
            .collect::<Result<Vec<NamedNode>, Error>>()?;
        dimensions.sort();
        Ok(dimensions)
    }

    /// Retrieves all named metrics and their values, for a given dimension.
    fn metrics(&self, dimension: NamedNodeRef) -> Result<Vec<ScoreMetric>, Error> {
        let q = format!(
            "
                SELECT ?metric ?score
                WHERE {{
                    ?metric a {} .
                    ?metric {} {dimension} .
                    ?metric {} ?score .
                }}
                ORDER BY ?metric
            ",
            dqv::METRIC,
            dqv::IN_DIMENSION,
            dcat_mqa::TRUE_SCORE,
        );
        execute_query(&self.0, &q)?
            .into_iter()
            .map(|qs| {
                let name = match qs.get("metric") {
                    Some(Term::NamedNode(node)) => Ok(node.clone()),
                    _ => Err("unable to read metric from score graph"),
                }?;
                let score = match qs.get("score") {
                    Some(Term::Literal(literal)) => literal.value().parse::<u64>().map_err(|_| {
                        format!(
                            "unable to parse metric score from score graph: '{}'",
                            literal.value()
                        )
                    }),
                    _ => Err("unable to read metric score from score graph".into()),
                }?;
                Ok(ScoreMetric { name, score })
            })
            .collect()
    }
}

impl ScoreMetric {
    /// Score a measurement value.
    pub fn score(&self, value: &MeasurementValue) -> Result<u64, Error> {
        use crate::vocab::dcat_mqa::*;
        use MeasurementValue::*;

        let ok = match self.name.as_ref() {
            ACCESS_URL_STATUS_CODE | DOWNLOAD_URL_STATUS_CODE => match value {
                Int(code) => Ok(200 <= code.clone() && code.clone() < 300),
                _ => Err(format!(
                    "measurement '{}' must be of type int: '{:?}'",
                    self.name, value
                )),
            },
            _ => match value {
                Bool(bool) => Ok(bool.clone()),
                _ => Err(format!(
                    "measurement '{}' must be of type bool: '{:?}'",
                    self.name, value
                )),
            },
        }?;
        Ok(if ok { self.score } else { 0 })
    }
}

#[cfg(test)]
mod tests {
    use super::MeasurementValue;
    use super::*;
    use crate::test::{mqa_node, METRIC_GRAPH, SCORE_GRAPH};
    use crate::vocab::dcat_mqa::*;
    use oxigraph::model::NamedNode;

    fn score_graph() -> ScoreGraph {
        ScoreGraph(parse_graphs(vec![METRIC_GRAPH, SCORE_GRAPH]).unwrap())
    }

    #[test]
    fn dimensions() {
        assert_eq!(
            score_graph().dimensions().unwrap(),
            vec![mqa_node("accessibility"), mqa_node("interoperability")]
        )
    }

    #[test]
    fn score() {
        assert_eq!(
            score_graph().scores().unwrap(),
            ScoreDefinitions {
                dimensions: vec![
                    ScoreDimension {
                        name: mqa_node("accessibility"),
                        metrics: vec![
                            ScoreMetric {
                                name: mqa_node("accessUrlStatusCode"),
                                score: 50
                            },
                            ScoreMetric {
                                name: mqa_node("downloadUrlAvailability"),
                                score: 20
                            },
                        ],
                        total_score: 70,
                    },
                    ScoreDimension {
                        name: mqa_node("interoperability"),
                        metrics: vec![ScoreMetric {
                            name: mqa_node("formatAvailability"),
                            score: 20
                        }],
                        total_score: 20,
                    }
                ],
                total_score: 90,
            }
        );
    }

    #[test]
    fn full_size_graph() {
        assert!(ScoreGraph::new().is_ok());
    }

    #[test]
    fn url_int_measurement() {
        assert_eq!(
            ScoreMetric {
                name: NamedNode::new_unchecked(ACCESS_URL_STATUS_CODE.as_str()),
                score: 20,
            }
            .score(&MeasurementValue::Int(200))
            .unwrap(),
            20
        );
    }

    #[test]
    fn url_bool_measurement() {
        assert!(ScoreMetric {
            name: NamedNode::new_unchecked(DOWNLOAD_URL_STATUS_CODE.as_str()),
            score: 20
        }
        .score(&MeasurementValue::Bool(true))
        .is_err());
    }

    #[test]
    fn bool_measurements() {
        assert!(ScoreMetric {
            name: NamedNode::new_unchecked(""),
            score: 10
        }
        .score(&MeasurementValue::Int(10))
        .is_err(),);

        assert_eq!(
            ScoreMetric {
                name: NamedNode::new_unchecked(""),
                score: 10
            }
            .score(&MeasurementValue::Bool(true))
            .unwrap(),
            10
        );
        assert_eq!(
            ScoreMetric {
                name: NamedNode::new_unchecked(""),
                score: 10
            }
            .score(&MeasurementValue::Bool(false))
            .unwrap(),
            0
        );
    }
}
