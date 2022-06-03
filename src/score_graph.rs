use oxigraph::model::{vocab::rdf, NamedNode, NamedNodeRef, Term};

use crate::{
    error::MqaError,
    helpers::execute_query,
    helpers::{named_quad_subject, parse_graphs},
    measurement_value::MeasurementValue,
    vocab::{dcat_mqa, dqv},
};

static VOCAB_GRAPH: &str = include_str!("../graphs/dcatno-mqa-vocabulary.ttl");
static SCORE_GRAPH: &str = include_str!("../graphs/dcatno-mqa-vocabulary-default-score-values.ttl");

pub struct ScoreGraph(pub oxigraph::store::Store);
pub type Dimension = (NamedNode, Vec<ScoreMetric>);
#[derive(Debug, PartialEq)]
pub struct ScoreMetric(pub NamedNode, u64);

impl ScoreGraph {
    // Loads score graph from files.
    pub fn load() -> Result<Self, MqaError> {
        parse_graphs(vec![VOCAB_GRAPH, SCORE_GRAPH]).map(|store| Self(store))
    }

    // Retrieves the metrics and values of each score dimension.
    pub fn scores(&self) -> Result<Vec<Dimension>, MqaError> {
        self.dimensions()?
            .into_iter()
            .map(|dimension| {
                let metrics = self.metrics(dimension.as_ref())?;
                Ok((dimension, metrics))
            })
            .collect()
    }

    /// Retrieves all named dimensions.
    fn dimensions(&self) -> Result<Vec<NamedNode>, MqaError> {
        self.0
            .quads_for_pattern(None, Some(rdf::TYPE), Some(dqv::DIMENSION.into()), None)
            .map(named_quad_subject)
            .collect()
    }

    /// Retrieves all named metrics and their values, for a given dimension.
    fn metrics(&self, dimension: NamedNodeRef) -> Result<Vec<ScoreMetric>, MqaError> {
        let q = format!(
            "
                SELECT ?metric ?value
                WHERE {{
                    ?metric a {} .
                    ?metric {} {dimension} .
                    ?metric {} ?value .
                }}
            ",
            dqv::METRIC,
            dqv::IN_DIMENSION,
            dcat_mqa::TRUE_SCORE,
        );
        execute_query(&self.0, &q)?
            .into_iter()
            .map(|qs| {
                let metric = match qs.get("metric") {
                    Some(Term::NamedNode(node)) => Ok(node.clone()),
                    _ => Err("unable to read metric from score graph"),
                }?;
                let value = match qs.get("value") {
                    Some(Term::Literal(literal)) => literal.value().parse::<u64>().map_err(|_| {
                        format!(
                            "unable to parse metric score from score graph: '{}'",
                            literal.value()
                        )
                    }),
                    _ => Err("unable to read metric value from score graph".into()),
                }?;
                Ok(ScoreMetric(metric, value))
            })
            .collect()
    }
}

impl ScoreMetric {
    // Score a measurement value.
    pub fn score(&self, value: &MeasurementValue) -> Result<u64, MqaError> {
        use crate::vocab::dcat_mqa::*;
        use MeasurementValue::*;

        let ok = match self.0.as_ref() {
            ACCESS_URL_STATUS_CODE | DOWNLOAD_URL_STATUS_CODE => match value {
                Int(code) => Ok(200 <= code.clone() && code.clone() < 300),
                _ => Err(format!(
                    "measurement '{}' must be of type int: '{:?}'",
                    self.0, value
                )),
            },
            _ => match value {
                Bool(bool) => Ok(bool.clone()),
                _ => Err(format!(
                    "measurement '{}' must be of type bool: '{:?}'",
                    self.0, value
                )),
            },
        }?;
        Ok(if ok { self.1 } else { 0 })
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
            vec![mqa_node("interoperability"), mqa_node("accessibility"),]
        )
    }

    #[test]
    fn score() {
        assert_eq!(
            score_graph().scores().unwrap(),
            vec![
                (
                    mqa_node("interoperability"),
                    vec![ScoreMetric(mqa_node("formatAvailability"), 20)]
                ),
                (
                    mqa_node("accessibility"),
                    vec![
                        ScoreMetric(mqa_node("downloadUrlAvailability"), 20),
                        ScoreMetric(mqa_node("accessUrlStatusCode"), 50),
                    ]
                )
            ]
        );
    }

    #[test]
    fn full_size_graph() {
        assert!(ScoreGraph::load().is_ok());
    }

    #[test]
    fn url_int_measurement() {
        assert_eq!(
            ScoreMetric(
                NamedNode::new_unchecked(ACCESS_URL_STATUS_CODE.as_str()),
                20
            )
            .score(&MeasurementValue::Int(200))
            .unwrap(),
            20
        );
    }

    #[test]
    fn url_bool_measurement() {
        assert!(ScoreMetric(
            NamedNode::new_unchecked(DOWNLOAD_URL_STATUS_CODE.as_str()),
            20
        )
        .score(&MeasurementValue::Bool(true))
        .is_err());
    }

    #[test]
    fn bool_measurements() {
        assert!(ScoreMetric(NamedNode::new_unchecked(""), 10)
            .score(&MeasurementValue::Int(10))
            .is_err(),);

        assert_eq!(
            ScoreMetric(NamedNode::new_unchecked(""), 10)
                .score(&MeasurementValue::Bool(true))
                .unwrap(),
            10
        );
        assert_eq!(
            ScoreMetric(NamedNode::new_unchecked(""), 10)
                .score(&MeasurementValue::Bool(false))
                .unwrap(),
            0
        );
    }
}
