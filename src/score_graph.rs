use crate::{
    error::MqaError,
    helpers::execute_query,
    helpers::{load_files, named_quad_subject, parse_graphs},
    vocab::{dcat_mqa, dqv},
};
use oxigraph::model::{vocab::rdf, NamedNode, NamedNodeRef, Term};

pub struct ScoreGraph(oxigraph::store::Store);
pub type Dimension = (NamedNode, Vec<Metric>);
pub type Metric = (NamedNode, u64);

impl ScoreGraph {
    // Loads score graph from files.
    pub fn load() -> Result<Self, MqaError> {
        let fnames = vec![
            "graphs/dcatno-mqa-vocabulary.ttl",
            "graphs/dcatno-mqa-vocabulary-default-score-values.ttl",
        ];
        let graphs = load_files(fnames)?;
        parse_graphs(graphs).map(|store| Self(store))
    }

    // Returns metrics and values of each score dimension.
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
    fn metrics(&self, dimension: NamedNodeRef) -> Result<Vec<Metric>, MqaError> {
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
                    _ => Err("unable to get metric"),
                }?;
                let value = match qs.get("value") {
                    Some(Term::Literal(literal)) => match literal.value().parse::<u64>() {
                        Ok(score) => Ok(score),
                        _ => Err("unable to parse metric score".into()),
                    },
                    _ => Err("unable to get metric value"),
                }?;
                Ok((metric, value))
            })
            .collect()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        helpers::tests::mqa_node,
        test::{METRIC_GRAPH, SCORE_GRAPH},
    };

    pub fn score_graph() -> ScoreGraph {
        ScoreGraph(parse_graphs(vec![METRIC_GRAPH.to_string(), SCORE_GRAPH.to_string()]).unwrap())
    }

    #[test]
    fn store() {
        let _ = ScoreGraph::load().unwrap();
    }

    #[test]
    fn dimensions() {
        let score_graph = score_graph();
        let dimension = score_graph.dimensions().unwrap();
        assert_eq!(
            dimension,
            vec![mqa_node("interoperability"), mqa_node("accessibility"),]
        )
    }

    #[test]
    fn score() {
        let score_graph = score_graph();
        assert_eq!(
            score_graph.scores().unwrap(),
            vec![
                (
                    mqa_node("interoperability"),
                    vec![(mqa_node("formatAvailability"), 20)]
                ),
                (
                    mqa_node("accessibility"),
                    vec![
                        (mqa_node("downloadUrlAvailability"), 20),
                        (mqa_node("accessUrlStatusCode"), 50),
                    ]
                )
            ]
        );
    }
}
