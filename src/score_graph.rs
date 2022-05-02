use crate::{
    error::MqaError,
    helpers::execute_query,
    helpers::{load_files, named_quad_subject, parse_graphs},
    vocab::{dcat_mqa, dqv},
};
use oxigraph::{
    model::{vocab::rdf, NamedNode, NamedNodeRef, Term},
    store::StorageError,
};

pub struct ScoreGraph(oxigraph::store::Store);
pub type DimensionScores = Vec<(NamedNode, MetricScores)>;
pub type MetricScores = Vec<(NamedNode, u64)>;

impl ScoreGraph {
    // Returns metrics and values of each score dimension.
    pub fn scores(&self) -> Result<DimensionScores, MqaError> {
        self.dimensions()?
            .into_iter()
            .map(|dimension| {
                let metrics = self.metrics(dimension.as_ref())?;
                Ok((dimension, metrics))
            })
            .collect()
    }

    // Loads score graph from files.
    pub fn load() -> Result<Self, MqaError> {
        let fnames = vec![
            "graphs/dcatno-mqa-vocabulary.ttl",
            "graphs/dcatno-mqa-vocabulary-default-score-values.ttl",
        ];
        match load_files(fnames) {
            Ok(graphs) => match parse_graphs(graphs) {
                Ok(store) => Ok(ScoreGraph(store)),
                Err(e) => Err(e.into()),
            },
            Err(e) => Err(StorageError::Io(e).into()),
        }
    }

    /// Retrieves all named dimensions.
    fn dimensions(&self) -> Result<Vec<NamedNode>, MqaError> {
        self.0
            .quads_for_pattern(None, Some(rdf::TYPE), Some(dqv::DIMENSION.into()), None)
            .map(named_quad_subject)
            .collect()
    }

    /// Retrieves all named metrics and their values, for a given dimension.
    fn metrics(&self, dimension: NamedNodeRef) -> Result<MetricScores, MqaError> {
        let q = format!(
            "
                SELECT ?metric ?value
                WHERE {{
                    ?metric a {} .
                    ?metric {} {} .
                    ?metric {} ?value .
                }}
            ",
            dqv::METRIC,
            dqv::IN_DIMENSION,
            dimension,
            dcat_mqa::TRUE_SCORE,
        );
        execute_query(&q, &self.0)?
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

    pub fn score_graph() -> ScoreGraph {
        ScoreGraph(
            parse_graphs(vec![
                r#"
                @prefix dcatno-mqa: <https://data.norge.no/vocabulary/dcatno-mqa#> .
                @prefix dqv:        <http://www.w3.org/ns/dqv#> .
                dcatno-mqa:accessibility
                    a                       dqv:Dimension .
                dcatno-mqa:interoperability
                    a                       dqv:Dimension .
                dcatno-mqa:accessUrlStatusCode
                    a                       dqv:Metric ;
                    dqv:inDimension         dcatno-mqa:accessibility .
                dcatno-mqa:downloadUrlAvailability
                    a                       dqv:Metric ;
                    dqv:inDimension         dcatno-mqa:accessibility .
                dcatno-mqa:formatAvailability
                    a                       dqv:Metric ;
                    dqv:inDimension         dcatno-mqa:interoperability .
                "#
                .to_string(),
                r#"
                @prefix dcatno-mqa: <https://data.norge.no/vocabulary/dcatno-mqa#> .
                @prefix xsd:        <http://www.w3.org/2001/XMLSchema#> .
                dcatno-mqa:accessUrlStatusCode
                    dcatno-mqa:trueScore            "50"^^xsd:integer .
                dcatno-mqa:downloadUrlAvailability
                    dcatno-mqa:trueScore            "20"^^xsd:integer .
                dcatno-mqa:formatAvailability
                    dcatno-mqa:trueScore            "20"^^xsd:integer .
                "#
                .to_string(),
            ])
            .unwrap(),
        )
    }

    fn node(name: &str) -> NamedNode {
        NamedNode::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#".to_string() + name)
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
            vec![node("interoperability"), node("accessibility"),]
        )
    }

    #[test]
    fn score() {
        let score_graph = score_graph();
        assert_eq!(
            score_graph.scores().unwrap(),
            vec![
                (
                    node("interoperability"),
                    vec![(node("formatAvailability"), 20)]
                ),
                (
                    node("accessibility"),
                    vec![
                        (node("downloadUrlAvailability"), 20),
                        (node("accessUrlStatusCode"), 50),
                    ]
                )
            ]
        );
    }
}
