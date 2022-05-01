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
    pub fn scores() -> Result<DimensionScores, MqaError> {
        let store = ScoreGraph::load()?;

        store
            .dimensions()?
            .into_iter()
            .map(|dimension| {
                let metrics = store.metrics(dimension.as_ref())?;
                Ok((dimension, metrics))
            })
            .collect()
    }

    fn load() -> Result<Self, MqaError> {
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
mod tests {
    use super::*;

    #[test]
    fn store() {
        let store = ScoreGraph::load();
        assert!(store.is_ok());
    }

    #[test]
    fn dimensions() {
        let store = ScoreGraph::load().unwrap();
        let dimension = store.dimensions().unwrap();
        assert_eq!(dimension.len(), 5);
    }

    #[test]
    fn score() {
        let scores = ScoreGraph::scores().unwrap();
        assert_eq!(scores.len(), 5);
    }
}
