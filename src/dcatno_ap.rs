use crate::{
    helpers::query,
    helpers::{load_files, named_quad_subject, parse_graphs, StoreError},
    vocab::{dcat_mqa, dqv},
};
use oxigraph::{
    model::{vocab::rdf, NamedNode, Quad, Subject, Term},
    store::StorageError,
};
use std::collections::HashMap;

pub struct DcatapMqaStore(oxigraph::store::Store);
pub type DcatapMqaMetricScores = Vec<(NamedNode, Vec<(NamedNode, u64)>)>;

impl DcatapMqaStore {
    pub fn dimension_metric_scores() -> Result<DcatapMqaMetricScores, StoreError> {
        let store = DcatapMqaStore::load()?;
        let scores = store.metric_scores()?;
        let dimensions = store.dimensions()?;

        dimensions
            .into_iter()
            .map(|dim| match store.metrics(dim.clone()) {
                Ok(metrics) => Ok((
                    dim.clone(),
                    metrics
                        .into_iter()
                        .filter_map(|metric| match scores.get(&metric) {
                            Some(score) => Some((metric, score.clone())),
                            _ => None,
                        })
                        .collect(),
                )),
                Err(e) => Err(e),
            })
            .collect()
    }

    fn load() -> Result<Self, StoreError> {
        let fnames = vec![
            "graphs/dcatno-mqa-vocabulary.ttl",
            "graphs/dcatno-mqa-vocabulary-default-score-values.ttl",
        ];
        match load_files(fnames) {
            Ok(graphs) => match parse_graphs(graphs) {
                Ok(store) => Ok(DcatapMqaStore(store)),
                Err(e) => Err(StoreError::from(e)),
            },
            Err(e) => Err(StoreError::StorageError(StorageError::Io(e))),
        }
    }

    /// Retrieves all named dimensions.
    fn dimensions(&self) -> Result<Vec<NamedNode>, StoreError> {
        self.0
            .quads_for_pattern(None, Some(rdf::TYPE), Some(dqv::DIMENSION.into()), None)
            .filter_map(named_quad_subject)
            .collect::<Result<Vec<NamedNode>, StorageError>>()
            .or_else(|e| Err(e.into()))
    }

    /// Fetches all named metrics of a given dimension.
    /// ```
    /// <metric>
    ///     a                   dqv:Metric ;
    ///     dqv:inDimension     <dimension> .
    /// ```
    fn metrics(&self, dimension: NamedNode) -> Result<Vec<NamedNode>, StoreError> {
        let q = format!(
            "
                SELECT ?metric
                WHERE {{
                    ?metric a {} .
                    ?metric {} {} .
                }}
            ",
            dqv::METRIC,
            dqv::IN_DIMENSION,
            dimension
        );
        let metrics = query(&q, &self.0)
            .or_else(|e| Err(StoreError::from(e)))?
            .into_iter()
            .filter_map(|qs| match qs.get("metric") {
                Some(Term::NamedNode(metric)) => Some(metric.clone()),
                _ => None,
            })
            .collect::<Vec<NamedNode>>();

        Ok(metrics)
    }

    /// Fetches all named metrics of a given dimension.
    /// ```
    /// <metric>
    ///     a                   dqv:Metric ;
    ///     dqv:inDimension     <dimension> .
    /// ```
    /* fn metrics(&self, dimension: NamedNodeRef) -> Result<Vec<NamedNode>, StorageError> {
        let metrics = self
            .0
            .quads_for_pattern(None, None, Some(dqv::METRIC.into()), None)
            .filter_map(named_quad_subject)
            .collect::<Result<HashSet<NamedNode>, StorageError>>()?;

        self.0
            .quads_for_pattern(
                None,
                Some(dqv::IN_DIMENSION.into()),
                Some(dimension.into()),
                None,
            )
            .filter_map(named_quad_subject)
            .filter(|result| match result {
                Ok(node) => metrics.contains(node),
                _ => true,
            })
            .collect()
    }
    */

    /// Fetches all true scores and returns a mapping from metric to true score.
    /// ```
    /// <metric>
    ///     dcatno-mqa:trueScore                "<score>"^^xsd:integer .
    /// ```
    fn metric_scores(&self) -> Result<HashMap<NamedNode, u64>, StoreError> {
        self.0
            .quads_for_pattern(None, Some(dcat_mqa::TRUE_SCORE.into()), None, None)
            .filter_map(|result| match result {
                Ok(Quad {
                    subject,
                    object: Term::Literal(literal),
                    ..
                }) => {
                    // Only named nodes with parsable scores
                    // TODO: Fail on non-named node or non-parsable score?
                    match (subject, literal.value().to_string().parse::<u64>()) {
                        (Subject::NamedNode(node), Ok(score)) => Some(Ok((node, score))),
                        _ => None,
                    }
                }
                Err(e) => Some(Err(StoreError::StorageError(e))),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store() {
        let store = DcatapMqaStore::load();
        assert!(store.is_ok());
    }

    #[test]
    fn test_scores() {
        let store = DcatapMqaStore::load().unwrap();
        let scores = store.metric_scores().unwrap();
        assert!(scores.len() > 0);
    }

    #[test]
    fn test_dimension_metrics() {
        let store = DcatapMqaStore::load().unwrap();
        let scores = store.metric_scores().unwrap();

        let dimensions = store.dimensions().unwrap();
        assert!(dimensions.len() > 0);

        for dim in dimensions {
            let metrics = store.metrics(dim).unwrap();
            assert!(metrics.len() > 0);

            let exceptions = vec![
                "https://data.norge.no/vocabulary/dcatno-mqa#atLeastFourStars",
                "https://data.norge.no/vocabulary/dcatno-mqa#score",
                "https://data.norge.no/vocabulary/dcatno-mqa#openLicense",
            ];
            for metric in metrics {
                let score = scores.get(&metric);
                assert!(exceptions.contains(&metric.as_str()) || score.is_some());
            }
        }
    }
}
