use crate::{
    error::MqaError,
    quality_measurements::{MeasurementGraph, QualityMeasurementValue},
};
use oxigraph::model::{NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
pub struct DistributionScore(pub NamedOrBlankNode, pub Vec<DimensionScore>);
#[derive(Clone, Debug, PartialEq)]
pub struct DimensionScore(pub NamedNode, pub Vec<MetricScore>);
#[derive(Clone, Debug, PartialEq)]
pub struct MetricScore(pub NamedNode, pub Option<u64>);

/// Parses graph and calculates score for all metrics in all dimensions, for all distributions.
pub fn parse_graph_and_calculate_score(
    graph: String,
    scores: &Vec<crate::score_graph::Dimension>,
) -> Result<String, MqaError> {
    let mut measurement_graph = MeasurementGraph::parse(graph)?;
    let (dataset_score, distribution_scores) = calculate_score(&measurement_graph, scores)?;
    measurement_graph.insert_scores(&vec![dataset_score])?;
    measurement_graph.insert_scores(&distribution_scores)?;
    measurement_graph.to_string()
}

/// Calculates score for all metrics in all dimensions, for all distributions.
fn calculate_score(
    store: &MeasurementGraph,
    scores: &Vec<crate::score_graph::Dimension>,
) -> Result<(DistributionScore, Vec<DistributionScore>), MqaError> {
    let graph_measurements = store.quality_measurements()?;

    let dataset = store.dataset()?;
    let dataset_score = node_score(scores, &graph_measurements, dataset.as_ref());

    let distributions = store.distributions()?;
    let distribution_scores: Vec<DistributionScore> = distributions
        .into_iter()
        .map(|distribution| {
            DistributionScore(
                distribution.clone(),
                node_score(scores, &graph_measurements, distribution.as_ref()),
            )
        })
        .collect();

    let dataset_merged_distribution_scores: Vec<DistributionScore> = distribution_scores
        .iter()
        .map(|DistributionScore(distribution, score)| {
            DistributionScore(
                distribution.clone(),
                merge_distribution_scores(score.clone(), &dataset_score),
            )
        })
        .collect();

    let dataset_score = best_distribution(dataset_merged_distribution_scores)
        .map(|DistributionScore(_, score)| score)
        .unwrap_or(dataset_score);

    Ok((
        DistributionScore(dataset, dataset_score),
        distribution_scores,
    ))
}

// Merges two distribution scores by taking the max value of each metric.
// NOTE: both inputs MUST be of same size have equal dimension/metric order.
fn merge_distribution_scores(
    score: Vec<DimensionScore>,
    other: &Vec<DimensionScore>,
) -> Vec<DimensionScore> {
    score
        .into_iter()
        .zip(other)
        .map(
            |(DimensionScore(dimension, scores), DimensionScore(_, other_scores))| {
                DimensionScore(
                    dimension,
                    scores
                        .into_iter()
                        .zip(other_scores)
                        .map(
                            |(MetricScore(metric, value), MetricScore(_, other_value))| {
                                MetricScore(metric, value.max(other_value.clone()))
                            },
                        )
                        .collect(),
                )
            },
        )
        .collect()
}

// Find best scoring distribution.
pub fn best_distribution(distribution_scores: Vec<DistributionScore>) -> Option<DistributionScore> {
    distribution_scores
        .iter()
        .max_by_key::<u64, _>(|DistributionScore(_, dimensions)| {
            dimensions
                .iter()
                .map::<u64, _>(|DimensionScore(_, metrics)| {
                    metrics
                        .iter()
                        .map(|MetricScore(_, value)| value.unwrap_or(0))
                        .sum()
                })
                .sum()
        })
        .map(|best| best.clone())
}

/// Calculates score for all metrics in all dimensions, for a distribution or dataset resource.
fn node_score(
    dimension_scores: &Vec<crate::score_graph::Dimension>,
    graph_measurements: &HashMap<(NamedOrBlankNode, NamedNode), QualityMeasurementValue>,
    resource: NamedOrBlankNodeRef,
) -> Vec<DimensionScore> {
    dimension_scores
        .iter()
        .map(|(dimension, metrics_scores)| {
            DimensionScore(
                dimension.clone(),
                metrics_scores
                    .iter()
                    .map(|(metric, score)| {
                        match graph_measurements.get(&(resource.into(), metric.clone())) {
                            Some(val) => MetricScore(
                                metric.clone(),
                                Some(if val.acceptable() { score.clone() } else { 0 }),
                            ),
                            None => MetricScore(metric.clone(), None),
                        }
                    })
                    .collect(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{mqa_node, node, MEASUREMENT_GRAPH};

    #[test]
    fn test_score_measurements() {
        let measurement_graph = MeasurementGraph::parse(MEASUREMENT_GRAPH.to_string()).unwrap();
        let metric_scores = crate::score_graph::tests::score_graph().scores().unwrap();
        let (dataset_score, distribution_scores) =
            calculate_score(&measurement_graph, &metric_scores).unwrap();

        assert_eq!(
            dataset_score,
            DistributionScore(
                node("https://dataset.foo"),
                vec![
                    DimensionScore(
                        mqa_node("interoperability"),
                        vec![MetricScore(mqa_node("formatAvailability"), None)],
                    ),
                    DimensionScore(
                        mqa_node("accessibility"),
                        vec![
                            MetricScore(mqa_node("downloadUrlAvailability"), Some(20)),
                            MetricScore(mqa_node("accessUrlStatusCode"), None),
                        ],
                    ),
                ],
            )
        );

        let a = DistributionScore(
            node("https://distribution.a"),
            vec![
                DimensionScore(
                    mqa_node("interoperability"),
                    vec![MetricScore(mqa_node("formatAvailability"), Some(0))],
                ),
                DimensionScore(
                    mqa_node("accessibility"),
                    vec![
                        MetricScore(mqa_node("downloadUrlAvailability"), None),
                        MetricScore(mqa_node("accessUrlStatusCode"), Some(50)),
                    ],
                ),
            ],
        );
        let b = DistributionScore(
            node("https://distribution.b"),
            vec![
                DimensionScore(
                    mqa_node("interoperability"),
                    vec![MetricScore(mqa_node("formatAvailability"), Some(20))],
                ),
                DimensionScore(
                    mqa_node("accessibility"),
                    vec![
                        MetricScore(mqa_node("downloadUrlAvailability"), None),
                        MetricScore(mqa_node("accessUrlStatusCode"), None),
                    ],
                ),
            ],
        );
        assert_eq!(distribution_scores, vec![b.clone(), a.clone()]);
        assert_eq!(best_distribution(distribution_scores), Some(a));
    }
}
