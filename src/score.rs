use crate::{
    error::MqaError,
    quality_measurements::{MeasurementGraph, QualityMeasurementValue},
};
use oxigraph::model::{NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
pub struct DistributionScore(NamedOrBlankNode, Vec<DimensionScore>);
#[derive(Clone, Debug, PartialEq)]
pub struct DimensionScore(NamedNode, Vec<MetricScore>);
#[derive(Clone, Debug, PartialEq)]
pub struct MetricScore(NamedNode, Option<u64>);

/// Parses graph and calculates score for all metrics in all dimensions, for all distributions.
pub fn parse_graph_and_calculate_score(
    graph: String,
    scores: &Vec<crate::score_graph::Dimension>,
) -> Result<Vec<DistributionScore>, MqaError> {
    MeasurementGraph::parse(graph)
        .and_then(|measurement_graph| calculate_score(&measurement_graph, scores))
}

/// Calculates score for all metrics in all dimensions, for all distributions.
fn calculate_score(
    store: &MeasurementGraph,
    scores: &Vec<crate::score_graph::Dimension>,
) -> Result<Vec<DistributionScore>, MqaError> {
    let graph_measurements = store.quality_measurements()?;

    let dataset = match store.datasets()?.into_iter().next() {
        Some(dataset) => Ok(dataset),
        None => Err(MqaError::from("store has no dataset")),
    }?;
    let dataset_score = node_score(scores, &graph_measurements, dataset.as_ref());

    let distributions = store.distributions()?;
    Ok(distributions
        .into_iter()
        .map(|distribution| {
            DistributionScore(
                distribution.clone(),
                merge_distribution_scores(
                    node_score(scores, &graph_measurements, distribution.as_ref()),
                    &dataset_score,
                ),
            )
        })
        .collect())
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
                                Some(if score_true(val) { score.clone() } else { 0 }),
                            ),
                            None => MetricScore(metric.clone(), None),
                        }
                    })
                    .collect(),
            )
        })
        .collect()
}

// Whether a measurement value is considered true.
fn score_true(value: &QualityMeasurementValue) -> bool {
    match value {
        QualityMeasurementValue::Int(code) => 200 <= code.clone() && code.clone() < 300,
        QualityMeasurementValue::Bool(bool) => bool.clone(),
        _ => false,
    }
}

/// Prints score for all metrics in all dimensions, for all distributions.
pub fn print_scores(scores: &Vec<DistributionScore>) {
    for DistributionScore(distribution, dimensions) in scores {
        println!("{}", distribution);
        for DimensionScore(dimension, measurements) in dimensions {
            println!("  {}", dimension);
            for MetricScore(measurement, score) in measurements {
                println!(
                    "    {}: {}",
                    measurement,
                    match score {
                        Some(val) => val.to_string(),
                        None => "-".to_string(),
                    }
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        helpers::tests::{mqa_node, node},
        test::MEASUREMENT_GRAPH,
    };

    #[test]
    fn test_score_measurements() {
        let metric_scores = crate::score_graph::tests::score_graph().scores().unwrap();
        let distribution_scores =
            parse_graph_and_calculate_score(MEASUREMENT_GRAPH.to_string(), &metric_scores).unwrap();

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
                        MetricScore(mqa_node("downloadUrlAvailability"), Some(20)),
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
                        MetricScore(mqa_node("downloadUrlAvailability"), Some(20)),
                        MetricScore(mqa_node("accessUrlStatusCode"), None),
                    ],
                ),
            ],
        );
        assert_eq!(distribution_scores, vec![b.clone(), a.clone()]);
        assert_eq!(best_distribution(distribution_scores), Some(a));
    }
}
