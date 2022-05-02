use crate::{
    error::MqaError,
    helpers::parse_graphs,
    quality_measurements::{
        datasets, distributions, quality_measurements, QualityMeasurementValue,
    },
};
use oxigraph::{
    model::{NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef},
    store::Store,
};
use std::collections::HashMap;

pub type DistributionScores = Vec<(NamedOrBlankNode, DimensionScores)>;
pub type DimensionScores = Vec<(NamedNode, MetricScores)>;
pub type MetricScores = Vec<(NamedNode, Option<u64>)>;

/// Parses graph and calculates score for all metrics in all dimensions, for all distributions.
pub fn parse_graph_and_calculate_score(
    graph: String,
    scores: &crate::score_graph::DimensionScores,
) -> Result<DistributionScores, MqaError> {
    parse_graphs(vec![graph]).and_then(|store| calculate_score(&store, scores))
}

/// Calculates score for all metrics in all dimensions, for all distributions.
fn calculate_score(
    store: &Store,
    scores: &crate::score_graph::DimensionScores,
) -> Result<DistributionScores, MqaError> {
    let graph_measurements = quality_measurements(store)?;

    let dataset = match datasets(store)?.into_iter().next() {
        Some(dataset) => Ok(dataset),
        None => Err(MqaError::from("store has no dataset")),
    }?;
    let dataset_score = node_score(scores, &graph_measurements, dataset.as_ref());

    let dists = distributions(store)?;
    Ok(dists
        .into_iter()
        .map(|dist| {
            (
                dist.clone(),
                merge_dimension_scores(
                    node_score(scores, &graph_measurements, dist.as_ref()),
                    &dataset_score,
                ),
            )
        })
        .collect())
}

// Merges two dimension scores by taking the max value of each metric.
// NOTE: both inputs MUST be of same size have equal dimension/metric order.
fn merge_dimension_scores(score: DimensionScores, other: &DimensionScores) -> DimensionScores {
    score
        .into_iter()
        .zip(other)
        .map(|((dimension, scores), (_, dataset_scores))| {
            (
                dimension,
                scores
                    .into_iter()
                    .zip(dataset_scores)
                    .map(|((metric, value), (_, dataset_value))| {
                        (metric, value.max(dataset_value.clone()))
                    })
                    .collect(),
            )
        })
        .collect()
}

// Find best scoring distribution.
pub fn best_distribution(
    distribution_scores: DistributionScores,
) -> Option<(NamedOrBlankNode, DimensionScores)> {
    distribution_scores
        .iter()
        .max_by_key::<u64, _>(|(_, dimensions)| {
            dimensions
                .iter()
                .map::<u64, _>(|(_, metrics)| {
                    metrics.iter().map(|(_, value)| value.unwrap_or(0)).sum()
                })
                .sum()
        })
        .map(|best| best.clone())
}

/// Calculates score for all metrics in all dimensions, for a distribution or dataset resource.
fn node_score(
    dimension_scores: &crate::score_graph::DimensionScores,
    graph_measurements: &HashMap<(NamedOrBlankNode, NamedNode), QualityMeasurementValue>,
    resource: NamedOrBlankNodeRef,
) -> DimensionScores {
    dimension_scores
        .iter()
        .map(|(dimension, metrics_scores)| {
            (
                dimension.clone(),
                metrics_scores
                    .iter()
                    .map(|(metric, score)| {
                        match graph_measurements.get(&(resource.into(), metric.clone())) {
                            Some(val) => (
                                metric.clone(),
                                Some(if score_true(val) { score.clone() } else { 0 }),
                            ),
                            None => (metric.clone(), None),
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
pub fn print_scores(scores: &DistributionScores) {
    for (distribution, dimensions) in scores {
        println!("{}", distribution);
        for (dimension, measurements) in dimensions {
            println!("  {}", dimension);
            for (measurement, score) in measurements {
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

        let a = (
            node("https://distribution.a"),
            vec![
                (
                    mqa_node("interoperability"),
                    vec![(mqa_node("formatAvailability"), Some(0))],
                ),
                (
                    mqa_node("accessibility"),
                    vec![
                        (mqa_node("downloadUrlAvailability"), Some(20)),
                        (mqa_node("accessUrlStatusCode"), Some(50)),
                    ],
                ),
            ],
        );
        let b = (
            node("https://distribution.b"),
            vec![
                (
                    mqa_node("interoperability"),
                    vec![(mqa_node("formatAvailability"), Some(20))],
                ),
                (
                    mqa_node("accessibility"),
                    vec![
                        (mqa_node("downloadUrlAvailability"), Some(20)),
                        (mqa_node("accessUrlStatusCode"), None),
                    ],
                ),
            ],
        );
        assert_eq!(distribution_scores, vec![b.clone(), a.clone()]);
        assert_eq!(best_distribution(distribution_scores), Some(a));
    }
}
