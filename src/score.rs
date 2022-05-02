use crate::{
    error::MqaError,
    helpers::parse_graphs,
    quality_measurements::{distributions, quality_measurements, QualityMeasurementValue},
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
    let dists = distributions(store)?;

    Ok(dists
        .into_iter()
        .map(|dist| {
            (
                dist.clone(),
                distribution_score(scores, &graph_measurements, dist.as_ref()),
            )
        })
        .collect())
}

/// Calculates score for all metrics in all dimensions, for a distributions.
fn distribution_score(
    dimesion_scores: &crate::score_graph::DimensionScores,
    graph_measurements: &HashMap<(NamedOrBlankNode, NamedNode), QualityMeasurementValue>,
    distribution: NamedOrBlankNodeRef,
) -> DimensionScores {
    dimesion_scores
        .iter()
        .map(|(dimension, metrics_scores)| {
            (
                dimension.clone(),
                metrics_scores
                    .iter()
                    .map(|(metric, score)| {
                        match graph_measurements.get(&(distribution.into(), metric.clone())) {
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

fn score_true(value: &QualityMeasurementValue) -> bool {
    match value {
        QualityMeasurementValue::Int(code) => 200 <= code.clone() && code.clone() < 300,
        QualityMeasurementValue::Bool(bool) => bool.clone(),
        _ => false,
    }
}

/// Prints score for all metrics in all dimensions, for all distributions.
pub fn print_scores(scores: DistributionScores) {
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
    use crate::score_graph::ScoreGraph;
    use std::fs;

    #[test]
    fn test_score_measurements() {
        let metric_scores = ScoreGraph::load().unwrap().scores().unwrap();
        let graph_content = fs::read_to_string("test/measurement_graph.ttl")
            .unwrap()
            .to_string();
        let distribution_scores = parse_graph_and_calculate_score(graph_content, &metric_scores);
        assert!(distribution_scores.is_ok());
    }
}
