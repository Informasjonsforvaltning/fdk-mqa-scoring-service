use crate::{
    dcatno_ap::DcatapMqaMetricScores,
    helpers::{parse_graphs, StoreError},
    quality_measurements::{distributions, quality_measurements, QualityMeasurementValue},
};
use oxigraph::{
    model::{NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef},
    store::Store,
};

/// Parses graph and calculates score for all metrics in all dimensions, for all distributions.
pub fn parse_graph_and_calculate_score(
    graph: String,
    scores: &DcatapMqaMetricScores,
) -> Result<Vec<(NamedOrBlankNode, Vec<(NamedNode, Vec<(NamedNode, Option<u64>)>)>)>, StoreError> {
    parse_graphs(vec![graph]).and_then(|store| calculate_score(&store, scores))
}

/// Calculates score for all metrics in all dimensions, for all distributions.
fn calculate_score(
    store: &Store,
    scores: &DcatapMqaMetricScores,
) -> Result<Vec<(NamedOrBlankNode, Vec<(NamedNode, Vec<(NamedNode, Option<u64>)>)>)>, StoreError> {
    distributions(store)?
        .iter()
        .map(|dist| {
            distribution_score(store, scores, dist.as_ref()).map(|scores| (dist.clone(), scores))
        })
        .collect()
}

/// Calculates score for all metrics in all dimensions, for a distributions.
fn distribution_score(
    store: &Store,
    scores: &DcatapMqaMetricScores,
    distribution: NamedOrBlankNodeRef,
) -> Result<Vec<(NamedNode, Vec<(NamedNode, Option<u64>)>)>, StoreError> {
    quality_measurements(store, distribution.into()).map(|graph_dist_measurements| {
        scores
            .iter()
            .map(|(diemsion, score_measurements)| {
                (
                    diemsion.clone(),
                    score_measurements
                        .iter()
                        .map(|(measurement, score)| {
                            match graph_dist_measurements.get(measurement) {
                                Some(val) => (
                                    measurement.clone(),
                                    Some(if score_true(val) { score.clone() } else { 0 }),
                                ),
                                None => (measurement.clone(), None),
                            }
                        })
                        .collect(),
                )
            })
            .collect()
    })
}

fn score_true(value: &QualityMeasurementValue) -> bool {
    match value {
        QualityMeasurementValue::Int(code) => 200 <= code.clone() && code.clone() < 300,
        QualityMeasurementValue::Bool(bool) => bool.clone(),
        _ => false,
    }
}

/// Prints score for all metrics in all dimensions, for all distributions.
pub fn print_scores(scores: Vec<(NamedOrBlankNode, Vec<(NamedNode, Vec<(NamedNode, Option<u64>)>)>)>) {
    for (distribution, dimensions) in scores {
        println!("{}", distribution);
        for (dimension, measurements) in dimensions {
            println!("  {}", dimension);
            for (measurement, score) in measurements {
                println!("    {}: {}", measurement, match score {
                    Some(val) => val.to_string(),
                    None => "-".to_string(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dcatno_ap::DcatapMqaStore;
    use std::fs;

    #[test]
    fn test_score_measurements() {
        let metric_scores = DcatapMqaStore::dimension_metric_scores().unwrap();

        let graph_content = fs::read_to_string("test/measurement_graph.ttl")
            .unwrap()
            .to_string();
        let distribution_scores = parse_graph_and_calculate_score(graph_content, &metric_scores);
        assert!(distribution_scores.is_ok());
    }
}
