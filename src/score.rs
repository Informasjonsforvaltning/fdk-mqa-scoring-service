use oxigraph::model::{NamedNode, NamedNodeRef};
use std::collections::HashMap;

use crate::{
    error::MqaError, measurement_graph::MeasurementGraph, measurement_value::MeasurementValue,
    score_graph::ScoreDimension,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Score {
    pub name: NamedNode,
    pub dimensions: Vec<DimensionScore>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DimensionScore {
    pub name: NamedNode,
    pub metrics: Vec<MetricScore>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricScore {
    pub name: NamedNode,
    pub score: Option<u64>,
}

/// Calculates score for all metrics in all dimensions, for all distributions.
pub fn calculate_score(
    measurement_graph: &MeasurementGraph,
    score_dimensions: &Vec<crate::score_graph::ScoreDimension>,
) -> Result<(Score, Vec<Score>), MqaError> {
    let graph_measurements = measurement_graph.quality_measurements()?;

    let dataset_name = measurement_graph.dataset()?;
    let dataset_dim_scores =
        node_dimension_scores(score_dimensions, &graph_measurements, dataset_name.as_ref())?;

    let distributions = measurement_graph.distributions()?;
    let distribution_scores: Vec<Score> = distributions
        .into_iter()
        .map(|distribution| {
            Ok(Score {
                name: distribution.clone(),
                dimensions: node_dimension_scores(
                    score_dimensions,
                    &graph_measurements,
                    distribution.as_ref(),
                )?,
            })
        })
        .collect::<Result<_, MqaError>>()?;

    let dataset_merged_distribution_scores: Vec<Score> = distribution_scores
        .iter()
        .map(|Score { name, dimensions }| Score {
            name: name.clone(),
            dimensions: merge_scores(dimensions.clone(), &dataset_dim_scores),
        })
        .collect();

    let dataset_dimensions = best_score(dataset_merged_distribution_scores)
        .map(|Score { dimensions, .. }| dimensions)
        .unwrap_or(dataset_dim_scores);

    Ok((
        Score {
            name: dataset_name,
            dimensions: dataset_dimensions,
        },
        distribution_scores,
    ))
}

// Merges two distribution scores by taking the max value of each metric.
// NOTE: both inputs MUST be of same size have equal dimension/metric order.
fn merge_scores(score: Vec<DimensionScore>, other: &Vec<DimensionScore>) -> Vec<DimensionScore> {
    score
        .into_iter()
        .zip(other)
        .map(
            |(
                DimensionScore { name, metrics },
                DimensionScore {
                    metrics: other_metrics,
                    ..
                },
            )| {
                DimensionScore {
                    name,
                    metrics: metrics
                        .into_iter()
                        .zip(other_metrics)
                        .map(
                            |(
                                MetricScore { name, score },
                                MetricScore {
                                    score: other_score, ..
                                },
                            )| {
                                MetricScore {
                                    name,
                                    score: score.max(other_score.clone()),
                                }
                            },
                        )
                        .collect(),
                }
            },
        )
        .collect()
}

// Find best scoring distribution.
pub fn best_score(scores: Vec<Score>) -> Option<Score> {
    scores
        .iter()
        .max_by_key::<u64, _>(|Score { dimensions, .. }| {
            dimensions
                .iter()
                .map::<u64, _>(|DimensionScore { metrics, .. }| {
                    metrics
                        .iter()
                        .map(|MetricScore { score, .. }| score.unwrap_or(0))
                        .sum()
                })
                .sum()
        })
        .map(|best| best.clone())
}

/// Calculates score for all metrics in all dimensions, for a distribution or dataset node.
fn node_dimension_scores(
    score_dimensions: &Vec<crate::score_graph::ScoreDimension>,
    graph_measurements: &HashMap<(NamedNode, NamedNode), MeasurementValue>,
    node: NamedNodeRef,
) -> Result<Vec<DimensionScore>, MqaError> {
    score_dimensions
        .iter()
        .map(|ScoreDimension { name, metrics, .. }| {
            Ok(DimensionScore {
                name: name.clone(),
                metrics: metrics
                    .iter()
                    .map(|metric| {
                        Ok(MetricScore {
                            name: metric.name.clone(),
                            score: match graph_measurements.get(&(node.into(), metric.name.clone()))
                            {
                                Some(val) => Some(metric.score(val)?),
                                None => None,
                            },
                        })
                    })
                    .collect::<Result<_, MqaError>>()?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        helpers::parse_graphs,
        score_graph::ScoreGraph,
        test::{mqa_node, node, MEASUREMENT_GRAPH, METRIC_GRAPH, SCORE_GRAPH},
    };

    #[test]
    fn score_measurements() {
        let mut measurement_graph = MeasurementGraph::new().unwrap();
        measurement_graph.load(MEASUREMENT_GRAPH).unwrap();
        let metric_scores = ScoreGraph(parse_graphs(vec![METRIC_GRAPH, SCORE_GRAPH]).unwrap())
            .scores()
            .unwrap();
        let (dataset_score, distribution_scores) =
            calculate_score(&measurement_graph, &metric_scores).unwrap();

        assert_eq!(
            dataset_score,
            Score {
                name: node("https://dataset.foo"),
                dimensions: vec![
                    DimensionScore {
                        name: mqa_node("interoperability"),
                        metrics: vec![MetricScore {
                            name: mqa_node("formatAvailability"),
                            score: Some(0)
                        }],
                    },
                    DimensionScore {
                        name: mqa_node("accessibility"),
                        metrics: vec![
                            MetricScore {
                                name: mqa_node("downloadUrlAvailability"),
                                score: Some(20)
                            },
                            MetricScore {
                                name: mqa_node("accessUrlStatusCode"),
                                score: Some(50)
                            },
                        ],
                    },
                ],
            }
        );

        let a = Score {
            name: node("https://distribution.a"),
            dimensions: vec![
                DimensionScore {
                    name: mqa_node("interoperability"),
                    metrics: vec![MetricScore {
                        name: mqa_node("formatAvailability"),
                        score: Some(0),
                    }],
                },
                DimensionScore {
                    name: mqa_node("accessibility"),
                    metrics: vec![
                        MetricScore {
                            name: mqa_node("downloadUrlAvailability"),
                            score: None,
                        },
                        MetricScore {
                            name: mqa_node("accessUrlStatusCode"),
                            score: Some(50),
                        },
                    ],
                },
            ],
        };
        let b = Score {
            name: node("https://distribution.b"),
            dimensions: vec![
                DimensionScore {
                    name: mqa_node("interoperability"),
                    metrics: vec![MetricScore {
                        name: mqa_node("formatAvailability"),
                        score: Some(20),
                    }],
                },
                DimensionScore {
                    name: mqa_node("accessibility"),
                    metrics: vec![
                        MetricScore {
                            name: mqa_node("downloadUrlAvailability"),
                            score: None,
                        },
                        MetricScore {
                            name: mqa_node("accessUrlStatusCode"),
                            score: None,
                        },
                    ],
                },
            ],
        };
        assert_eq!(distribution_scores, vec![b.clone(), a.clone()]);
        assert_eq!(best_score(distribution_scores), Some(a));
    }
}
