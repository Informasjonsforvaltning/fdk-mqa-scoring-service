use oxigraph::model::{NamedNode, NamedNodeRef};
use std::collections::HashMap;

use crate::{
    error::Error,
    measurement_graph::MeasurementGraph,
    measurement_value::MeasurementValue,
    score_graph::{ScoreDefinitions, ScoreDimension},
};

#[derive(Clone, Debug, PartialEq)]
pub struct Score {
    pub name: NamedNode,
    pub dimensions: Vec<DimensionScore>,
    pub score: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DimensionScore {
    pub name: NamedNode,
    pub metrics: Vec<MetricScore>,
    pub score: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricScore {
    pub name: NamedNode,
    pub score: Option<u64>,
}

fn sum_dimensions(dimensions: &Vec<DimensionScore>) -> u64 {
    dimensions.iter().map(|dimension| dimension.score).sum()
}

fn sum_metrics(metrics: &Vec<MetricScore>) -> u64 {
    metrics
        .iter()
        .map(|metric| metric.score.unwrap_or_default())
        .sum()
}

/// Calculates score for all metrics in all dimensions, for all distributions.
pub fn calculate_score(
    measurement_graph: &MeasurementGraph,
    score_definitions: &ScoreDefinitions,
) -> Result<(Score, Vec<Score>), Error> {
    let graph_measurements = measurement_graph.quality_measurements()?;

    let dataset_name = measurement_graph.dataset()?;
    let dataset_dimensions = node_dimension_scores(
        score_definitions,
        &graph_measurements,
        dataset_name.as_ref(),
    )?;

    let distributions = measurement_graph.distributions()?;
    let distribution_scores: Vec<Score> = distributions
        .into_iter()
        .map(|distribution| {
            let dimensions = node_dimension_scores(
                score_definitions,
                &graph_measurements,
                distribution.as_ref(),
            )?;
            Ok(Score {
                name: distribution.clone(),
                score: sum_dimensions(&dimensions),
                dimensions,
            })
        })
        .collect::<Result<_, Error>>()?;

    let dataset_merged_distribution_scores: Vec<Score> = distribution_scores
        .iter()
        .map(|score| {
            let dimensions = merge_dimension_scores(score.dimensions.clone(), &dataset_dimensions);
            Score {
                name: score.name.clone(),
                score: sum_dimensions(&dimensions),
                dimensions,
            }
        })
        .collect();

    let (dataset_total_score, dataset_dimensions) =
        if let Some(best) = best_score(dataset_merged_distribution_scores) {
            (best.score, best.dimensions)
        } else {
            (sum_dimensions(&dataset_dimensions), dataset_dimensions)
        };

    Ok((
        Score {
            name: dataset_name,
            dimensions: dataset_dimensions,
            score: dataset_total_score,
        },
        distribution_scores,
    ))
}

// Merges two node scores by taking the max value of each metric.
// NOTE: both inputs MUST be of same size have equal dimension/metric order.
fn merge_dimension_scores(
    dimensions: Vec<DimensionScore>,
    other: &Vec<DimensionScore>,
) -> Vec<DimensionScore> {
    dimensions
        .into_iter()
        .zip(other)
        .map(|(dimension, other)| {
            let metrics = dimension
                .metrics
                .into_iter()
                .zip(other.metrics.iter())
                .map(|(metric, other)| MetricScore {
                    name: metric.name,
                    score: metric.score.max(other.score.clone()),
                })
                .collect();
            DimensionScore {
                name: dimension.name,
                score: sum_metrics(&metrics),
                metrics,
            }
        })
        .collect()
}

// Find best scoring distribution.
pub fn best_score(scores: Vec<Score>) -> Option<Score> {
    scores.into_iter().max_by_key::<u64, _>(|score| score.score)
}

/// Calculates score for all metrics in all dimensions, for a distribution or dataset node.
fn node_dimension_scores(
    score_definitions: &ScoreDefinitions,
    graph_measurements: &HashMap<(NamedNode, NamedNode), MeasurementValue>,
    node: NamedNodeRef,
) -> Result<Vec<DimensionScore>, Error> {
    score_definitions
        .dimensions
        .iter()
        .map(|ScoreDimension { name, metrics, .. }| {
            let metrics = metrics
                .iter()
                .map(|metric| {
                    Ok(MetricScore {
                        name: metric.name.clone(),
                        score: match graph_measurements.get(&(node.into(), metric.name.clone())) {
                            Some(val) => Some(metric.score(val)?),
                            None => None,
                        },
                    })
                })
                .collect::<Result<_, Error>>()?;
            Ok(DimensionScore {
                name: name.clone(),
                score: sum_metrics(&metrics),
                metrics,
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
        let score_definitions = ScoreGraph(parse_graphs(vec![METRIC_GRAPH, SCORE_GRAPH]).unwrap())
            .scores()
            .unwrap();

        let mut measurement_graph = MeasurementGraph::new().unwrap();
        measurement_graph.load(MEASUREMENT_GRAPH).unwrap();
        let (dataset_score, distribution_scores) =
            calculate_score(&measurement_graph, &score_definitions).unwrap();

        assert_eq!(
            dataset_score,
            Score {
                name: node("https://dataset.foo"),
                dimensions: vec![
                    DimensionScore {
                        name: mqa_node("accessibility"),
                        metrics: vec![
                            MetricScore {
                                name: mqa_node("accessUrlStatusCode"),
                                score: Some(50)
                            },
                            MetricScore {
                                name: mqa_node("downloadUrlAvailability"),
                                score: Some(20),
                            },
                        ],
                        score: 70,
                    },
                    DimensionScore {
                        name: mqa_node("interoperability"),
                        metrics: vec![MetricScore {
                            name: mqa_node("formatAvailability"),
                            score: Some(0)
                        }],
                        score: 0
                    },
                ],
                score: 70,
            }
        );

        let a = Score {
            name: node("https://distribution.a"),
            dimensions: vec![
                DimensionScore {
                    name: mqa_node("accessibility"),
                    metrics: vec![
                        MetricScore {
                            name: mqa_node("accessUrlStatusCode"),
                            score: Some(50),
                        },
                        MetricScore {
                            name: mqa_node("downloadUrlAvailability"),
                            score: None,
                        },
                    ],
                    score: 50,
                },
                DimensionScore {
                    name: mqa_node("interoperability"),
                    metrics: vec![MetricScore {
                        name: mqa_node("formatAvailability"),
                        score: Some(0),
                    }],
                    score: 0,
                },
            ],
            score: 50,
        };
        let b = Score {
            name: node("https://distribution.b"),
            dimensions: vec![
                DimensionScore {
                    name: mqa_node("accessibility"),
                    metrics: vec![
                        MetricScore {
                            name: mqa_node("accessUrlStatusCode"),
                            score: None,
                        },
                        MetricScore {
                            name: mqa_node("downloadUrlAvailability"),
                            score: None,
                        },
                    ],
                    score: 0,
                },
                DimensionScore {
                    name: mqa_node("interoperability"),
                    metrics: vec![MetricScore {
                        name: mqa_node("formatAvailability"),
                        score: Some(20),
                    }],
                    score: 20,
                },
            ],
            score: 20,
        };
        assert_eq!(distribution_scores, vec![a.clone(), b.clone()]);
        assert_eq!(best_score(distribution_scores), Some(a));
    }
}
