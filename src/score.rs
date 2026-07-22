use oxigraph::model::{NamedNode, NamedNodeRef};
use std::collections::HashMap;

use crate::{
    assessment_graph::{AssessmentGraph, AssessmentNode},
    error::Error,
    measurement_value::MeasurementValue,
    score_graph::{ScoreDefinitions, ScoreDimension},
};

#[derive(Clone, Debug, PartialEq)]
pub struct Score {
    pub assessment: NamedNode,
    pub resource: NamedNode,
    pub dimensions: Vec<DimensionScore>,
    pub score: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DimensionScore {
    pub id: NamedNode,
    pub metrics: Vec<MetricScore>,
    pub score: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricScore {
    pub id: NamedNode,
    pub score: Option<u64>,
}

fn sum_dimensions(dimensions: &[DimensionScore]) -> u64 {
    dimensions.iter().map(|dimension| dimension.score).sum()
}

fn sum_metrics(metrics: &[MetricScore]) -> u64 {
    metrics
        .iter()
        .map(|metric| metric.score.unwrap_or_default())
        .sum()
}

/// Calculates dataset and distribution scores from quality measurements.
///
/// Steps:
/// 1. Score the dataset and each distribution from their measurements.
/// 2. For each distribution, merge its metrics with the dataset's (max per metric).
/// 3. Take the best merged result as the dataset score; if there are no
///    distributions, use the dataset-only scores.
/// 4. Return that dataset score together with the **unmerged** distribution scores.
pub fn calculate_score(
    measurement_graph: &AssessmentGraph,
    score_definitions: &ScoreDefinitions,
) -> Result<(Score, Vec<Score>), Error> {
    let graph_measurements = measurement_graph.quality_measurements()?;

    let dataset = measurement_graph.dataset()?;
    let dataset_dimensions = node_dimension_scores(
        score_definitions,
        &graph_measurements,
        dataset.assessment.as_ref(),
    )?;

    let distribution_scores =
        score_distributions(measurement_graph, score_definitions, &graph_measurements)?;

    let dataset_score = pick_dataset_score(&dataset, dataset_dimensions, &distribution_scores);

    Ok((dataset_score, distribution_scores))
}

/// Scores each distribution from its quality measurements.
fn score_distributions(
    measurement_graph: &AssessmentGraph,
    score_definitions: &ScoreDefinitions,
    graph_measurements: &HashMap<(NamedNode, NamedNode), MeasurementValue>,
) -> Result<Vec<Score>, Error> {
    measurement_graph
        .distributions()?
        .into_iter()
        .map(|distribution| {
            let dimensions = node_dimension_scores(
                score_definitions,
                graph_measurements,
                distribution.assessment.as_ref(),
            )?;
            Ok(Score {
                assessment: distribution.assessment.clone(),
                resource: distribution.resource.clone(),
                score: sum_dimensions(&dimensions),
                dimensions,
            })
        })
        .collect()
}

/// Builds the dataset score from the best distribution after merging each
/// distribution with the dataset's own metric scores.
fn pick_dataset_score(
    dataset: &AssessmentNode,
    dataset_dimensions: Vec<DimensionScore>,
    distribution_scores: &[Score],
) -> Score {
    let merged_scores: Vec<Score> = distribution_scores
        .iter()
        .map(|score| merge_with_dataset(score, &dataset_dimensions))
        .collect();

    let (score, dimensions) = if let Some(best) = best_score(merged_scores) {
        (best.score, best.dimensions)
    } else {
        (sum_dimensions(&dataset_dimensions), dataset_dimensions)
    };

    Score {
        assessment: dataset.assessment.clone(),
        resource: dataset.resource.clone(),
        dimensions,
        score,
    }
}

/// Merges a distribution's dimension scores with the dataset's (max per metric).
fn merge_with_dataset(distribution: &Score, dataset_dimensions: &[DimensionScore]) -> Score {
    let dimensions = merge_dimension_scores(&distribution.dimensions, dataset_dimensions);
    Score {
        assessment: distribution.assessment.clone(),
        resource: distribution.resource.clone(),
        score: sum_dimensions(&dimensions),
        dimensions,
    }
}

/// Merges two node scores by taking the max value of each metric, matched by id.
fn merge_dimension_scores(
    dimensions: &[DimensionScore],
    other: &[DimensionScore],
) -> Vec<DimensionScore> {
    dimensions
        .iter()
        .map(|dimension| {
            let other_dimension = other.iter().find(|d| d.id == dimension.id);
            let metrics: Vec<MetricScore> = dimension
                .metrics
                .iter()
                .map(|metric| {
                    let other_score = other_dimension
                        .and_then(|d| d.metrics.iter().find(|m| m.id == metric.id))
                        .and_then(|m| m.score);
                    MetricScore {
                        id: metric.id.clone(),
                        score: metric.score.max(other_score),
                    }
                })
                .collect();
            DimensionScore {
                id: dimension.id.clone(),
                score: sum_metrics(&metrics),
                metrics,
            }
        })
        .collect()
}

/// Find best scoring distribution.
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
        .map(|ScoreDimension { id, metrics, .. }| {
            let metric_scores: Vec<MetricScore> = metrics
                .iter()
                .map(|metric| {
                    Ok(MetricScore {
                        id: metric.id.clone(),
                        score: match graph_measurements.get(&(node.into(), metric.id.clone())) {
                            Some(val) => Some(metric.score(val)?),
                            None => None,
                        },
                    })
                })
                .collect::<Result<_, Error>>()?;
            Ok(DimensionScore {
                id: id.clone(),
                score: sum_metrics(&metric_scores),
                metrics: metric_scores,
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

        let measurement_graph = AssessmentGraph::new().unwrap();
        measurement_graph.load(MEASUREMENT_GRAPH).unwrap();
        let (dataset_score, distribution_scores) =
            calculate_score(&measurement_graph, &score_definitions).unwrap();

        assert_eq!(
            dataset_score,
            Score {
                assessment: node("https://dataset.assessment.foo"),
                resource: node("https://dataset.foo"),
                dimensions: vec![
                    DimensionScore {
                        id: mqa_node("accessibility"),
                        metrics: vec![
                            MetricScore {
                                id: mqa_node("accessUrlStatusCode"),
                                score: Some(50)
                            },
                            MetricScore {
                                id: mqa_node("downloadUrlAvailability"),
                                score: Some(20),
                            },
                        ],
                        score: 70,
                    },
                    DimensionScore {
                        id: mqa_node("interoperability"),
                        metrics: vec![MetricScore {
                            id: mqa_node("formatAvailability"),
                            score: Some(0)
                        }],
                        score: 0
                    },
                ],
                score: 70,
            }
        );

        let a = Score {
            assessment: node("https://distribution.assessment.a"),
            resource: node("https://distribution.a"),
            dimensions: vec![
                DimensionScore {
                    id: mqa_node("accessibility"),
                    metrics: vec![
                        MetricScore {
                            id: mqa_node("accessUrlStatusCode"),
                            score: Some(50),
                        },
                        MetricScore {
                            id: mqa_node("downloadUrlAvailability"),
                            score: None,
                        },
                    ],
                    score: 50,
                },
                DimensionScore {
                    id: mqa_node("interoperability"),
                    metrics: vec![MetricScore {
                        id: mqa_node("formatAvailability"),
                        score: Some(0),
                    }],
                    score: 0,
                },
            ],
            score: 50,
        };
        let b = Score {
            assessment: node("https://distribution.assessment.b"),
            resource: node("https://distribution.b"),
            dimensions: vec![
                DimensionScore {
                    id: mqa_node("accessibility"),
                    metrics: vec![
                        MetricScore {
                            id: mqa_node("accessUrlStatusCode"),
                            score: None,
                        },
                        MetricScore {
                            id: mqa_node("downloadUrlAvailability"),
                            score: None,
                        },
                    ],
                    score: 0,
                },
                DimensionScore {
                    id: mqa_node("interoperability"),
                    metrics: vec![MetricScore {
                        id: mqa_node("formatAvailability"),
                        score: Some(20),
                    }],
                    score: 20,
                },
            ],
            score: 20,
        };
        assert_eq!(distribution_scores, vec![b.clone(), a.clone()]);
        assert_eq!(best_score(distribution_scores), Some(a));
    }

    #[test]
    fn merge_matches_metrics_by_id_not_order() {
        let left = vec![DimensionScore {
            id: mqa_node("accessibility"),
            metrics: vec![
                MetricScore {
                    id: mqa_node("downloadUrlAvailability"),
                    score: Some(20),
                },
                MetricScore {
                    id: mqa_node("accessUrlStatusCode"),
                    score: Some(10),
                },
            ],
            score: 30,
        }];
        let right = vec![DimensionScore {
            id: mqa_node("accessibility"),
            metrics: vec![
                MetricScore {
                    id: mqa_node("accessUrlStatusCode"),
                    score: Some(50),
                },
                MetricScore {
                    id: mqa_node("downloadUrlAvailability"),
                    score: Some(5),
                },
            ],
            score: 55,
        }];

        assert_eq!(
            merge_dimension_scores(&left, &right),
            vec![DimensionScore {
                id: mqa_node("accessibility"),
                metrics: vec![
                    MetricScore {
                        id: mqa_node("downloadUrlAvailability"),
                        score: Some(20),
                    },
                    MetricScore {
                        id: mqa_node("accessUrlStatusCode"),
                        score: Some(50),
                    },
                ],
                score: 70,
            }]
        );
    }
}
