use serde::{Deserialize, Serialize};

use crate::{score, score_graph::ScoreDefinitions};

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateRequest {
    pub turtle_assessment: String,
    pub jsonld_assessment: String,
    pub scores: Scores,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Scores {
    dataset: Score,
    distributions: Vec<Score>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Score {
    id: String,
    dimensions: Vec<DimensionScore>,
    score: u64,
    max_score: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DimensionScore {
    id: String,
    metrics: Vec<MetricScore>,
    score: u64,
    max_score: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricScore {
    id: String,
    score: u64,
    is_scored: bool,
    max_score: u64,
}

fn convert_score(score_definitions: &ScoreDefinitions, score: &score::Score) -> Score {
    let dimensions = score_definitions
        .dimensions
        .iter()
        .zip(score.dimensions.iter())
        .map(|(score_dimension, dimension_score)| DimensionScore {
            // .to_string() without .as_str() returns name wrapped in < >
            id: dimension_score.id.as_str().to_string(),
            metrics: score_dimension
                .metrics
                .iter()
                .zip(dimension_score.metrics.iter())
                .map(|(score_metric, metric_score)| MetricScore {
                    // .to_string() without .as_str() returns name wrapped in < >
                    id: metric_score.id.as_str().to_string(),
                    score: metric_score.score.unwrap_or_default(),
                    is_scored: metric_score.score.is_some(),
                    max_score: score_metric.score,
                })
                .collect(),
            score: dimension_score.score,
            max_score: score_dimension.total_score,
        })
        .collect();

    Score {
        id: score.resource.as_str().to_string(),
        dimensions,
        score: score.score,
        max_score: score_definitions.total_score,
    }
}

pub fn convert_scores(
    score_definitions: &ScoreDefinitions,
    dataset_score: &score::Score,
    distribution_scores: &Vec<score::Score>,
) -> Scores {
    Scores {
        dataset: convert_score(score_definitions, dataset_score),
        distributions: distribution_scores
            .into_iter()
            .map(|score| convert_score(score_definitions, score))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        assessment_graph::AssessmentGraph,
        helpers::parse_graphs,
        score::calculate_score,
        score_graph::ScoreGraph,
        test::{MEASUREMENT_GRAPH, METRIC_GRAPH, SCORE_GRAPH},
    };

    use super::*;

    #[test]
    fn score() {
        let score_definitions = ScoreGraph(parse_graphs(vec![METRIC_GRAPH, SCORE_GRAPH]).unwrap())
            .scores()
            .unwrap();

        let mut measurement_graph = AssessmentGraph::new().unwrap();
        measurement_graph.load(MEASUREMENT_GRAPH).unwrap();
        let (dataset_score, distribution_scores) =
            calculate_score(&measurement_graph, &score_definitions).unwrap();

        let scores = convert_scores(&score_definitions, &dataset_score, &distribution_scores);

        assert_eq!(scores, Scores {
            dataset: Score {
                id: "https://dataset.foo".to_string(),
                dimensions: vec![
                    DimensionScore {
                        id: "https://data.norge.no/vocabulary/dcatno-mqa#accessibility".to_string(),
                        metrics: vec![
                            MetricScore {
                                id: "https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode".to_string(),
                                score: 50,
                                is_scored: true,
                                max_score: 50,
                            },
                            MetricScore {
                                id: "https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability".to_string(),
                                score: 20,
                                is_scored: true,
                                max_score: 20,
                            },
                        ],
                        score: 70,
                        max_score: 70,
                    },
                    DimensionScore {
                        id: "https://data.norge.no/vocabulary/dcatno-mqa#interoperability".to_string(),
                        metrics: vec![
                            MetricScore {
                                id: "https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability".to_string(),
                                score: 0,
                                is_scored: true,
                                max_score: 20,
                            },
                        ],
                        score: 0,
                        max_score: 20,
                    },
                ],
                score: 70,
                max_score: 90,
            },
            distributions: vec![
                Score {
                    id: "https://distribution.a".to_string(),
                    dimensions: vec![
                        DimensionScore {
                            id: "https://data.norge.no/vocabulary/dcatno-mqa#accessibility".to_string(),
                            metrics: vec![
                                MetricScore {
                                    id: "https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode".to_string(),
                                    score: 50,
                                    is_scored: true,
                                    max_score: 50,
                                },
                                MetricScore {
                                    id: "https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability".to_string(),
                                    score: 0,
                                    is_scored: false,
                                    max_score: 20,
                                },
                            ],
                            score: 50,
                            max_score: 70,
                        },
                        DimensionScore {
                            id: "https://data.norge.no/vocabulary/dcatno-mqa#interoperability".to_string(),
                            metrics: vec![
                                MetricScore {
                                    id: "https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability".to_string(),
                                    score: 0,
                                    is_scored: true,
                                    max_score: 20,
                                },
                            ],
                            score: 0,
                            max_score: 20,
                        },
                    ],
                    score: 50,
                    max_score: 90,
                },
                Score {
                    id: "https://distribution.b".to_string(),
                    dimensions: vec![
                        DimensionScore {
                            id: "https://data.norge.no/vocabulary/dcatno-mqa#accessibility".to_string(),
                            metrics: vec![
                                MetricScore {
                                    id: "https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode".to_string(),
                                    score: 0,
                                    is_scored: false,
                                    max_score: 50,
                                },
                                MetricScore {
                                    id: "https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability".to_string(),
                                    score: 0,
                                    is_scored: false,
                                    max_score: 20,
                                },
                            ],
                            score: 0,
                            max_score: 70,
                        },
                        DimensionScore {
                            id: "https://data.norge.no/vocabulary/dcatno-mqa#interoperability".to_string(),
                            metrics: vec![
                                MetricScore {
                                    id: "https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability".to_string(),
                                    score: 20,
                                    is_scored: true,
                                    max_score: 20,
                                },
                            ],
                            score: 20,
                            max_score: 20,
                        },
                    ],
                    score: 20,
                    max_score: 90,
                },
            ],
        });
    }
}
