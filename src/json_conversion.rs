use serde::{Deserialize, Serialize};

use crate::{score::Score, score_graph::ScoreDefinitions};

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateRequest {
    pub turtle_assessment: String,
    pub jsonld_assessment: String,
    pub scores: ApiScores,
}

/// Score payload sent to the scoring API (includes max scores and is_scored flags).
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ApiScores {
    dataset: ApiScore,
    distributions: Vec<ApiScore>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ApiScore {
    id: String,
    dimensions: Vec<ApiDimensionScore>,
    score: u64,
    max_score: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ApiDimensionScore {
    id: String,
    metrics: Vec<ApiMetricScore>,
    score: u64,
    max_score: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ApiMetricScore {
    id: String,
    score: u64,
    is_scored: bool,
    max_score: u64,
}

fn convert_score(score_definitions: &ScoreDefinitions, score: &Score) -> ApiScore {
    let dimensions = score_definitions
        .dimensions
        .iter()
        .zip(score.dimensions.iter())
        .map(|(score_dimension, dimension_score)| ApiDimensionScore {
            // .to_string() without .as_str() returns name wrapped in < >
            id: dimension_score.id.as_str().to_string(),
            metrics: score_dimension
                .metrics
                .iter()
                .zip(dimension_score.metrics.iter())
                .map(|(score_metric, metric_score)| ApiMetricScore {
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

    ApiScore {
        id: score.resource.as_str().to_string(),
        dimensions,
        score: score.score,
        max_score: score_definitions.total_score,
    }
}

pub fn convert_scores(
    score_definitions: &ScoreDefinitions,
    dataset_score: &Score,
    distribution_scores: &[Score],
) -> ApiScores {
    ApiScores {
        dataset: convert_score(score_definitions, dataset_score),
        distributions: distribution_scores
            .iter()
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

        let measurement_graph = AssessmentGraph::new().unwrap();
        measurement_graph.load(MEASUREMENT_GRAPH).unwrap();
        let (dataset_score, distribution_scores) =
            calculate_score(&measurement_graph, &score_definitions).unwrap();

        let scores = convert_scores(&score_definitions, &dataset_score, &distribution_scores);

        assert_eq!(scores, ApiScores {
            dataset: ApiScore {
                id: "https://dataset.foo".to_string(),
                dimensions: vec![
                    ApiDimensionScore {
                        id: "https://data.norge.no/vocabulary/dcatno-mqa#accessibility".to_string(),
                        metrics: vec![
                            ApiMetricScore {
                                id: "https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode".to_string(),
                                score: 50,
                                is_scored: true,
                                max_score: 50,
                            },
                            ApiMetricScore {
                                id: "https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability".to_string(),
                                score: 20,
                                is_scored: true,
                                max_score: 20,
                            },
                        ],
                        score: 70,
                        max_score: 70,
                    },
                    ApiDimensionScore {
                        id: "https://data.norge.no/vocabulary/dcatno-mqa#interoperability".to_string(),
                        metrics: vec![
                            ApiMetricScore {
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
                ApiScore {
                    id: "https://distribution.b".to_string(),
                    dimensions: vec![
                        ApiDimensionScore {
                            id: "https://data.norge.no/vocabulary/dcatno-mqa#accessibility".to_string(),
                            metrics: vec![
                                ApiMetricScore {
                                    id: "https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode".to_string(),
                                    score: 0,
                                    is_scored: false,
                                    max_score: 50,
                                },
                                ApiMetricScore {
                                    id: "https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability".to_string(),
                                    score: 0,
                                    is_scored: false,
                                    max_score: 20,
                                },
                            ],
                            score: 0,
                            max_score: 70,
                        },
                        ApiDimensionScore {
                            id: "https://data.norge.no/vocabulary/dcatno-mqa#interoperability".to_string(),
                            metrics: vec![
                                ApiMetricScore {
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
                ApiScore {
                    id: "https://distribution.a".to_string(),
                    dimensions: vec![
                        ApiDimensionScore {
                            id: "https://data.norge.no/vocabulary/dcatno-mqa#accessibility".to_string(),
                            metrics: vec![
                                ApiMetricScore {
                                    id: "https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode".to_string(),
                                    score: 50,
                                    is_scored: true,
                                    max_score: 50,
                                },
                                ApiMetricScore {
                                    id: "https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability".to_string(),
                                    score: 0,
                                    is_scored: false,
                                    max_score: 20,
                                },
                            ],
                            score: 50,
                            max_score: 70,
                        },
                        ApiDimensionScore {
                            id: "https://data.norge.no/vocabulary/dcatno-mqa#interoperability".to_string(),
                            metrics: vec![
                                ApiMetricScore {
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
            ],
        });
    }
}
