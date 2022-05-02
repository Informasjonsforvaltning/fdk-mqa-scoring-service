use score::{parse_graph_and_calculate_score, print_scores};
use score_graph::ScoreGraph;
use std::fs;

mod error;
mod helpers;
mod quality_measurements;
mod score;
mod score_graph;
mod vocab;

fn main() {
    let score_graph = ScoreGraph::load().unwrap();
    let metric_scores = score_graph.scores().unwrap();

    let graph_content = fs::read_to_string("test/measurement_graph.ttl")
        .unwrap()
        .to_string();
    let distribution_scores =
        parse_graph_and_calculate_score(graph_content, &metric_scores).unwrap();

    print_scores(distribution_scores);
}
