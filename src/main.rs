use score::{best_distribution, parse_graph_and_calculate_score, print_scores};
use score_graph::ScoreGraph;
use std::fs;

mod error;
mod helpers;
mod quality_measurements;
mod score;
mod score_graph;
pub mod test;
mod vocab;

fn main() {
    let score_graph = ScoreGraph::load().unwrap();
    let metric_scores = score_graph.scores().unwrap();

    let graph_content = fs::read_to_string("measurement_graph.ttl")
        .unwrap()
        .to_string();
    let distribution_scores =
        parse_graph_and_calculate_score(graph_content, &metric_scores).unwrap();

    match best_distribution(distribution_scores) {
        Some(scores) => print_scores(&vec![scores]),
        None => (),
    }
}
