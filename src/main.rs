use score::parse_graph_and_calculate_score;
use score_graph::ScoreGraph;
use std::fs;

mod error;
mod helpers;
mod quality_measurements;
mod score;
mod score_graph;
mod test;
mod vocab;

fn main() {
    let score_graph = ScoreGraph::load().unwrap();
    let metric_scores = score_graph.scores().unwrap();

    let measurement_graph_turtle = fs::read_to_string("measurement_graph.ttl")
        .unwrap()
        .to_string();
    let scored_graph =
        parse_graph_and_calculate_score(measurement_graph_turtle, &metric_scores).unwrap();

    println!("{}", scored_graph);
}
