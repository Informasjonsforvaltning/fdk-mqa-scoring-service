use std::fs;

use dcatno_ap::DcatapMqaStore;
use score::{parse_graph_and_calculate_score, print_scores};

mod dcatno_ap;
mod helpers;
mod quality_measurements;
mod score;
mod vocab;

fn main() {
    let metric_scores = DcatapMqaStore::dimension_metric_scores().unwrap();

    let graph_content = fs::read_to_string("test/measurement_graph.ttl")
        .unwrap()
        .to_string();
    let distribution_scores =
        parse_graph_and_calculate_score(graph_content, &metric_scores).unwrap();

    print_scores(distribution_scores);
}
