use fdk_mqa_scoring_service::{
    helpers::execute_query,
    vocab::{dcat_mqa, dqv},
};
use oxigraph::{io::GraphFormat, model::GraphNameRef, store::Store};

pub fn sorted_lines(graph: &str) -> Vec<String> {
    let store = Store::new().unwrap();
    store
        .load_graph(
            graph.to_string().as_ref(),
            GraphFormat::Turtle,
            GraphNameRef::DefaultGraph,
            None,
        )
        .unwrap();

    let q = format!(
        "
            SELECT ?node ?metric ?value ?score
            WHERE {{
                ?node {} ?measurement .
                OPTIONAL {{ ?measurement {} ?metric . }}
                OPTIONAL {{ ?measurement {} ?value . }}
                OPTIONAL {{ ?measurement {} ?score . }}
            }}
        ",
        dcat_mqa::CONTAINS_QUALITY_MEASUREMENT,
        dqv::IS_MEASUREMENT_OF,
        dqv::VALUE,
        dcat_mqa::SCORE,
    );
    let mut lines: Vec<String> = execute_query(&store, &q)
        .unwrap()
        .into_iter()
        .map(|qs| {
            format!(
                "{:?} {:?} {:?} {:?}",
                qs.get("node"),
                qs.get("metric"),
                qs.get("value"),
                qs.get("score"),
            )
        })
        .collect();
    lines.sort();
    lines
}
