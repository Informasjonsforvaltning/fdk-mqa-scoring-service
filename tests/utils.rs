use fdk_mqa_scoring_service::{
    helpers::execute_query,
    vocab::{dcat_mqa, dcat_terms, dqv, rdf_syntax},
};
use oxigraph::{io::GraphFormat, model::GraphNameRef, store::Store};

/// Extracts most node names and properties in a deterministic order. Ignores blank nodes.
pub fn comparable_turtle_content(graph: &str) -> Vec<String> {
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
            SELECT ?node ?type ?modified ?assessmentOf ?distributionAssessment ?metric ?value ?score
            WHERE {{
                OPTIONAL {{ ?node {} ?type }}
                OPTIONAL {{ ?node {} ?modified }}
                OPTIONAL {{ ?node {} ?assessmentOf }}
                OPTIONAL {{ ?node {} ?distributionAssessment }}
                OPTIONAL {{ ?node {} ?measurement . }}
                OPTIONAL {{ ?measurement {} ?metric . }}
                OPTIONAL {{ ?measurement {} ?value . }}
                OPTIONAL {{ ?measurement {} ?score . }}
            }}
        ",
        rdf_syntax::TYPE,
        dcat_terms::MODIFIED,
        dcat_mqa::ASSESSMENT_OF,
        dcat_mqa::HAS_DISTRIBUTION_ASSESSMENTS,
        dcat_mqa::CONTAINS_QUALITY_MEASUREMENT,
        dqv::IS_MEASUREMENT_OF,
        dqv::VALUE,
        dcat_mqa::SCORE,
    );
    let mut lines: Vec<String> = execute_query(&store, &q)
        .unwrap()
        .into_iter()
        .filter(|qs| {
            // Measurements are not named nodes, and therefore excluded. A
            // measurement's values are instead part of the line containting the
            // node the measurement is a measurement of.
            qs.get("node").unwrap().is_named_node()
        })
        .map(|qs| {
            format!(
                "{:?} {:?} {:?} {:?} {:?} {:?} {:?} {:?}",
                qs.get("node"),
                qs.get("type"),
                qs.get("modified"),
                qs.get("assessmentOf"),
                qs.get("distributionAssessment"),
                qs.get("metric"),
                qs.get("value"),
                qs.get("score"),
            )
        })
        .collect();
    lines.sort();
    lines
}
