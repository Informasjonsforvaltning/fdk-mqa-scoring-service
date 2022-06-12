use fdk_mqa_scoring_service::{
    database::{migrate_database, PgPool},
    kafka::INPUT_TOPIC,
    schemas::{MQAEvent, MQAEventType},
};
use kafka_utils::{process_single_message, TestProducer};
use utils::sorted_lines;
use uuid::Uuid;

mod kafka_utils;
mod utils;

#[tokio::test]
async fn score() {
    assert_transformation(
        r#"
            <https://dataset.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
            <https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.a>  .
            <https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.b>  .
            <https://distribution.a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
            <https://distribution.b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
            <https://dataset.foo> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:a .
            <https://distribution.a> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:b .
            <https://distribution.a> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:c .
            <https://distribution.b> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:d .
            _:a <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:a <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability> .
            _:b <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:b <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:b <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode> .
            _:c <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:c <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:c <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability> .
            _:d <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:d <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability> .
        "#,
        include_str!("expected.ttl"),
    ).await;
}

async fn assert_transformation(input: &str, expected: &str) {
    migrate_database().unwrap();

    let uuid = Uuid::new_v4();
    let input_message = MQAEvent {
        event_type: MQAEventType::PropertiesChecked,
        timestamp: 1647698566000,
        fdk_id: uuid.to_string(),
        graph: input.to_string(),
    };

    let pool = PgPool::new().unwrap();

    // Start async node-namer process
    let processor = process_single_message(pool.clone());

    // Produce message to node-namer input topic
    TestProducer::new(&INPUT_TOPIC)
        .produce(&input_message, "no.fdk.mqa.MQAEvent")
        .await;

    // Wait for node-namer to process message and assert result is ok
    processor.await.unwrap();

    let mut conn = pool.get().unwrap();
    let graph = conn.get_score_graph_by_id(uuid).unwrap().unwrap();

    assert_eq!(sorted_lines(&graph), sorted_lines(expected));
}
