use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use fdk_mqa_scoring_service::{
    json_conversion::Scores,
    kafka::{create_consumer, INPUT_TOPIC},
    schemas::{MqaEvent, MqaEventType},
};
use httptest::{
    matchers::{all_of, json_decoded, request, ExecutionContext, Matcher},
    responders::status_code,
    Expectation, Server, ServerBuilder,
};
use kafka_utils::{consume_all_messages, process_single_message, TestProducer};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::utils::comparable_turtle_content;

mod kafka_utils;
mod utils;

#[tokio::test]
async fn test() {
    let mut server = ServerBuilder::new()
        .bind_addr(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8082,
        ))
        .run()
        .unwrap();

    assert_transformation(
        &server,
        r#"
            <https://dataset.assessment.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DatasetAssessment> .
            <https://dataset.assessment.foo> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://dataset.foo> .
            <https://dataset.assessment.foo> <https://data.norge.no/vocabulary/dcatno-mqa#hasDistributionAssessment> <https://distribution.assessment.a>  .
            <https://dataset.assessment.foo> <https://data.norge.no/vocabulary/dcatno-mqa#hasDistributionAssessment> <https://distribution.assessment.b>  .
            <https://distribution.assessment.a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DistributionAssessment> .
            <https://distribution.assessment.b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DistributionAssessment> .
            <https://distribution.assessment.a> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://distribution.a> .
            <https://distribution.assessment.b> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://distribution.b> .
            <https://dataset.assessment.foo> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:a .
            <https://distribution.assessment.a> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:b .
            _:a <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:a <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability> .
            _:b <http://www.w3.org/ns/dqv#value> "200"^^<http://www.w3.org/2001/XMLSchema#int> .
            _:b <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:b <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode> .
        "#,
        None,
        include_str!("data/new_dataset/assessment.ttl"),
        include_str!("data/new_dataset/scores.json"),
    ).await;

    assert_transformation(
        &server,
        r#"
            <https://dataset.assessment.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DatasetAssessment> .
            <https://dataset.assessment.foo> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://dataset.foo> .
            <https://dataset.assessment.foo> <https://data.norge.no/vocabulary/dcatno-mqa#hasDistributionAssessment> <https://distribution.assessment.a>  .
            <https://dataset.assessment.foo> <https://data.norge.no/vocabulary/dcatno-mqa#hasDistributionAssessment> <https://distribution.assessment.b>  .
            <https://distribution.assessment.a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DistributionAssessment> .
            <https://distribution.assessment.b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DistributionAssessment> .
            <https://distribution.assessment.a> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://distribution.a> .
            <https://distribution.assessment.b> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://distribution.b> .
            <https://distribution.assessment.a> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:c .
            <https://distribution.assessment.b> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:d .
            _:c <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:c <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:c <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability> .
            _:d <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:d <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability> .
        "#,
        Some(include_str!("data/second_half_of_dataset/api_response.ttl")),
        include_str!("data/second_half_of_dataset/assessment.ttl"),
        include_str!("data/second_half_of_dataset/scores.json"),
    ).await;

    // Assert that scoring api received expected requests.
    server.verify_and_clear();
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateRequest {
    pub turtle_assessment: String,
    pub scores: Scores,
}

impl Matcher<UpdateRequest> for UpdateRequest {
    fn matches(&mut self, update: &UpdateRequest, _ctx: &mut ExecutionContext) -> bool {
        println!("{}", update.turtle_assessment);
        println!("{}", serde_json::to_string(&update.scores).unwrap());
        assert_eq!(
            comparable_turtle_content(&update.turtle_assessment),
            comparable_turtle_content(&self.turtle_assessment)
        );
        assert_eq!(update.scores, self.scores);
        true
    }

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <Self as fmt::Debug>::fmt(self, f)
    }
}

async fn assert_transformation(
    server: &Server,
    input: &str,
    api_response: Option<&'static str>,
    expected_ttl: &str,
    expected_json: &str,
) {
    let consumer = create_consumer().unwrap();
    // Clear topic of all existing messages.
    consume_all_messages(&consumer).await.unwrap();
    // Start async node-namer process.
    let processor = process_single_message(consumer);

    // Create MQA test event.
    let uuid = Uuid::new_v4();
    let input_message = MqaEvent {
        event_type: MqaEventType::PropertiesChecked,
        timestamp: 1647698566000,
        fdk_id: uuid.to_string(),
        graph: input.to_string(),
    };

    // Configure scoring api responses.
    match api_response {
        // Simulate existing event beeing processed.
        Some(graph) => server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("/api/assessments/{}", uuid)),
            ])
            .respond_with(status_code(200).body(graph)),
        ),
        // Dataset never processed before.
        None => server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path(format!("/api/assessments/{}", uuid)),
            ])
            .respond_with(status_code(404)),
        ),
    }
    server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path(format!("/api/assessments/{}", uuid)),
            request::body(json_decoded::<UpdateRequest, UpdateRequest>(
                UpdateRequest {
                    turtle_assessment: expected_ttl.to_string(),
                    scores: serde_json::from_str(expected_json).unwrap(),
                }
            )),
        ])
        .respond_with(status_code(202)),
    );

    // Produce message to topic.
    TestProducer::new(&INPUT_TOPIC)
        .produce(&input_message, "no.fdk.mqa.MQAEvent")
        .await;

    // Wait for node-namer to process message and assert result is ok.
    processor.await.unwrap();
}
