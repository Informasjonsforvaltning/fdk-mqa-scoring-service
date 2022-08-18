use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use fdk_mqa_scoring_service::{
    json_conversion::Scores,
    kafka::INPUT_TOPIC,
    schemas::{MqaEvent, MqaEventType},
};
use httptest::{
    matchers::{all_of, json_decoded, request, ExecutionContext, Matcher},
    responders::status_code,
    Expectation, ServerBuilder,
};
use kafka_utils::{process_single_message, TestProducer};
use serde::{Deserialize, Serialize};
use utils::sorted_lines;
use uuid::Uuid;

mod kafka_utils;
mod utils;

#[tokio::test]
async fn score() {
    assert_transformation(
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
            <https://distribution.assessment.a> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:c .
            <https://distribution.assessment.b> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:d .
            _:a <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:a <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability> .
            _:b <http://www.w3.org/ns/dqv#value> "200"^^<http://www.w3.org/2001/XMLSchema#int> .
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
        include_str!("expected.json"),
    ).await;
}

#[derive(Debug)]
pub struct ExpectedRequestContent {
    turtle: String,
    scores: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateRequest {
    pub turtle_assessment: String,
    pub jsonld_assessment: String,
    pub scores: Scores,
}

impl Matcher<UpdateRequest> for ExpectedRequestContent {
    fn matches(&mut self, update: &UpdateRequest, _ctx: &mut ExecutionContext) -> bool {
        assert_eq!(
            sorted_lines(&update.turtle_assessment),
            sorted_lines(&self.turtle)
        );
        assert_eq!(
            update.scores,
            serde_json::from_str::<Scores>(&self.scores).unwrap()
        );
        true
    }

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <Self as fmt::Debug>::fmt(self, f)
    }
}

async fn assert_transformation(input: &str, expected_ttl: &str, expected_json: &str) {
    let uuid = Uuid::new_v4();
    let input_message = MqaEvent {
        event_type: MqaEventType::PropertiesChecked,
        timestamp: 1647698566000,
        fdk_id: uuid.to_string(),
        graph: input.to_string(),
    };

    // Start async node-namer process
    let processor = process_single_message();

    // Create mock mqa scoring api server
    let mut server = ServerBuilder::new()
        .bind_addr(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8082,
        ))
        .run()
        .unwrap();

    server.expect(
        Expectation::matching(all_of![
            request::method("GET"),
            request::path(format!("/api/assessments/{}", uuid)),
        ])
        .respond_with(status_code(404)),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path(format!("/api/assessments/{}", uuid)),
            request::body(json_decoded::<UpdateRequest, ExpectedRequestContent>(
                ExpectedRequestContent {
                    turtle: expected_ttl.to_string(),
                    scores: expected_json.to_string(),
                }
            )),
        ])
        .respond_with(status_code(202)),
    );

    // Produce message to node-namer input topic
    TestProducer::new(&INPUT_TOPIC)
        .produce(&input_message, "no.fdk.mqa.MQAEvent")
        .await;

    // Wait for node-namer to process message and assert result is ok
    processor.await.unwrap();
    server.verify_and_clear();
}
