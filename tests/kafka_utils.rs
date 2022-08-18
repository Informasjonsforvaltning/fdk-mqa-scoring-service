use std::time::Duration;

use fdk_mqa_scoring_service::{
    assessment_graph::AssessmentGraph,
    error::Error,
    kafka::{handle_message, BROKERS},
    score_graph::ScoreGraph,
};
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::KafkaError,
    message::BorrowedMessage,
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use schema_registry_converter::{
    async_impl::{
        avro::{AvroDecoder, AvroEncoder},
        schema_registry::SrSettings,
    },
    schema_registry_common::SubjectNameStrategy,
};
use serde::Serialize;

/// Consumes all messages in all subscribed topics and drops their content.
pub async fn consume_all_messages(consumer: &StreamConsumer) -> Result<(), KafkaError> {
    let timeout_duration = Duration::from_millis(500);
    loop {
        match tokio::time::timeout(timeout_duration, consumer.recv()).await {
            // Consume message and commit offset.
            Ok(message) => consumer.commit_message(&message?, CommitMode::Sync)?,
            // Timeout, no more messages to consume.
            Err(_) => return Ok(()),
        }
    }
}

/// Consumes and returns a single message, if received within the timeout period.
pub async fn consume_single_message(
    consumer: &StreamConsumer,
) -> Result<Option<BorrowedMessage>, KafkaError> {
    let timeout_duration = Duration::from_millis(1000);
    match tokio::time::timeout(timeout_duration, consumer.recv()).await {
        Ok(Ok(message)) => {
            consumer.commit_message(&message, CommitMode::Sync)?;
            Ok(Some(message))
        }
        Ok(Err(e)) => Err(e),
        // Timeout.
        Err(_) => Ok(None),
    }
}
pub async fn process_single_message(consumer: StreamConsumer) -> Result<(), Error> {
    let mut decoder = AvroDecoder::new(sr_settings());
    let score_definitions = ScoreGraph::new()?.scores()?;
    let assessment_graph = AssessmentGraph::new()?;
    let http_client = reqwest::Client::new();

    // Attempt to receive message for 3s before aborting with an error
    let message = consume_single_message(&consumer)
        .await?
        .expect("no message received");

    handle_message(
        &mut decoder,
        &score_definitions,
        &assessment_graph,
        &http_client,
        &message,
    )
    .await
}

pub fn sr_settings() -> SrSettings {
    let schema_registry = "http://localhost:8081";
    SrSettings::new_builder(schema_registry.to_string())
        .set_timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

pub struct TestProducer<'a> {
    producer: FutureProducer,
    encoder: AvroEncoder<'a>,
    topic: &'static str,
}

impl TestProducer<'_> {
    pub fn new(topic: &'static str) -> Self {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", BROKERS.clone())
            .create::<FutureProducer>()
            .expect("Failed to create Kafka FutureProducer");

        let encoder = AvroEncoder::new(sr_settings());
        Self {
            producer,
            encoder,
            topic,
        }
    }

    pub async fn produce<I: Serialize>(&mut self, item: I, schema: &str) {
        let encoded = self
            .encoder
            .encode_struct(
                item,
                &SubjectNameStrategy::RecordNameStrategy(schema.to_string()),
            )
            .await
            .unwrap();
        let record: FutureRecord<String, Vec<u8>> = FutureRecord::to(self.topic).payload(&encoded);
        self.producer
            .send(record, Duration::from_secs(0))
            .await
            .unwrap();
    }
}
