use std::time::Duration;

use fdk_mqa_scoring_service::{
    error::Error,
    kafka::{create_consumer, handle_message, BROKERS},
};
use futures::StreamExt;
use rdkafka::{
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

pub async fn process_single_message() -> Result<(), Error> {
    let consumer = create_consumer().unwrap();
    let mut decoder = AvroDecoder::new(sr_settings());

    // Attempt to receive message for 3s before aborting with an error
    let message = tokio::time::timeout(Duration::from_millis(3000), consumer.stream().next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    handle_message(&mut decoder, &message).await
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
