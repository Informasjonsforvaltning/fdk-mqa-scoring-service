use std::env;

use avro_rs::schema::Name;
use lazy_static::lazy_static;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::BorrowedMessage,
    ClientConfig, Message,
};
use reqwest::StatusCode;
use schema_registry_converter::{
    async_impl::{avro::AvroDecoder, schema_registry::SrSettings},
    avro_common::DecodeResult,
};
use tracing::{Instrument, Level};
use uuid::Uuid;

use crate::{
    assessment_graph::AssessmentGraph,
    error::Error,
    json_conversion::{convert_scores, UpdateRequest},
    schemas::MQAEvent,
    score::calculate_score,
    score_graph::ScoreGraph,
};

lazy_static! {
    pub static ref SCORING_API_URL: String =
        env::var("SCORING_API_URL").unwrap_or("http://localhost:8082".to_string());
    pub static ref SCORING_API_KEY: String = env::var("API_KEY").unwrap_or_default();
    pub static ref BROKERS: String = env::var("BROKERS").unwrap_or("localhost:9092".to_string());
    pub static ref SCHEMA_REGISTRY: String =
        env::var("SCHEMA_REGISTRY").unwrap_or("http://localhost:8081".to_string());
    pub static ref INPUT_TOPIC: String =
        env::var("INPUT_TOPIC").unwrap_or("mqa-events".to_string());
}

pub fn create_consumer() -> Result<StreamConsumer, KafkaError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "fdk-mqa-scoring-service")
        .set("bootstrap.servers", BROKERS.clone())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "beginning")
        .set("api.version.request", "false")
        .set("security.protocol", "plaintext")
        .set("debug", "all")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    consumer.subscribe(&[&INPUT_TOPIC])?;
    Ok(consumer)
}

pub async fn run_async_processor(worker_id: usize, sr_settings: SrSettings) -> Result<(), Error> {
    tracing::info!(worker_id, "starting worker");
    
    let consumer: StreamConsumer = create_consumer()?;
    let mut decoder = AvroDecoder::new(sr_settings);

    tracing::info!(worker_id, "listening for messages");
    loop {
        let message = consumer.recv().await?;
        let span = tracing::span!(
            Level::INFO,
            "message",
            // topic = message.topic(),
            // partition = message.partition(),
            offset = message.offset(),
            kafka_timestamp = message.timestamp().to_millis(),
        );

        receive_message(&consumer, &mut decoder, &message)
            .instrument(span)
            .await;
    }
}

async fn receive_message(
    consumer: &StreamConsumer,
    decoder: &mut AvroDecoder<'_>,
    message: &BorrowedMessage<'_>,
) {
    match handle_message(decoder, message).await {
        Ok(_) => {
            tracing::info!("message handled successfully");
        }
        Err(e) => tracing::error!(
            error = e.to_string().as_str(),
            "failed while handling message"
        ),
    };
    if let Err(e) = consumer.store_offset_from_message(&message) {
        tracing::warn!(error = e.to_string().as_str(), "failed to store offset");
    };
}

pub async fn handle_message(
    decoder: &mut AvroDecoder<'_>,
    message: &BorrowedMessage<'_>,
) -> Result<(), Error> {
    if let Some(event) = decode_message(decoder, message).await? {
        let span = tracing::span!(
            Level::INFO,
            "event",
            fdk_id = event.fdk_id.as_str(),
            event_type = format!("{:?}", event.event_type).as_str(),
        );

        tokio::task::spawn_blocking(|| handle_event(event).instrument(span))
            .await
            .map_err(|e| e.to_string())?
            .await
    } else {
        tracing::info!("skipping event");
        Ok(())
    }
}

async fn decode_message(
    decoder: &mut AvroDecoder<'_>,
    message: &BorrowedMessage<'_>,
) -> Result<Option<MQAEvent>, Error> {
    match decoder.decode(message.payload()).await {
        Ok(DecodeResult {
            name:
                Some(Name {
                    name,
                    namespace: Some(namespace),
                    ..
                }),
            value,
        }) if namespace == "no.fdk.mqa" && name == "MQAEvent" => {
            let event = avro_rs::from_value(&value)?;
            Ok(Some(event))
        }
        Ok(_) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn handle_event(event: MQAEvent) -> Result<(), Error> {
    // TODO: load one per worker and pass metrics_scores to `handle_event`
    let score_graph = ScoreGraph::new()?;
    let score_definitions = score_graph.scores()?;

    let fdk_id = Uuid::parse_str(event.fdk_id.as_str())
        .map_err(|e| format!("unable to parse FDK ID: {e}"))?;

    let client = reqwest::Client::new();
    let mut assessment_graph = AssessmentGraph::new()?;
    if let Some(graph) = get_graph(&client, &fdk_id).await? {
        assessment_graph.load(graph)?;

        let current_timestamp = assessment_graph.get_modified_timestmap()?;

        if current_timestamp < event.timestamp {
            tracing::debug!(
                existing_timestamp = current_timestamp,
                event_timestamp = event.timestamp,
                "overriding existing assessment"
            );
            assessment_graph.clear()?;
        } else if current_timestamp > event.timestamp {
            tracing::debug!(
                existing_timestamp = current_timestamp,
                event_timestamp = event.timestamp,
                "skipping outdated assessment event"
            );
            return Ok(());
        } else {
            tracing::debug!(
                existing_timestamp = current_timestamp,
                event_timestamp = event.timestamp,
                "merging with existing assessment"
            );
        }
    } else {
        tracing::debug!("saving new assessment");
    }

    assessment_graph.load(event.graph)?;
    assessment_graph.insert_modified_timestmap(event.timestamp)?;

    let (dataset_score, distribution_scores) =
        calculate_score(&assessment_graph, &score_definitions)?;
    let scores = convert_scores(&score_definitions, &dataset_score, &distribution_scores);

    assessment_graph.insert_scores(&vec![dataset_score])?;
    assessment_graph.insert_scores(&distribution_scores)?;

    tracing::debug!("posting assessment to api");
    post_scores(
        &client,
        &fdk_id,
        UpdateRequest {
            scores,
            turtle_assessment: assessment_graph.to_turtle()?,
            jsonld_assessment: assessment_graph.to_jsonld()?,
        },
    )
    .await
}

async fn get_graph(client: &reqwest::Client, fdk_id: &Uuid) -> Result<Option<String>, Error> {
    let response = client
        .get(format!(
            "{}/api/assessments/{fdk_id}",
            SCORING_API_URL.clone()
        ))
        .send()
        .await?;

    match response.status() {
        StatusCode::NOT_FOUND => Ok(None),
        StatusCode::OK => Ok(Some(response.text().await?)),
        _ => Err(format!(
            "Invalid response from scoring api: {} - {}",
            response.status(),
            response.text().await?
        )
        .into()),
    }
}

async fn post_scores(
    client: &reqwest::Client,
    fdk_id: &Uuid,
    update: UpdateRequest,
) -> Result<(), Error> {
    let response = client
        .post(format!(
            "{}/api/assessments/{fdk_id}",
            SCORING_API_URL.clone()
        ))
        .header("X-API-KEY", SCORING_API_KEY.clone())
        .json(&update)
        .send()
        .await?;

    if response.status() == StatusCode::ACCEPTED {
        Ok(())
    } else {
        Err(format!(
            "Invalid response from scoring api: {} - {}",
            response.status(),
            response.text().await?
        )
        .into())
    }
}
