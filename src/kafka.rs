use std::{
    env,
    time::{Duration, Instant},
};

use apache_avro::schema::Name;
use futures::future::ok;
use lazy_static::lazy_static;
use rdkafka::{
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
    metrics::{PROCESSED_MESSAGES, PROCESSING_TIME},
    schemas::{InputEvent, MqaEvent, MqaEventType},
    score::calculate_score,
    score_graph::{ScoreDefinitions, ScoreGraph},
};

lazy_static! {
    pub static ref BROKERS: String = env::var("BROKERS").unwrap_or("localhost:9092".to_string());
    pub static ref SCHEMA_REGISTRY: String =
        env::var("SCHEMA_REGISTRY").unwrap_or("http://localhost:8081".to_string());
    pub static ref INPUT_TOPIC: String =
        env::var("INPUT_TOPIC").unwrap_or("mqa-events".to_string());
    pub static ref SCORING_API_URL: String =
        env::var("SCORING_API_URL").unwrap_or("http://localhost:8082".to_string());
    pub static ref SCORING_API_KEY: String = env::var("API_KEY").unwrap_or_default();
}

pub fn create_sr_settings() -> Result<SrSettings, Error> {
    let mut schema_registry_urls = SCHEMA_REGISTRY.split(",");

    let mut sr_settings_builder =
        SrSettings::new_builder(schema_registry_urls.next().unwrap_or_default().to_string());
    schema_registry_urls.for_each(|url| {
        sr_settings_builder.add_url(url.to_string());
    });

    let sr_settings = sr_settings_builder
        .set_timeout(Duration::from_secs(30))
        .build()?;
    Ok(sr_settings)
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
        .set("max.partition.fetch.bytes", "2097152")
        .create()?;
    consumer.subscribe(&[&INPUT_TOPIC])?;
    Ok(consumer)
}

pub async fn run_async_processor(worker_id: usize, sr_settings: SrSettings) -> Result<(), Error> {
    tracing::info!(worker_id, "starting worker");

    let consumer: StreamConsumer = create_consumer()?;
    let mut decoder = AvroDecoder::new(sr_settings);
    let score_definitions = ScoreGraph::new()?.scores()?;
    let http_client = reqwest::Client::new();

    tracing::info!(worker_id, "listening for messages");
    loop {
        let assessment_graph = AssessmentGraph::new()?;

        let message = consumer.recv().await?;
        let span = tracing::span!(
            Level::INFO,
            "message",
            // topic = message.topic(),
            partition = message.partition(),
            offset = message.offset(),
            timestamp = message.timestamp().to_millis(),
        );

        receive_message(
            &consumer,
            &mut decoder,
            &score_definitions,
            &assessment_graph,
            &http_client,
            &message,
        )
        .instrument(span)
        .await;
    }
}

async fn receive_message(
    consumer: &StreamConsumer,
    decoder: &mut AvroDecoder<'_>,
    score_definitions: &ScoreDefinitions,
    assessment_graph: &AssessmentGraph,
    http_client: &reqwest::Client,
    message: &BorrowedMessage<'_>,
) {
    let start_time = Instant::now();
    let mut attempts = 0;
    let mut result: Result<(), Error> = Err("handle_message not attempted".into());

    for _ in 0..5 {
        attempts += 1;
        result = handle_message(
            decoder,
            score_definitions,
            assessment_graph,
            http_client,
            message,
        )
        .await;

        if let Ok(_) = result {
            break;
        }
        tokio::time::sleep(Duration::from_millis(3000)).await;
    }
    let elapsed_millis = start_time.elapsed().as_millis();

    match result {
        Ok(_) => {
            tracing::info!(elapsed_millis, attempts, "message handled successfully");
            PROCESSED_MESSAGES.with_label_values(&["success"]).inc();

            if let Err(e) = consumer.store_offset_from_message(&message) {
                tracing::warn!(error = e.to_string(), "failed to store offset");
            };
        }
        Err(e) => {
            tracing::error!(
                elapsed_millis,
                attempts,
                error = e.to_string(),
                "failed while handling message"
            );
            PROCESSED_MESSAGES.with_label_values(&["error"]).inc();
        }
    };
    PROCESSING_TIME.observe(elapsed_millis as f64 / 1000.0);
}

pub async fn handle_message(
    decoder: &mut AvroDecoder<'_>,
    score_definitions: &ScoreDefinitions,
    assessment_graph: &AssessmentGraph,
    http_client: &reqwest::Client,
    message: &BorrowedMessage<'_>,
) -> Result<(), Error> {
    match decode_message(decoder, message).await? {
        InputEvent::MqaEvent(event) => {
            let span = tracing::span!(
                Level::INFO,
                "event",
                fdk_id = event.fdk_id.as_str(),
                event_type = format!("{:?}", event.event_type).as_str(),
            );

            handle_mqa_event(score_definitions, assessment_graph, http_client, event)
                .instrument(span)
                .await
                .map_err(|e| e.to_string())?;
        }
        InputEvent::Unknown { namespace, name } => {
            tracing::warn!(namespace, name, "skipping unknown event");
        }
    }
    Ok(())
}

async fn decode_message(
    decoder: &mut AvroDecoder<'_>,
    message: &BorrowedMessage<'_>,
) -> Result<InputEvent, Error> {
    match decoder.decode(message.payload()).await? {
        DecodeResult {
            name:
                Some(Name {
                    name,
                    namespace: Some(namespace),
                    ..
                }),
            value,
        } => {
            let event = match (namespace.as_str(), name.as_str()) {
                ("no.fdk.mqa", "MQAEvent") => {
                    InputEvent::MqaEvent(apache_avro::from_value::<MqaEvent>(&value)?)
                }
                _ => InputEvent::Unknown { namespace, name },
            };
            Ok(event)
        }
        _ => Err("unable to identify event without namespace and name".into()),
    }
}

async fn handle_mqa_event(
    score_definitions: &ScoreDefinitions,
    assessment_graph: &AssessmentGraph,
    http_client: &reqwest::Client,
    event: MqaEvent,
) -> Result<(), Error> {
    match event.event_type {
        MqaEventType::PropertiesChecked
        | MqaEventType::UrlsChecked
        | MqaEventType::DcatComplienceChecked => {
            let fdk_id = Uuid::parse_str(event.fdk_id.as_str())
                .map_err(|e| format!("unable to parse FDK ID: {e}"))?;

            if let Some(graph) = get_graph(&http_client, &fdk_id).await? {
                assessment_graph.load(graph)?;

                let current_timestamp = assessment_graph.get_modified_timestmap();
                
                match current_timestamp {
                    Some(timestamp) => {
                        if timestamp < event.timestamp {
                            tracing::debug!(
                                existing_timestamp = timestamp,
                                event_timestamp = event.timestamp,
                                "overriding existing assessment"
                            );
                        } else if timestamp > event.timestamp {
                            tracing::debug!(
                                existing_timestamp = timestamp,
                                event_timestamp = event.timestamp,
                                "skipping outdated assessment event"
                            );
                            return Ok(());
                        } else {
                            tracing::debug!(
                                existing_timestamp = timestamp,
                                event_timestamp = event.timestamp,
                                "merging with existing assessment"
                            );
                        }
                    }
                    None => {
                        tracing::debug!(
                            fdk_id = %fdk_id,
                            "no existing timestamp found, treating as new assessment"
                        );
                    }
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
            let turtle_assessment = assessment_graph.to_turtle()?;
            let jsonld_assessment = assessment_graph.turtle_to_jsonld(&turtle_assessment)?;

            post_scores(
                &http_client,
                &fdk_id,
                UpdateRequest {
                    scores,
                    turtle_assessment,
                    jsonld_assessment,
                },
            )
            .await
        }
        MqaEventType::Unknown => Err(format!("unknown MqaEventType").into()),
    }
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
        if response.status() == StatusCode::PAYLOAD_TOO_LARGE {
            tracing::warn!(payload = format!("{:?}", update), "payload too large");
        }
        Err(format!(
            "Invalid response from scoring api: {} - {}",
            response.status(),
            response.text().await?
        )
        .into())
    }
}
