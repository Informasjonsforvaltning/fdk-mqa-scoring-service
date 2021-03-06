use std::{env, time::Duration};

use avro_rs::schema::Name;
use futures::{lock::Mutex, TryStreamExt};
use lazy_static::lazy_static;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::OwnedMessage,
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
    static ref SR_SETTINGS: SrSettings = {
        let mut schema_registry_urls = SCHEMA_REGISTRY.split(",");
        let mut sr_settings_builder =
            SrSettings::new_builder(schema_registry_urls.next().unwrap().to_string());
        schema_registry_urls.for_each(|url| {
            sr_settings_builder.add_url(url.to_string());
        });

        let sr_settings = sr_settings_builder
            .set_timeout(Duration::from_secs(30))
            .build()
            .unwrap();

        sr_settings
    };
    static ref AVRO_DECODER: Mutex<AvroDecoder<'static>> =
        Mutex::new(AvroDecoder::new(SR_SETTINGS.clone()));
}

pub fn create_consumer() -> Result<StreamConsumer, KafkaError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "fdk-mqa-scoring-service")
        .set("bootstrap.servers", BROKERS.clone())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "beginning")
        .set("api.version.request", "false")
        .set("security.protocol", "plaintext")
        .set("debug", "all")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    consumer.subscribe(&[&INPUT_TOPIC])?;
    Ok(consumer)
}

pub async fn run_async_processor(worker_id: usize) -> Result<(), Error> {
    tracing::info!(worker_id, "starting worker");
    let consumer: StreamConsumer = create_consumer()?;

    tracing::info!(worker_id, "listening for messages");
    consumer
        .stream()
        .try_for_each(|borrowed_message| {
            let message = borrowed_message.detach();
            let span = tracing::span!(
                Level::INFO,
                "received_message",
                topic = message.topic(),
                partition = message.partition(),
                offset = message.offset(),
                timestamp = message.timestamp().to_millis(),
            );

            async move {
                tokio::spawn(
                    async move {
                        match handle_message(message).await {
                            Ok(_) => tracing::info!("message handeled successfully"),
                            Err(e) => tracing::error!(
                                error = e.to_string().as_str(),
                                "failed while handling message"
                            ),
                        };
                    }
                    .instrument(span),
                );
                Ok(())
            }
        })
        .await?;

    Ok(())
}

async fn parse_event(
    msg: OwnedMessage,
) -> Result<Option<MQAEvent>, Error> {
    match AVRO_DECODER.lock().await.decode(msg.payload()).await {
        Ok(DecodeResult {
            name:
                Some(Name {
                    name,
                    namespace: Some(namespace),
                    ..
                }),
            value,
        }) if name == "MQAEvent" && namespace == "no.fdk.mqa" => Ok(Some(
            avro_rs::from_value::<MQAEvent>(&value).map_err(|e| e.to_string())?,
        )),
        Ok(_) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub async fn handle_message(message: OwnedMessage) -> Result<(), Error> {
    if let Some(event) = parse_event(message).await? {
        let span = tracing::span!(
            Level::INFO,
            "parsed_event",
            fdk_id = event.fdk_id.as_str(),
            event_type = format!("{:?}", event.event_type).as_str(),
        );

        tokio::task::spawn_blocking(move || handle_event(event).instrument(span))
            .await
            .map_err(|e| e.to_string())?
            .await?;
    } else {
        tracing::info!("skipping event");
    }
    Ok(())
}

async fn handle_event(event: MQAEvent) -> Result<(), Error> {
    tracing::info!("handling event");

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
            assessment_graph.clear()?;
        } else if current_timestamp > event.timestamp {
            return Ok(());
        }
    }

    assessment_graph.load(event.graph)?;
    assessment_graph.insert_modified_timestmap(event.timestamp)?;

    let (dataset_score, distribution_scores) =
        calculate_score(&assessment_graph, &score_definitions)?;
    let scores = convert_scores(&score_definitions, &dataset_score, &distribution_scores);

    assessment_graph.insert_scores(&vec![dataset_score])?;
    assessment_graph.insert_scores(&distribution_scores)?;

    tracing::info!("posting scores");
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
        .get(format!("{}/api/graphs/{fdk_id}", SCORING_API_URL.clone()))
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
