use std::env;

use avro_rs::schema::Name;
use deadpool_postgres::Pool;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::OwnedMessage,
    ClientConfig, Message,
};
use schema_registry_converter::{
    async_impl::{avro::AvroDecoder, schema_registry::SrSettings},
    avro_common::DecodeResult,
};
use tracing::{Instrument, Level};
use uuid::Uuid;

use crate::{
    database::{create_table, get_graph_by_id, store_graph},
    error::MqaError,
    measurement_graph::MeasurementGraph,
    schemas::MQAEvent,
    score::calculate_score,
    score_graph::ScoreGraph,
};

lazy_static! {
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
        .set("auto.offset.reset", "beginning")
        .set("api.version.request", "false")
        .set("security.protocol", "plaintext")
        .set("debug", "all")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;
    consumer.subscribe(&[&INPUT_TOPIC])?;
    Ok(consumer)
}

pub async fn run_async_processor(
    worker_id: usize,
    sr_settings: SrSettings,
    pool: Pool,
) -> Result<(), MqaError> {
    tracing::info!(worker_id, "starting worker");

    let consumer: StreamConsumer = create_consumer()?;

    tracing::info!(worker_id, "listening for messages");
    consumer
        .stream()
        .try_for_each(|borrowed_message| {
            let sr_settings = sr_settings.clone();
            let pool = pool.clone();
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
                        match handle_message(message, sr_settings, pool).await {
                            Ok(_) => tracing::info!("message handeled successfully"),
                            Err(e) => tracing::info!(
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
    mut decoder: AvroDecoder<'_>,
) -> Result<Option<MQAEvent>, MqaError> {
    match decoder.decode(msg.payload()).await {
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

pub async fn handle_message(
    message: OwnedMessage,
    sr_settings: SrSettings,
    pool: Pool,
) -> Result<(), MqaError> {
    let decoder = AvroDecoder::new(sr_settings);
    if let Some(event) = parse_event(message, decoder).await? {
        let span = tracing::span!(
            Level::INFO,
            "parsed_event",
            fdk_id = event.fdk_id.as_str(),
            event_type = format!("{:?}", event.event_type).as_str(),
        );

        tokio::task::spawn_blocking(move || {
            let _enter = span.enter();
            handle_event(event, pool)
        })
        .await
        .map_err(|e| e.to_string())?
        .await?;
    } else {
        tracing::info!("skipping event");
    }
    Ok(())
}

async fn handle_event(event: MQAEvent, pool: Pool) -> Result<(), MqaError> {
    tracing::info!("handling event");

    // TODO: load one per worker and pass metrics_scores to `handle_event`
    let score_graph = ScoreGraph::load()?;
    let metric_scores = score_graph.scores()?;

    let mut measurement_graph = MeasurementGraph::new()?;
    measurement_graph.load(event.graph)?;

    let fdk_id = Uuid::parse_str(event.fdk_id.as_str())
        .map_err(|e| format!("unable to parse FDK ID: {e}"))?;

    let client = pool.get().await?;
    create_table(&client).await?;
    if let Some(graph) = get_graph_by_id(&client, fdk_id).await? {
        measurement_graph.load(graph)?;
    }

    let (dataset_score, distribution_scores) = calculate_score(&measurement_graph, &metric_scores)?;
    measurement_graph.insert_scores(&vec![dataset_score])?;
    measurement_graph.insert_scores(&distribution_scores)?;

    let vocab = format!(
        "{}\n{}",
        include_str!("../graphs/dcatno-mqa-vocabulary.ttl"),
        include_str!("../graphs/dcatno-mqa-vocabulary-default-score-values.ttl")
    );
    let score = measurement_graph.to_string()?;
    tracing::info!("writing score to database");
    store_graph(&client, &fdk_id, score, vocab).await?;

    Ok(())
}
