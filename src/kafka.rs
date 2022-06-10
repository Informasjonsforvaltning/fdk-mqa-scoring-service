use std::env;

use avro_rs::schema::Name;
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
use uuid::Uuid;

use crate::{
    database::PgPool, error::MqaError, measurement_graph::MeasurementGraph, models::Graph,
    schemas::MQAEvent, score::calculate_score, score_graph::ScoreGraph,
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

pub async fn run_async_processor(sr_settings: SrSettings, pool: PgPool) -> Result<(), MqaError> {
    let consumer: StreamConsumer = create_consumer()?;

    consumer
        .stream()
        .try_for_each(|borrowed_message| {
            let sr_settings = sr_settings.clone();
            let pool = pool.clone();
            async move {
                let message = borrowed_message.detach();
                tokio::spawn(async move {
                    match handle_message(message, sr_settings, pool).await {
                        Ok(_) => println!("ok"),
                        Err(e) => println!("Error: {:?}", e),
                    };
                });
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
    pool: PgPool,
) -> Result<(), MqaError> {
    let decoder = AvroDecoder::new(sr_settings);
    if let Some(event) = parse_event(message, decoder).await? {
        tokio::task::spawn_blocking(|| handle_event(event, pool))
            .await
            .map_err(|e| e.to_string())?
            .await?;
    }
    Ok(())
}

async fn handle_event(event: MQAEvent, pool: PgPool) -> Result<(), MqaError> {
    // TODO: load one per worker and pass metrics_scores to `handle_event`
    let score_graph = ScoreGraph::load()?;
    let metric_scores = score_graph.scores()?;

    let mut measurement_graph = MeasurementGraph::new()?;
    measurement_graph.load(event.graph)?;

    let fdk_id = Uuid::parse_str(event.fdk_id.as_str())
        .map_err(|e| format!("unable to parse FDK ID: {e}"))?;

    let mut conn = pool.get()?;
    if let Some(graph) = conn.get_score_graph_by_id(fdk_id)? {
        measurement_graph.load(graph)?;
    }

    let (dataset_score, distribution_scores) = calculate_score(&measurement_graph, &metric_scores)?;
    measurement_graph.insert_scores(&vec![dataset_score])?;
    measurement_graph.insert_scores(&distribution_scores)?;

    let graph = Graph {
        fdk_id: fdk_id.to_string(),
        score: measurement_graph.to_string()?,
        vocab: format!(
            "{}\n{}",
            include_str!("../graphs/dcatno-mqa-vocabulary.ttl"),
            include_str!("../graphs/dcatno-mqa-vocabulary-default-score-values.ttl")
        ),
    };
    conn.store_graph(graph)?;

    Ok(())
}
