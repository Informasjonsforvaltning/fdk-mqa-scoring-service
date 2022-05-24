use avro_rs::{from_value, schema::Name};
use futures::TryStreamExt;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    message::OwnedMessage,
    ClientConfig, Message,
};
use schema_registry_converter::{
    async_impl::{avro::AvroDecoder, schema_registry::SrSettings},
    avro_common::DecodeResult,
};

use crate::{
    error::MqaError, schemas::MQAEvent, score::parse_graph_and_calculate_score,
    score_graph::ScoreGraph,
};

pub async fn run_async_processor(
    brokers: String,
    group_id: String,
    input_topic: String,
    sr_settings: SrSettings,
) -> Result<(), MqaError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "beginning")
        .set("api.version.request", "false")
        .set("security.protocol", "plaintext")
        .set("debug", "all")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;

    consumer.subscribe(&[&input_topic])?;
    consumer
        .stream()
        .try_for_each(|borrowed_message| {
            let decoder = AvroDecoder::new(sr_settings.clone());
            async move {
                let message = borrowed_message.detach();
                tokio::spawn(async move {
                    match handle_message(message, decoder).await {
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
            from_value::<MQAEvent>(&value).map_err(|e| e.to_string())?,
        )),
        Ok(_) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn handle_message(message: OwnedMessage, decoder: AvroDecoder<'_>) -> Result<(), MqaError> {
    if let Some(event) = parse_event(message, decoder).await? {
        handle_event(event).await
    } else {
        Ok(())
    }
}

async fn handle_event(event: MQAEvent) -> Result<(), MqaError> {
    // TODO: load one per worker and pass metrics_scores to `handle_event`
    let score_graph = ScoreGraph::load()?;
    let metric_scores = score_graph.scores()?;

    let scored_graph = parse_graph_and_calculate_score(event.graph, &metric_scores)?;

    // TODO: save in postgres
    println!("{}", scored_graph);

    Ok(())
}
