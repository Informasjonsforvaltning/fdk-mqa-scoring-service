use avro_rs::{from_value, schema::Name};
use deadpool_postgres::Pool;
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
use uuid::Uuid;

use crate::{
    database::{create_table, get_graph_by_id, store_graph},
    error::MqaError,
    helpers::load_files,
    measurement_graph::MeasurementGraph,
    schemas::MQAEvent,
    score::calculate_score,
    score_graph::ScoreGraph,
};

pub async fn run_async_processor(
    brokers: String,
    group_id: String,
    input_topic: String,
    sr_settings: SrSettings,
    pool: Pool,
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
            let pool = pool.clone();
            async move {
                let message = borrowed_message.detach();
                tokio::spawn(async move {
                    match handle_message(message, decoder, pool).await {
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

async fn handle_message(
    message: OwnedMessage,
    decoder: AvroDecoder<'_>,
    pool: Pool,
) -> Result<(), MqaError> {
    if let Some(event) = parse_event(message, decoder).await? {
        handle_event(event, pool).await
    } else {
        Ok(())
    }
}

async fn handle_event(event: MQAEvent, pool: Pool) -> Result<(), MqaError> {
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

    let fnames = vec![
        "graphs/dcatno-mqa-vocabulary.ttl",
        "graphs/dcatno-mqa-vocabulary-default-score-values.ttl",
    ];
    let vocab = load_files(fnames)?.join("\n");
    let score = measurement_graph.to_string()?;
    store_graph(&client, &fdk_id, score, vocab).await?;

    Ok(())
}
