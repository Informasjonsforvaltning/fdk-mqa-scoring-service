use std::format;
use std::str;
use std::time::Duration;

use clap::{Arg, Command};

use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};

use log::{error, info};

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;

use avro_rs::from_value;
use schema_registry_converter::blocking::avro::{AvroDecoder, AvroEncoder};
use schema_registry_converter::blocking::schema_registry::SrSettings;

use chrono::{TimeZone, Utc};

use oxigraph::model::*;
use oxigraph::store::{StorageError, Store};

use tmp::utils::setup_logger;

use tmp::vocab::{dcat, dcat_mqa, dcterms, oa};

use tmp::rdf::{
    convert_term_to_named_or_blank_node_ref, get_dataset_node, list_distributions, parse_turtle,
    QualityMeasurementValue,
};
use tmp::schemas::{MQAEvent, MQAEventType};

use tmp::score::*;

mod graph;
mod rdf;
mod schemas;
mod utils;
mod vocab;

async fn record_borrowed_message_receipt(msg: &BorrowedMessage<'_>) {
    // Simulate some work that must be done in the same order as messages are
    // received; i.e., before truly parallel processing can begin.
    info!("Message received: {}", msg.offset());
}

async fn record_owned_message_receipt(_msg: &OwnedMessage) {
    // Like `record_borrowed_message_receipt`, but takes an `OwnedMessage`
    // instead, as in a real-world use case  an `OwnedMessage` might be more
    // convenient than a `BorrowedMessage`.
}

fn parse_mqa_event(msg: OwnedMessage, mut decoder: AvroDecoder) -> Result<MQAEvent, String> {
    match decoder.decode(msg.payload()) {
        Ok(result) => match result.name {
            Some(name) => match name.name.as_str() {
                "MQAEvent" => match name.namespace {
                    Some(namespace) => match namespace.as_str() {
                        "no.fdk.mqa" => match from_value::<MQAEvent>(&result.value) {
                            Ok(event) => Ok(event),
                            Err(e) => Err(format!("Deserialization failed {}", e)),
                        },
                        ns => Err(format!("Unexpected namespace {}", ns)),
                    },
                    None => Err("No namespace in schema, while expected".to_string()),
                },
                name => Err(format!("Unexpected name {}", name)),
            },
            None => Err("No name in schema, while expected".to_string()),
        },
        Err(e) => Err(format!("error getting dataset-event: {}", e)),
    }
}

// Read MQAEvent message
fn handle_mqa_event(msg: OwnedMessage, decoder: AvroDecoder) -> Result<Score, String> {
    info!("Handle MQAEvent on message {}", msg.offset());

    let mqa_event = parse_mqa_event(msg, decoder);

    match mqa_event {
        Ok(event) => {
            let store = parse_turtle(event.graph)?;
            match event.event_type {
                MQAEventType::UrlsChecked => {
                    let dt = Utc.timestamp_millis(event.timestamp);
                    info!(
                        "{} - Processing urls cheked event with timestamp {:?}",
                        event.fdk_id, dt
                    );
                    parse_metrics_graph_and_calculate_accessibility_score(event.fdk_id, event.graph)
                }
                MQAEventType::PropertiesChecked => Ok(Score::default()),
                MQAEventType::DcatComplienceChecked => Ok(Score::default()),
                _ => Ok(Score::default()),
            }
        }
        Err(e) => Err(format!("Unable to decode mqa event: {}", e)),
    }
}

/// Calculates accessibility score
///
/// Returns the score of the highest scoring distribution
fn parse_metrics_graph_and_calculate_accessibility_score(
    fdk_id: String,
    graph: String,
) -> Result<Score, String> {
    match parse_turtle(graph) {
        Ok(store) => match get_dataset_node(&store) {
            Some(dataset_node) => calculate_score(dataset_node.as_ref(), &store),
            None => Err(format!("{} - Dataset node not found in graph", fdk_id)),
        },
        Err(e) => Err(format!("{}", e)),
    }
}

fn status_code_ok(value: QualityMeasurementValue) -> bool {
    match value {
        QualityMeasurementValue::Int(code) => 200 <= code && code < 300,
        _ => false,
    }
}

fn calculate_distribution_score(dist_node: NamedOrBlankNodeRef, metrics_store: &Store) -> Score {
    let mut score = Score::default();

    match event {
        URL => {
            if let Some(dl_availability) = rdf::get_quality_measurement_value(
                dist_node,
                dcat_mqa::DOWNLOAD_URL_AVAILABILITY,
                metrics_store,
            ) {
                score.accessibility.download_url_availability =
                    if dl_availability == QualityMeasurementValue::Bool(true) {
                        20
                    } else {
                        0
                    }
            }
            if let Some(dl_status_code) = rdf::get_quality_measurement_value(
                dist_node,
                dcat_mqa::DOWNLOAD_URL_STATUS_CODE,
                metrics_store,
            ) {
                if status_code_ok(dl_status_code) {
                    score.accessibility.download_url_status_code = 30;
                }
            }
            if let Some(acc_status_code) = rdf::get_quality_measurement_value(
                dist_node,
                dcat_mqa::ACCESS_URL_STATUS_CODE,
                metrics_store,
            ) {
                if status_code_ok(acc_status_code) {
                    score.accessibility.access_url_status_code = 50;
                }
            }
        }
    }

    score
}

fn calculate_score(dataset_node: NamedNodeRef, metrics_store: &Store) -> Result<Score, String> {
    list_distributions(dataset_node, metrics_store)
        .map(|quad| match quad {
            Ok(dist_quad) => {
                match convert_term_to_named_or_blank_node_ref(dist_quad.object.as_ref()) {
                    Some(dist_node) => Ok(calculate_distribution_score(dist_node, metrics_store)),
                    None => Err(format!(
                        "Distribution is not a named or blank node {}",
                        dist_quad.object
                    )),
                }
            }
            Err(e) => Err(format!("Listing distributions failed {}", e)),
        })
        .fold(Ok(Score::default()), |max_score, dist_score| {
            match (max_score, dist_score) {
                (Ok(a), Ok(b)) => {
                    if a.score() > b.score() {
                        Ok(a)
                    } else {
                        Ok(b)
                    }
                }
                (_, Err(err)) => Err(err),
                (Err(err), _) => Err(err),
            }
        })
}

// Creates all the resources and runs the event loop. The event loop will:
//   1) receive a stream of messages from the `StreamConsumer`.
//   2) filter out eventual Kafka errors.
//   3) send the message to a thread pool for processing.
//   4) produce the result to the output topic.
// `tokio::spawn` is used to handle IO-bound tasks in parallel (e.g., producing
// the messages), while `tokio::task::spawn_blocking` is used to handle the
// simulated CPU-bound task.
async fn run_async_processor(
    brokers: String,
    group_id: String,
    input_topic: String,
    output_topic: String,
    sr_settings: SrSettings,
) {
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
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
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&input_topic])
        .expect("Can't subscribe to specified topic");

    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let decoder = AvroDecoder::new(sr_settings.clone());
        let encoder = AvroEncoder::new(sr_settings.clone());
        let producer = producer.clone();
        let output_topic = output_topic.to_string();
        async move {
            // Process each message
            record_borrowed_message_receipt(&borrowed_message).await;
            // Borrowed messages can't outlive the consumer they are received from, so they need to
            // be owned in order to be sent to a separate thread.
            let owned_message = borrowed_message.detach();
            record_owned_message_receipt(&owned_message).await;
            tokio::spawn(async move {
                // The body of this block will be executed on the main thread pool,
                // but we perform `expensive_computation` on a separate thread pool
                // for CPU-intensive tasks via `tokio::task::spawn_blocking`.
                let mqa_event =
                    tokio::task::spawn_blocking(|| handle_mqa_event(owned_message, decoder))
                        .await
                        .expect("failed to wait for handle dataset-event");
            });
            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
}

#[tokio::main]
async fn main() {
    let matches = Command::new("fdk-mqa-scoring-service")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("FDK MQA Scoring service")
        .arg(
            Arg::new("brokers")
                .short('b')
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::new("group-id")
                .short('g')
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("fdk-mqa-property-checker"),
        )
        .arg(
            Arg::new("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::new("input-topic")
                .long("input-topic")
                .help("Input topic")
                .takes_value(true)
                .default_value("dataset-events"),
        )
        .arg(
            Arg::new("num-workers")
                .long("num-workers")
                .help("Number of workers")
                .takes_value(true)
                .default_value("1"),
        )
        .arg(
            Arg::new("schema-registry")
                .long("schema-registry")
                .help("Schema registry')")
                .takes_value(true)
                .default_value("http://localhost:8081"),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let brokers = matches.value_of("brokers").unwrap_or("");
    let group_id = matches.value_of("group-id").unwrap_or("");
    let input_topic = matches.value_of("input-topic").unwrap_or("");
    let output_topic = matches.value_of("output-topic").unwrap_or("");
    let num_workers = matches.value_of_t("num-workers").unwrap_or(0);
    let schema_registry = matches.value_of("schema-registry").unwrap_or("");

    info!("Using following settings:");
    info!("  brokers:         {}", brokers);
    info!("  group_id:        {}", group_id);
    info!("  input_topic:     {}", input_topic);
    info!("  output_topic:    {}", output_topic);
    info!("  num_workers:     {}", num_workers);
    info!("  schema_registry: {}", schema_registry);

    let schema_registry_urls = schema_registry.split(",").collect::<Vec<&str>>();
    let mut sr_settings_builder =
        SrSettings::new_builder(schema_registry_urls.first().unwrap().to_string());
    for (i, url) in schema_registry_urls.iter().enumerate() {
        if i > 0 {
            sr_settings_builder.add_url(url.to_string());
        }
    }

    let sr_settings = sr_settings_builder
        .set_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    (0..num_workers)
        .map(|_| {
            tokio::spawn(run_async_processor(
                brokers.to_owned(),
                group_id.to_owned(),
                input_topic.to_owned(),
                output_topic.to_owned(),
                sr_settings.to_owned(),
            ))
        })
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_graph_anc_collect_metrics() {
        setup_logger(true, None);
    }
}
