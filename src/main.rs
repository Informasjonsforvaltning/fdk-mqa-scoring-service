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
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;

use chrono::{TimeZone, Utc};

use oxigraph::model::*;
use oxigraph::store::{StorageError, Store};

use crate::utils::setup_logger;

use crate::vocab::{dcat, dcat_mqa, dcterms, oa};

use crate::rdf::{
    add_derived_from, add_five_star_annotation, add_property, add_quality_measurement,
    convert_term_to_named_or_blank_node_ref, create_metrics_store, dump_graph_as_turtle,
    get_dataset_node, has_property, is_rdf_format, list_distributions, list_formats,
    list_media_types, parse_turtle,
};
use crate::schemas::{setup_schemas, DatasetEvent, DatasetEventType, MQAEvent, MQAEventType};

use crate::score::*;

mod rdf;
mod schemas;
mod score;
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

fn parse_dataset_event(
    msg: OwnedMessage,
    mut decoder: AvroDecoder,
) -> Result<DatasetEvent, String> {
    match decoder.decode(msg.payload()) {
        Ok(result) => match result.name {
            Some(name) => match name.name.as_str() {
                "DatasetEvent" => match name.namespace {
                    Some(namespace) => match namespace.as_str() {
                        "no.fdk.dataset" => match from_value::<DatasetEvent>(&result.value) {
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

// Read DatasetEvent message of type DATASET_HARVESTED
fn handle_dataset_event(msg: OwnedMessage, decoder: AvroDecoder) -> Result<Option<MQAEvent>, String> {
    info!("Handle MQAEvent on message {}", msg.offset());

    let mqa_event = parse_mqa_event(msg, decoder);

    match mqa_event {
        Ok(event) => {
            match event.event_type {
                MQAEventType::UrlsChecked => {
                    let dt = Utc.timestamp_millis(event.timestamp);
                    info!(
                        "{} - Processing urls cheked event with timestamp {:?}",
                        event.fdk_id, dt
                    );
                    parse_metrics_graph_and_calculate_accessibility_score(event.fdk_id, event.graph).map(|evt| Some(evt))
                },
                MQAEventType::PropertiesChecked => Ok(None),
                MQAEventType::DcatComplienceChecked => Ok(None),
                _ => Ok(None)
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
) -> Result<MQAEvent, String> {
    match parse_turtle(graph) {
        Ok(store) => {
            match get_dataset_node(&store) {
                Some(dataset_node) => {
                    match calculate_score(dataset_node.as_ref(), &store) {
                        Ok(score) => {
                            // TODO
                            // Save scores to database
                        }
                        Err(e) => Err(format!("{}", e)),
                    }
                }
                None => Err(format!("{} - Dataset node not found in graph", fdk_id)),
            }
        }
        Err(e) => Err(format!("{}", e)),
    }
}

fn calculate_distribution_score(
    dist_node: NamedOrBlankNodeRef,
    store: &Store,
    metrics_store: &Store,
) -> Result<Score, String> {
    let score = Score::new{};

    score
}

fn calculate_score(dataset_node: NamedNodeRef, store: &Store) -> Result<Score, String> {
    let score = Score::new{};

    for quad in list_distributions(dataset_node, &store) {
        match quad {
            Ok(dist_quad) => {
                match convert_term_to_named_or_blank_node_ref(dist_quad.object.as_ref()) {
                    Some(dist_node) => {
                        calculate_distribution_score(dist_node, store, &metrics_store)?;
                    }
                    None => error!(
                        "Distribution is not a named or blank node {}",
                        dist_quad.object
                    ),
                }
            },
            Err(e) => error!("Listing distributions failed {}", e),
        }
    }

    Ok(score)
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
        let mut encoder = AvroEncoder::new(sr_settings.clone());
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
                    tokio::task::spawn_blocking(|| handle_dataset_event(owned_message, decoder))
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
    let matches = Command::new("fdk-mqa-property-checker")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("FDK MQA Property checker")
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
            Arg::new("output-topic")
                .long("output-topic")
                .help("Output topic")
                .takes_value(true)
                .default_value("mqa-events"),
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
    use std::env;

    #[test]
    fn test_parse_graph_anc_collect_metrics() {
        setup_logger(true, None);

        let server = httpmock::MockServer::start();

        server.mock(|when, then| {
            when.path("/iana/media-types");
            then.status(200)
                .header("content-type", "application/json")
                .body(
                    r#"
                    {
                        "mediaTypes": []
                    }
                "#,
                );
        });

        server.mock(|when, then| {
            when.path("/eu/file-types");
            then.status(200)
                .header("content-type", "application/json")
                .body(
                    r#"
                    {
                        "fileTypes": []
                    }
                "#,
                );
        });

        env::set_var(
            "REFERENCE_DATA_BASE_URL",
            format!("http://{}", server.address()),
        );

        let mqa_event = parse_rdf_graph_and_calculate_metrics("1".to_string(), r#"
            @prefix adms: <http://www.w3.org/ns/adms#> . 
            @prefix cpsv: <http://purl.org/vocab/cpsv#> . 
            @prefix cpsvno: <https://data.norge.no/vocabulary/cpsvno#> . 
            @prefix dcat: <http://www.w3.org/ns/dcat#> . 
            @prefix dct: <http://purl.org/dc/terms/> . 
            @prefix dqv: <http://www.w3.org/ns/dqv#> . 
            @prefix eli: <http://data.europa.eu/eli/ontology#> . 
            @prefix foaf: <http://xmlns.com/foaf/0.1/> . 
            @prefix iso: <http://iso.org/25012/2008/dataquality/> . 
            @prefix oa: <http://www.w3.org/ns/oa#> . 
            @prefix prov: <http://www.w3.org/ns/prov#> . 
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . 
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
            @prefix schema: <http://schema.org/> . 
            @prefix skos: <http://www.w3.org/2004/02/skos/core#> . 
            @prefix vcard: <http://www.w3.org/2006/vcard/ns#> . 
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
            
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> rdf:type dcat:Dataset ; 
                dct:accessRights <http://publications.europa.eu/resource/authority/access-right/PUBLIC> ; 
                dct:description "Visning over all norsk offentlig bistand fra 1960 til siste kalenderår sortert etter partnerorganisasjoner."@nb ; 
                dct:identifier "https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572" ; 
                dct:language <http://publications.europa.eu/resource/authority/language/NOR> , <http://publications.europa.eu/resource/authority/language/ENG> ; 
                dct:provenance <http://data.brreg.no/datakatalog/provinens/nasjonal> ; 
                dct:publisher <https://organization-catalogue.fellesdatakatalog.digdir.no/organizations/971277882> ; 
                dct:title "Bistandsresultater - bistand etter partner"@nb ; 
                dct:type "Data" ; 
                dcat:contactPoint [ rdf:type vcard:Organization ; vcard:hasEmail <mailto:resultater@norad.no> ] ; 
                dcat:distribution [ 
                    rdf:type dcat:Distribution ; dct:description "Norsk bistand i tall etter partner"@nb ; 
                    dct:format <https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.spreadsheetml.sheet> , 
                            <https://www.iana.org/assignments/media-types/text/csv> ; 
                    dct:license <http://data.norge.no/nlod/no/2.0> ; 
                    dct:title "Bistandsresultater - bistand etter partner"@nb ; 
                    dcat:accessURL <https://resultater.norad.no/partner/> ] ; 
                dcat:keyword "oda"@nb , "norad"@nb , "bistand"@nb ; 
                dcat:landingPage <https://resultater.norad.no/partner/> ; 
                dcat:theme <http://publications.europa.eu/resource/authority/data-theme/INTR> ; 
                dqv:hasQualityAnnotation [ rdf:type dqv:QualityAnnotation ; dqv:inDimension iso:Currentness ] ; 
                prov:qualifiedAttribution [ 
                    rdf:type prov:Attribution ; 
                    dcat:hadRole <http://registry.it.csiro.au/def/isotc211/CI_RoleCode/contributor> ; 
                    prov:agent <https://data.brreg.no/enhetsregisteret/api/enheter/971277882> ] . 
                <http://publications.europa.eu/resource/authority/language/ENG> rdf:type dct:LinguisticSystem ; 
                    <http://publications.europa.eu/ontology/authority/authority-code> "ENG" ; 
                    skos:prefLabel "Engelsk"@nb . 
                <http://publications.europa.eu/resource/authority/language/NOR> rdf:type dct:LinguisticSystem ; 
                    <http://publications.europa.eu/ontology/authority/authority-code> "NOR" ; skos:prefLabel "Norsk"@nb .
        "#.to_string());

        let store_expected = parse_turtle(String::from(
            r#"<https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:2e0587e7a28b492755a38437372b2e05 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:38fc04f528a7eef5b4102f9fdd4b9ab6 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:5cead1a2399fcb8ea6ec957254ddf186 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:6fa77fe6d9fe5abd71949e9b74f63a46 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:9919fee0b16fa958dbc231c6f1f542d4 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:9e90199079487760d26f4e022db8c116 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:c0cc2452ef89d2b1343d07254497828e .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:f0c942556bf9c7b4ddc968bcef39b6f4 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:f991bd5d3daf2b0b894775e0797afeea .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:fd38f82c4726d61ffd3920fd165ba303 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dcat#distribution> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:972515fe91764948597fbb3beebedc5 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:972515fe91764948597fbb3beebedc5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:972515fe91764948597fbb3beebedc5 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#licenseAvailability> .
        _:972515fe91764948597fbb3beebedc5 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:17a511e66065f4607ba5bdb4a89bd2ee <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:17a511e66065f4607ba5bdb4a89bd2ee <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:17a511e66065f4607ba5bdb4a89bd2ee <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#openLicense> .
        _:17a511e66065f4607ba5bdb4a89bd2ee <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:2e0587e7a28b492755a38437372b2e05 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:2e0587e7a28b492755a38437372b2e05 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:2e0587e7a28b492755a38437372b2e05 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#categoryAvailability> .
        _:2e0587e7a28b492755a38437372b2e05 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:38fc04f528a7eef5b4102f9fdd4b9ab6 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:38fc04f528a7eef5b4102f9fdd4b9ab6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:38fc04f528a7eef5b4102f9fdd4b9ab6 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateIssuedAvailability> .
        _:38fc04f528a7eef5b4102f9fdd4b9ab6 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:5cead1a2399fcb8ea6ec957254ddf186 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:5cead1a2399fcb8ea6ec957254ddf186 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:5cead1a2399fcb8ea6ec957254ddf186 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#spatialAvailability> .
        _:5cead1a2399fcb8ea6ec957254ddf186 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:6fa77fe6d9fe5abd71949e9b74f63a46 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:6fa77fe6d9fe5abd71949e9b74f63a46 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:6fa77fe6d9fe5abd71949e9b74f63a46 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessRightsAvailability> .
        _:6fa77fe6d9fe5abd71949e9b74f63a46 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:75039bd0fdf7843c5441c5807a4ec42f <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:75039bd0fdf7843c5441c5807a4ec42f <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:75039bd0fdf7843c5441c5807a4ec42f <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateModifiedAvailability> .
        _:75039bd0fdf7843c5441c5807a4ec42f <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:832a54ed610f7d5636eb4c42a8ebfcd7 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:832a54ed610f7d5636eb4c42a8ebfcd7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:832a54ed610f7d5636eb4c42a8ebfcd7 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#knownLicense> .
        _:832a54ed610f7d5636eb4c42a8ebfcd7 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:88429707f7d93b283ba7f140c12044fe <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:88429707f7d93b283ba7f140c12044fe <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:88429707f7d93b283ba7f140c12044fe <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#mediaTypeAvailability> .
        _:88429707f7d93b283ba7f140c12044fe <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:8b20c408a89600e4c506d8ad0e0f4ef2 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:8b20c408a89600e4c506d8ad0e0f4ef2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:8b20c408a89600e4c506d8ad0e0f4ef2 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability> .
        _:8b20c408a89600e4c506d8ad0e0f4ef2 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:91a3e690dbbcf753008d6d1836be234e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityAnnotation> .
        _:91a3e690dbbcf753008d6d1836be234e <http://www.w3.org/ns/oa#hasBody> <https://data.norge.no/vocabulary/dcatno-mqa#zeroStars> .
        _:91a3e690dbbcf753008d6d1836be234e <http://www.w3.org/ns/oa#motivatedBy> <http://www.w3.org/ns/oa#classifying> .
        _:91a3e690dbbcf753008d6d1836be234e <http://www.w3.org/ns/prov#wasDerivedFrom> _:17a511e66065f4607ba5bdb4a89bd2ee .
        _:95261ca4b6eb5455fb9222dbc9481ee1 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:95261ca4b6eb5455fb9222dbc9481ee1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:95261ca4b6eb5455fb9222dbc9481ee1 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#rightsAvailability> .
        _:95261ca4b6eb5455fb9222dbc9481ee1 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:9919fee0b16fa958dbc231c6f1f542d4 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:9919fee0b16fa958dbc231c6f1f542d4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:9919fee0b16fa958dbc231c6f1f542d4 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#publisherAvailability> .
        _:9919fee0b16fa958dbc231c6f1f542d4 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:9e90199079487760d26f4e022db8c116 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:9e90199079487760d26f4e022db8c116 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:9e90199079487760d26f4e022db8c116 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessRightsVocabularyAlignment> .
        _:9e90199079487760d26f4e022db8c116 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:a28c8063eb23a04eb056ed77af71714a <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:a28c8063eb23a04eb056ed77af71714a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:a28c8063eb23a04eb056ed77af71714a <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#byteSizeAvailability> .
        _:a28c8063eb23a04eb056ed77af71714a <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:ac9b2d402b7da13f8ee4d49df729d93e <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:ac9b2d402b7da13f8ee4d49df729d93e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:ac9b2d402b7da13f8ee4d49df729d93e <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#atLeastFourStars> .
        _:ac9b2d402b7da13f8ee4d49df729d93e <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#keywordAvailability> .
        _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:e2e93b98661a6f50e837434ae104a538 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:e2e93b98661a6f50e837434ae104a538 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:e2e93b98661a6f50e837434ae104a538 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateIssuedAvailability> .
        _:e2e93b98661a6f50e837434ae104a538 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:ed65dfa5fa665e84b15bc107d9ccf087 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:ed65dfa5fa665e84b15bc107d9ccf087 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:ed65dfa5fa665e84b15bc107d9ccf087 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeVocabularyAlignment> .
        _:ed65dfa5fa665e84b15bc107d9ccf087 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:f014b40cce0afd210f34b97cf54e0a50 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:f014b40cce0afd210f34b97cf54e0a50 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:f014b40cce0afd210f34b97cf54e0a50 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability> .
        _:f014b40cce0afd210f34b97cf54e0a50 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:f0c942556bf9c7b4ddc968bcef39b6f4 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:f0c942556bf9c7b4ddc968bcef39b6f4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:f0c942556bf9c7b4ddc968bcef39b6f4 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#temporalAvailability> .
        _:f0c942556bf9c7b4ddc968bcef39b6f4 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:f991bd5d3daf2b0b894775e0797afeea <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:f991bd5d3daf2b0b894775e0797afeea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:f991bd5d3daf2b0b894775e0797afeea <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#contactPointAvailability> .
        _:f991bd5d3daf2b0b894775e0797afeea <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:972515fe91764948597fbb3beebedc5 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:17a511e66065f4607ba5bdb4a89bd2ee .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:75039bd0fdf7843c5441c5807a4ec42f .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:832a54ed610f7d5636eb4c42a8ebfcd7 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:88429707f7d93b283ba7f140c12044fe .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:8b20c408a89600e4c506d8ad0e0f4ef2 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:95261ca4b6eb5455fb9222dbc9481ee1 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:a28c8063eb23a04eb056ed77af71714a .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:ac9b2d402b7da13f8ee4d49df729d93e .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:e2e93b98661a6f50e837434ae104a538 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:ed65dfa5fa665e84b15bc107d9ccf087 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:f014b40cce0afd210f34b97cf54e0a50 .
        _:fd38f82c4726d61ffd3920fd165ba303 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:fd38f82c4726d61ffd3920fd165ba303 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:fd38f82c4726d61ffd3920fd165ba303 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateModifiedAvailability> .
        _:fd38f82c4726d61ffd3920fd165ba303 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> ."#,
        )).unwrap();

        assert!(mqa_event.is_ok());
        let store_actual = parse_turtle(mqa_event.unwrap().graph).unwrap();
        assert_eq!(
            store_expected
                .quads_for_pattern(None, None, None, None)
                .count(),
            store_actual
                .quads_for_pattern(None, None, None, None)
                .count()
        );
    }
}
