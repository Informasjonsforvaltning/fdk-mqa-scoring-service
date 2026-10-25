use std::{
    env,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use apache_avro::schema::Name;
use lazy_static::lazy_static;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::{KafkaError, RDKafkaErrorCode},
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
    kafka_context::ThrottledContext,
    metrics::{PROCESSED_MESSAGES, PROCESSING_TIME},
    schemas::{InputEvent, MqaEvent, MqaEventType},
    score::calculate_score,
    score_graph::{ScoreDefinitions, ScoreGraph},
};

lazy_static! {
    pub static ref BROKERS: String = env::var("BROKERS").unwrap_or("localhost:9092".to_string());
    pub static ref KAFKA_SECURITY_PROTOCOL: String =
        env::var("KAFKA_SECURITY_PROTOCOL").unwrap_or("plaintext".to_string());
    pub static ref KAFKA_SASL_MECHANISM: String =
        env::var("KAFKA_SASL_MECHANISM").unwrap_or("SCRAM-SHA-512".to_string());
    pub static ref KAFKA_SASL_USERNAME: String =
        env::var("KAFKA_SASL_USERNAME").unwrap_or_default();
    pub static ref KAFKA_SASL_PASSWORD: String =
        env::var("KAFKA_SASL_PASSWORD").unwrap_or_default();
    pub static ref SCHEMA_REGISTRY: String =
        env::var("SCHEMA_REGISTRY").unwrap_or("http://localhost:8081".to_string());
    pub static ref INPUT_TOPIC: String =
        env::var("INPUT_TOPIC").unwrap_or("mqa-events".to_string());
    pub static ref SCORING_API_URL: String =
        env::var("SCORING_API_URL").unwrap_or("http://localhost:8082".to_string());
    pub static ref SCORING_API_KEY: String = env::var("API_KEY").unwrap_or_default();
    pub static ref KAFKA_READY: AtomicBool = AtomicBool::new(false);
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

/// Consumer type used throughout the service.
pub type KafkaConsumer = StreamConsumer<ThrottledContext>;

pub fn create_consumer() -> Result<KafkaConsumer, KafkaError> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("group.id", "fdk-mqa-scoring-service")
        .set("bootstrap.servers", BROKERS.clone())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "beginning")
        .set("max.partition.fetch.bytes", "2097152")
        .set("queued.max.messages.kbytes", "32768")
        .set("queued.min.messages", "500");
    set_security(&mut client_config);
    let consumer: KafkaConsumer = client_config.create_with_context(ThrottledContext)?;
    consumer.subscribe(&[&INPUT_TOPIC])?;
    Ok(consumer)
}

/// Applies the broker security settings from the environment.
///
/// SASL credentials are only set when the security protocol requires them,
/// so local development and tests keep using plaintext.
fn set_security(client_config: &mut ClientConfig) {
    let security_protocol = KAFKA_SECURITY_PROTOCOL.to_lowercase();
    client_config.set("security.protocol", &security_protocol);
    if security_protocol.starts_with("sasl") {
        client_config
            .set("sasl.mechanism", KAFKA_SASL_MECHANISM.clone())
            .set("sasl.username", KAFKA_SASL_USERNAME.clone())
            .set("sasl.password", KAFKA_SASL_PASSWORD.clone());
    }
}

pub fn is_kafka_ready() -> bool {
    KAFKA_READY.load(Ordering::Relaxed)
}

pub async fn run_async_processor(worker_id: usize, sr_settings: SrSettings) -> Result<(), Error> {
    tracing::info!(worker_id, "starting worker");

    let consumer: KafkaConsumer = create_consumer()?;
    
    // Verify Kafka connection by fetching metadata
    match consumer.fetch_metadata(None, Duration::from_secs(5)) {
        Ok(_) => {
            tracing::info!(worker_id, "kafka consumer connected successfully");
            KAFKA_READY.store(true, Ordering::Relaxed);
        }
        Err(e) => {
            tracing::warn!(worker_id, error = e.to_string(), "failed to fetch kafka metadata");
            // Don't set ready if we can't connect
        }
    }
    
    let mut decoder = AvroDecoder::new(sr_settings);
    let score_definitions = ScoreGraph::new()?.scores()?;
    let http_client = reqwest::Client::new();
    tracing::info!(worker_id, "listening for messages");
    let mut errors = ErrorStreak::default();
    loop {
        let message = match consumer.recv().await {
            Ok(message) => {
                errors.reset();
                message
            }
            Err(e) => {
                handle_consumer_error(worker_id, &e, &mut errors, Instant::now())?;
                continue;
            }
        };
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
            &http_client,
            &message,
        )
        .instrument(span)
        .await;
    }
}

async fn receive_message(
    consumer: &KafkaConsumer,
    decoder: &mut AvroDecoder<'_>,
    score_definitions: &ScoreDefinitions,
    http_client: &reqwest::Client,
    message: &BorrowedMessage<'_>,
) {
    let start_time = Instant::now();

    let assessment_graph = match AssessmentGraph::new() {
        Ok(graph) => graph,
        Err(e) => {
            let elapsed_millis = start_time.elapsed().as_millis();
            tracing::error!(
                elapsed_millis,
                attempts = 0,
                error = e.to_string(),
                "failed while handling message"
            );
            PROCESSED_MESSAGES.with_label_values(&["error"]).inc();
            PROCESSING_TIME.observe(elapsed_millis as f64 / 1000.0);
            return;
        }
    };

    let mut attempts = 0;
    let mut result = Ok(());

    for attempt in 1..=5 {
        attempts = attempt;
        // Clear between retries so a partial failure does not pollute the next attempt.
        if let Err(e) = assessment_graph.clear() {
            result = Err(e);
            break;
        }

        result = handle_message(
            decoder,
            score_definitions,
            &assessment_graph,
            http_client,
            message,
        )
        .await;

        if result.is_ok() {
            break;
        }
        if attempt < 5 {
            tokio::time::sleep(Duration::from_millis(3000)).await;
        }
    }
    let elapsed_millis = start_time.elapsed().as_millis();

    match result {
        Ok(()) => {
            tracing::debug!(elapsed_millis, attempts, "message handled successfully");
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
                .await?;
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

/// Returns true when a stored assessment is newer than the incoming event.
fn is_outdated(existing_timestamp: Option<i64>, event_timestamp: i64) -> bool {
    let comparison = existing_timestamp.map(|ts| ts.cmp(&event_timestamp));
    tracing::debug!(
        existing_timestamp,
        event_timestamp,
        ?comparison,
        "assessment timestamp comparison"
    );
    matches!(comparison, Some(std::cmp::Ordering::Greater))
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
        | MqaEventType::DcatComplianceChecked => {
            let fdk_id = Uuid::parse_str(event.fdk_id.as_str())
                .map_err(|e| format!("unable to parse FDK ID: {e}"))?;

            if let Some(graph) = get_graph(&http_client, &fdk_id).await? {
                assessment_graph.load(&graph)?;
                if is_outdated(assessment_graph.get_modified_timestamp(), event.timestamp) {
                    return Ok(());
                }
            } else {
                tracing::debug!("saving new assessment");
            }

            assessment_graph.load(&event.graph)?;
            assessment_graph.insert_modified_timestamp(event.timestamp)?;

            let (dataset_score, distribution_scores) =
                calculate_score(&assessment_graph, &score_definitions)?;
            let scores = convert_scores(&score_definitions, &dataset_score, &distribution_scores);

            assessment_graph.insert_scores(&vec![dataset_score])?;
            assessment_graph.insert_scores(&distribution_scores)?;

            tracing::debug!("posting assessment to api");
            let turtle_assessment = assessment_graph.to_turtle()?;

            post_scores(
                &http_client,
                &fdk_id,
                UpdateRequest {
                    scores,
                    turtle_assessment,
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

    match response.status() {
        StatusCode::ACCEPTED => Ok(()),
        StatusCode::CONFLICT => {
            tracing::warn!(
                fdk_id = %fdk_id,
                "scoring api returned conflict: duplicate dataset_uri (same URI, different id); acking message"
            );
            Ok(())
        }
        _ => {
            if response.status() == StatusCode::PAYLOAD_TOO_LARGE {
                tracing::warn!(
                    fdk_id = %fdk_id,
                    turtle_bytes = update.turtle_assessment.len(),
                    "payload too large"
                );
            }
            Err(format!(
                "Invalid response from scoring api: {} - {}",
                response.status(),
                response.text().await?
            )
            .into())
        }
    }
}

/// How often a worker repeats the warning while the broker stays unreachable.
const CONSUMER_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// How long a worker puts up with nothing but errors from the consumer before
/// it gives up. A broker restart is over well within this; a wrong broker
/// address or a long outage ends in a restarting pod instead of a silent one.
const CONSUMER_ERROR_DEADLINE: Duration = Duration::from_secs(5 * 60);

/// A pause between errors long enough to count them as separate incidents.
/// librdkafka repeats an error at least every 30 seconds while it lasts.
const CONSUMER_ERROR_STREAK_GAP: Duration = Duration::from_secs(60);

/// A run of consecutive errors from the consumer stream, cleared by the next
/// successfully received message or by a gap of `CONSUMER_ERROR_STREAK_GAP`
/// without errors, so that two short blips on a quiet topic do not add up.
#[derive(Debug, Default)]
struct ErrorStreak {
    started: Option<Instant>,
    last_seen: Option<Instant>,
    last_logged: Option<Instant>,
}

impl ErrorStreak {
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Decides what to do with an error from the consumer stream.
///
/// librdkafka reports broker outages, transport failures and failed (re)connects
/// as error events and reconnects on its own, so the worker keeps consuming and
/// logs at most one warning per interval. The worker ends instead when the error
/// is one no amount of retrying will fix, such as a wrong password, a missing
/// ACL or an unknown topic, or when it has seen nothing but errors for
/// `CONSUMER_ERROR_DEADLINE`. Either way the process exits and the pod restarts,
/// which is visible, rather than idling forever.
fn handle_consumer_error(
    worker_id: usize,
    error: &KafkaError,
    streak: &mut ErrorStreak,
    now: Instant,
) -> Result<(), Error> {
    if is_unrecoverable(error) {
        return Err(error.clone().into());
    }
    if streak
        .last_seen
        .is_some_and(|at| now.duration_since(at) > CONSUMER_ERROR_STREAK_GAP)
    {
        streak.reset();
    }
    streak.last_seen = Some(now);
    let started = *streak.started.get_or_insert(now);
    if now.duration_since(started) >= CONSUMER_ERROR_DEADLINE {
        tracing::error!(
            worker_id,
            error = %error,
            seconds = now.duration_since(started).as_secs(),
            "consumer has seen nothing but errors, giving up"
        );
        return Err(error.clone().into());
    }
    if streak
        .last_logged
        .is_none_or(|at| now.duration_since(at) >= CONSUMER_ERROR_LOG_INTERVAL)
    {
        tracing::warn!(
            worker_id,
            error = %error,
            "consumer error, retrying"
        );
        streak.last_logged = Some(now);
    }
    Ok(())
}

/// librdkafka's fatal errors, plus the errors that only change when the
/// configuration does: failed authentication, denied authorization and a
/// subscribed topic that does not exist.
fn is_unrecoverable(error: &KafkaError) -> bool {
    match error {
        KafkaError::MessageConsumptionFatal(_) => true,
        KafkaError::MessageConsumption(code) => matches!(
            code,
            RDKafkaErrorCode::Authentication
                | RDKafkaErrorCode::SaslAuthenticationFailed
                | RDKafkaErrorCode::TopicAuthorizationFailed
                | RDKafkaErrorCode::GroupAuthorizationFailed
                | RDKafkaErrorCode::ClusterAuthorizationFailed
                | RDKafkaErrorCode::UnknownTopicOrPartition
        ),
        _ => false,
    }
}

#[cfg(test)]
mod consumer_error_tests {
    use std::time::{Duration, Instant};

    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    use super::{
        handle_consumer_error, ErrorStreak, CONSUMER_ERROR_DEADLINE, CONSUMER_ERROR_LOG_INTERVAL,
        CONSUMER_ERROR_STREAK_GAP,
    };

    fn transient() -> KafkaError {
        KafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure)
    }

    /// Returns whether the worker stays alive.
    fn handle(error: &KafkaError, streak: &mut ErrorStreak, now: Instant) -> bool {
        handle_consumer_error(0, error, streak, now).is_ok()
    }

    #[test]
    fn transient_errors_keep_the_worker_alive() {
        for code in [
            RDKafkaErrorCode::BrokerTransportFailure,
            RDKafkaErrorCode::AllBrokersDown,
            RDKafkaErrorCode::Resolve,
            RDKafkaErrorCode::NotCoordinator,
        ] {
            let error = KafkaError::MessageConsumption(code);
            let mut streak = ErrorStreak::default();
            assert!(handle(&error, &mut streak, Instant::now()), "{code:?}");
        }
    }

    #[test]
    fn authentication_error_ends_the_worker() {
        for code in [
            RDKafkaErrorCode::Authentication,
            RDKafkaErrorCode::SaslAuthenticationFailed,
        ] {
            let error = KafkaError::MessageConsumption(code);
            let mut streak = ErrorStreak::default();
            assert!(!handle(&error, &mut streak, Instant::now()), "{code:?}");
        }
    }

    #[test]
    fn authorization_error_ends_the_worker() {
        for code in [
            RDKafkaErrorCode::TopicAuthorizationFailed,
            RDKafkaErrorCode::GroupAuthorizationFailed,
            RDKafkaErrorCode::ClusterAuthorizationFailed,
        ] {
            let error = KafkaError::MessageConsumption(code);
            let mut streak = ErrorStreak::default();
            assert!(!handle(&error, &mut streak, Instant::now()), "{code:?}");
        }
    }

    #[test]
    fn unknown_topic_ends_the_worker() {
        let error = KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition);
        assert!(!handle(&error, &mut ErrorStreak::default(), Instant::now()));
    }

    #[test]
    fn fatal_error_ends_the_worker() {
        let error = KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::Fatal);
        assert!(!handle(&error, &mut ErrorStreak::default(), Instant::now()));
    }

    #[test]
    fn repeated_errors_are_logged_once_per_interval() {
        let mut streak = ErrorStreak::default();
        let start = Instant::now();

        assert!(handle(&transient(), &mut streak, start));
        assert_eq!(streak.last_logged, Some(start));

        let halfway = start + CONSUMER_ERROR_LOG_INTERVAL / 2;
        assert!(handle(&transient(), &mut streak, halfway));
        assert_eq!(streak.last_logged, Some(start));

        let later = start + CONSUMER_ERROR_LOG_INTERVAL;
        assert!(handle(&transient(), &mut streak, later));
        assert_eq!(streak.last_logged, Some(later));
    }

    #[test]
    fn errors_past_the_deadline_end_the_worker() {
        let mut streak = ErrorStreak::default();
        let start = Instant::now();
        let deadline = start + CONSUMER_ERROR_DEADLINE;

        // librdkafka repeats the error every 30 seconds while the broker is down.
        let mut now = start;
        while now < deadline {
            assert!(handle(&transient(), &mut streak, now));
            now += Duration::from_secs(30);
        }
        assert_eq!(streak.started, Some(start));
        assert!(!handle(&transient(), &mut streak, deadline));
    }

    #[test]
    fn a_quiet_spell_between_errors_restarts_the_deadline() {
        let mut streak = ErrorStreak::default();
        let start = Instant::now();
        let second_blip = start + CONSUMER_ERROR_DEADLINE + CONSUMER_ERROR_STREAK_GAP;

        let first_blip_again = start + Duration::from_secs(30);

        assert!(handle(&transient(), &mut streak, start));
        assert!(handle(&transient(), &mut streak, first_blip_again));

        assert!(handle(&transient(), &mut streak, second_blip));
        assert_eq!(streak.started, Some(second_blip));
    }

    #[test]
    fn a_received_message_restarts_the_deadline() {
        let mut streak = ErrorStreak::default();
        let start = Instant::now();
        let late = start + CONSUMER_ERROR_DEADLINE;

        assert!(handle(&transient(), &mut streak, start));
        streak.reset();

        assert!(handle(&transient(), &mut streak, late));
        assert_eq!(streak.started, Some(late));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_only_when_existing_is_newer() {
        assert!(!is_outdated(None, 100));
        assert!(!is_outdated(Some(50), 100));
        assert!(!is_outdated(Some(100), 100));
        assert!(is_outdated(Some(150), 100));
    }
}
