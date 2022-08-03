use fdk_mqa_scoring_service::kafka::{
    create_sr_settings, run_async_processor, BROKERS, INPUT_TOPIC, SCHEMA_REGISTRY, SCORING_API_URL,
};
use futures::stream::{FuturesUnordered, StreamExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_current_span(false)
        .init();

    tracing::info!(
        brokers = BROKERS.to_string(),
        schema_registry = SCHEMA_REGISTRY.to_string(),
        input_topic = INPUT_TOPIC.to_string(),
        scoring_api_url = SCORING_API_URL.to_string(),
        "starting service"
    );

    let sr_settings = create_sr_settings().unwrap_or_else(|e| {
        tracing::error!(error = e.to_string(), "sr settings creation error");
        std::process::exit(1);
    });

    (0..4)
        .map(|i| tokio::spawn(run_async_processor(i, sr_settings.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|result| async {
            result
                .unwrap_or_else(|e| {
                    tracing::error!(error = e.to_string(), "unable to run worker thread");
                    std::process::exit(1);
                })
                .unwrap_or_else(|e| {
                    tracing::error!(error = e.to_string(), "worker failed");
                    std::process::exit(1);
                });
        })
        .await
}
