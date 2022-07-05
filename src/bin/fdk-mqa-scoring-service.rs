use fdk_mqa_scoring_service::kafka::{self};
use futures::stream::{FuturesUnordered, StreamExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_current_span(false)
        .init();

    (0..4)
        .map(|i| tokio::spawn(kafka::run_async_processor(i)))
        .collect::<FuturesUnordered<_>>()
        .for_each(|result| async {
            result
                .unwrap_or_else(|e| {
                    tracing::error!(
                        error = e.to_string().as_str(),
                        "unable to run worker thread"
                    );
                    std::process::exit(1);
                })
                .unwrap_or_else(|e| {
                    tracing::error!(error = e.to_string().as_str(), "worker failed");
                    std::process::exit(1);
                });
        })
        .await
}
