use actix_web::{get, App, HttpServer, Responder};
use fdk_mqa_scoring_service::{
    kafka::{
        create_sr_settings, run_async_processor, BROKERS, INPUT_TOPIC, SCHEMA_REGISTRY,
        SCORING_API_URL,
    },
    metrics::{get_metrics, register_metrics},
};
use futures::{
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};

lazy_static! {
    pub static ref LOG_LEVEL: String = env::var("LOG_LEVEL").unwrap_or("INFO".to_string());
}

#[get("/ping")]
async fn ping() -> impl Responder {
    "pong"
}

#[get("/ready")]
async fn ready() -> impl Responder {
    "ok"
}

#[get("/metrics")]
async fn metrics() -> impl Responder {
    match get_metrics() {
        Ok(metrics) => metrics,
        Err(e) => {
            tracing::error!(error = e.to_string(), "unable to gather metrics");
            "".to_string()
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::from_str(&env_log_level).unwrap())
        .with_target(false)
        .with_current_span(false)
        .init();

    register_metrics();

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

    let http_server = tokio::spawn(
        HttpServer::new(|| App::new().service(ping).service(ready).service(metrics))
            .bind(("0.0.0.0", 8080))
            .unwrap_or_else(|e| {
                tracing::error!(error = e.to_string(), "metrics server error");
                std::process::exit(1);
            })
            .run()
            .map(|f| f.map_err(|e| e.into())),
    );

    (0..4)
        .map(|i| tokio::spawn(run_async_processor(i, sr_settings.clone())))
        .chain(std::iter::once(http_server))
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
        .await;
}
