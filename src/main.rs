use std::{env, time::Duration};

use futures::stream::{FuturesUnordered, StreamExt};
use schema_registry_converter::async_impl::schema_registry::SrSettings;

mod error;
mod graph;
mod helpers;
mod kafka;
mod measurement_graph;
mod measurement_value;
mod schemas;
mod score;
mod score_graph;
mod test;
mod vocab;

#[tokio::main]
async fn main() {
    let schema_registry =
        env::var("SCHEMA_REGISTRY_URL").unwrap_or("http://localhost:8081".to_string());
    let broker = env::var("BROKER_URL").unwrap_or("localhost:9092".to_string());

    let sr_settings = SrSettings::new_builder(schema_registry)
        .set_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    (0..4)
        .map(|_| {
            tokio::spawn(kafka::run_async_processor(
                broker.clone(),
                "fdk-mqa-scoring-service".to_string(),
                "mqa-events".to_string(),
                sr_settings.clone(),
            ))
        })
        .collect::<FuturesUnordered<_>>()
        .for_each(|result| async {
            match result {
                Err(e) => panic!("{}", e),
                Ok(Err(e)) => panic!("{}", e),
                _ => (),
            }
        })
        .await;
}
