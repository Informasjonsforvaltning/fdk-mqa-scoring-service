use std::time::Duration;

use fdk_mqa_scoring_service::{
    database::{migrate_database, PgPool},
    kafka::{self, SCHEMA_REGISTRY},
};
use futures::stream::{FuturesUnordered, StreamExt};
use schema_registry_converter::async_impl::schema_registry::SrSettings;

#[tokio::main]
async fn main() {
    let mut schema_registry_urls = SCHEMA_REGISTRY.split(",");
    let mut sr_settings_builder =
        SrSettings::new_builder(schema_registry_urls.next().unwrap().to_string());
    schema_registry_urls.for_each(|url| {
        sr_settings_builder.add_url(url.to_string());
    });

    let sr_settings = sr_settings_builder
        .set_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    migrate_database().unwrap();
    let pool = PgPool::new().unwrap();

    (0..4)
        .map(|_| {
            tokio::spawn(kafka::run_async_processor(
                sr_settings.clone(),
                pool.clone(),
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
