use deadpool_postgres::{Client, Manager, ManagerConfig, Pool, RecyclingMethod};
use thiserror::Error;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("{0}: {1}")]
    ConfigError(&'static str, String),
    #[error(transparent)]
    BuildError(#[from] deadpool_postgres::BuildError),
    #[error(transparent)]
    PostgresError(#[from] tokio_postgres::Error),
}

fn var(key: &'static str) -> Result<String, DatabaseError> {
    std::env::var(key).map_err(|e| DatabaseError::ConfigError(key, e.to_string()))
}

pub fn connection_pool() -> Result<Pool, DatabaseError> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host(var("POSTGRES_HOST")?.as_str());
    cfg.port(
        var("POSTGRES_PORT")?
            .parse::<u16>()
            .map_err(|e| DatabaseError::ConfigError("POSTGRES_PORT", e.to_string()))?,
    );
    cfg.user(var("POSTGRES_USERNAME")?.as_str());
    cfg.password(var("POSTGRES_PASSWORD")?.as_str());
    cfg.dbname(var("POSTGRES_DB_NAME")?.as_str());

    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let mgr = Manager::from_config(cfg, NoTls, mgr_config);
    let pool = Pool::builder(mgr).max_size(16).build()?;
    Ok(pool)
}

pub async fn create_table(client: &Client) -> Result<(), DatabaseError> {
    let q = "
        CREATE TABLE IF NOT EXISTS mqa (
            id    VARCHAR PRIMARY KEY,
            score VARCHAR NOT NULL,
            vocab VARCHAR NOT NULL
        )
    ";
    client.batch_execute(q).await?;
    Ok(())
}

pub async fn store_graph(
    client: &Client,
    uuid: &Uuid,
    score: String,
    vocab: String,
) -> Result<(), DatabaseError> {
    client
        .execute(
            "
            INSERT INTO mqa (id, score, vocab) VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE 
            SET score = excluded.score, vocab = excluded.vocab;
        ",
            &[&uuid.to_string(), &score, &vocab],
        )
        .await?;
    Ok(())
}

pub async fn get_graph_by_id(client: &Client, id: Uuid) -> Result<Option<String>, DatabaseError> {
    let q = "SELECT score FROM mqa WHERE id = $1";
    let stmt = client.prepare(q).await?;

    client
        .query(&stmt, &[&id.to_string()])
        .await?
        .first()
        .map_or(Ok(None), |row| Ok(row.try_get(0)?))
}
