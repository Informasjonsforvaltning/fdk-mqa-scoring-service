use diesel::{
    expression_methods::ExpressionMethods,
    r2d2::{ConnectionManager, Pool, PooledConnection},
    result::Error::NotFound,
    Connection, PgConnection, QueryDsl, RunQueryDsl,
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use uuid::Uuid;

use crate::{models::Graph, schema};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("{0}: {1}")]
    ConfigError(&'static str, String),
    #[error("{0}")]
    MigrationError(String),
    #[error(transparent)]
    R2d2Error(#[from] r2d2::Error),
    #[error(transparent)]
    DieselError(#[from] diesel::result::Error),
    #[error(transparent)]
    DieselConnectionError(#[from] diesel::ConnectionError),
}

fn var(key: &'static str) -> Result<String, DatabaseError> {
    std::env::var(key).map_err(|e| DatabaseError::ConfigError(key, e.to_string()))
}

fn database_url() -> Result<String, DatabaseError> {
    let host = var("POSTGRES_HOST")?;
    let port = var("POSTGRES_PORT")?
        .parse::<u16>()
        .map_err(|e| DatabaseError::ConfigError("POSTGRES_PORT", e.to_string()))?;
    let user = var("POSTGRES_USERNAME")?;
    let password = var("POSTGRES_PASSWORD")?;
    let dbname = var("POSTGRES_DB_NAME")?;
    let url = format!("postgres://{user}:{password}@{host}:{port}/{dbname}");

    Ok(url)
}

pub fn migrate_database() -> Result<(), DatabaseError> {
    let url = database_url()?;
    PgConnection::establish(&url)?
        .run_pending_migrations(MIGRATIONS)
        .map_err(|e| DatabaseError::MigrationError(e.to_string()))?;

    Ok(())
}

#[derive(Clone)]
pub struct PgPool(Pool<ConnectionManager<PgConnection>>);

impl PgPool {
    pub fn new() -> Result<Self, DatabaseError> {
        let url = database_url()?;
        let manager = ConnectionManager::new(url);
        let pool = Pool::builder().test_on_check_out(true).build(manager)?;
        Ok(PgPool(pool))
    }

    pub fn get(&self) -> Result<PgConn, DatabaseError> {
        Ok(PgConn(self.0.get()?))
    }
}

pub struct PgConn(PooledConnection<ConnectionManager<PgConnection>>);

impl PgConn {
    pub fn store_graph(&mut self, graph: Graph) -> Result<(), DatabaseError> {
        use schema::graphs::dsl;

        diesel::insert_into(dsl::graphs)
            .values(&graph)
            .on_conflict(dsl::fdk_id)
            .do_update()
            .set(&graph)
            .execute(&mut self.0)?;

        Ok(())
    }

    pub fn get_score_graph_by_id(&mut self, fdk_id: Uuid) -> Result<Option<String>, DatabaseError> {
        use schema::graphs::dsl;

        match dsl::graphs
            .filter(dsl::fdk_id.eq(fdk_id.to_string()))
            .select(dsl::score)
            .first(&mut self.0)
        {
            Ok(graph) => Ok(Some(graph)),
            Err(NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
