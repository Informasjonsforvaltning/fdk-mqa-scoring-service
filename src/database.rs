use diesel::{
    expression_methods::ExpressionMethods,
    r2d2::{ConnectionManager, Pool, PooledConnection},
    result::Error::NotFound,
    Connection, PgConnection, QueryDsl, RunQueryDsl,
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use uuid::Uuid;

use crate::{
    models::{Dataset, Dimension},
    schema,
};

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
    let host = var("POSTGRES_HOST").unwrap_or("localhost".to_string());
    let port = var("POSTGRES_PORT")
        .unwrap_or("5432".to_string())
        .parse::<u16>()
        .map_err(|e| DatabaseError::ConfigError("POSTGRES_PORT", e.to_string()))?;
    let user = var("POSTGRES_USERNAME").unwrap_or("postgres".to_string());
    let password = var("POSTGRES_PASSWORD").unwrap_or("postgres".to_string());
    let dbname = var("POSTGRES_DB_NAME").unwrap_or("mqa".to_string());
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
    pub fn store_dataset(&mut self, dataset: Dataset) -> Result<(), DatabaseError> {
        use schema::datasets::dsl;

        diesel::insert_into(dsl::datasets)
            .values(&dataset)
            .on_conflict(dsl::id)
            .do_update()
            .set(&dataset)
            .execute(&mut self.0)?;

        Ok(())
    }

    pub fn store_dimension(&mut self, dimension: Dimension) -> Result<(), DatabaseError> {
        use schema::dimensions::dsl;

        diesel::insert_into(dsl::dimensions)
            .values(&dimension)
            .on_conflict((dsl::dataset_id, dsl::title))
            .do_update()
            .set(&dimension)
            .execute(&mut self.0)?;

        Ok(())
    }

    pub fn get_score_graph_by_id(&mut self, id: Uuid) -> Result<Option<String>, DatabaseError> {
        use schema::datasets::dsl;

        match dsl::datasets
            .filter(dsl::id.eq(id.to_string()))
            .select(dsl::score_graph)
            .first(&mut self.0)
        {
            Ok(graph) => Ok(Some(graph)),
            Err(NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
