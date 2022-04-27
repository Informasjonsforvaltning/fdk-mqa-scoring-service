use log::{error, info};

use oxigraph::io::GraphFormat;
use oxigraph::model::vocab::{rdf, xsd};
use oxigraph::model::*;
use oxigraph::store::{QuadIter, StorageError, Store};

use vocab::{dcat, dcat_mqa, dqv};

#[derive(Debug, PartialEq)]
pub enum QualityMeasurementValue {
    Bool(bool),
    Int(i32),
    String(String),
}

// Parse Turtle RDF and load into store
pub fn parse_turtle(turtle: String) -> Result<Store, StorageError> {
    info!("Loading turtle graph");

    let store = Store::new()?;
    match store.load_graph(
        turtle.as_ref(),
        GraphFormat::Turtle,
        GraphNameRef::DefaultGraph,
        None,
    ) {
        Ok(_) => info!("Graph loaded successfully"),
        Err(e) => error!("Loading graph failed {}", e),
    }

    Ok(store)
}

// Retrieve distributions of a dataset
pub fn list_distributions(dataset: NamedNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(dataset.into()),
        Some(dcat::DISTRIBUTION.into()),
        None,
        None,
    )
}

// Retrieve datasets
fn list_datasets(store: &Store) -> QuadIter {
    store.quads_for_pattern(
        None,
        Some(rdf::TYPE),
        Some(dcat::DATASET_CLASS.into()),
        None,
    )
}

// Retrieve dataset namednode
pub fn get_dataset_node(store: &Store) -> Option<NamedNode> {
    list_datasets(&store).next().and_then(|d| match d {
        Ok(Quad {
            subject: Subject::NamedNode(n),
            ..
        }) => Some(n),
        _ => None,
    })
}

pub fn convert_term_to_named_or_blank_node_ref(term: TermRef) -> Option<NamedOrBlankNodeRef> {
    match term {
        TermRef::NamedNode(node) => Some(NamedOrBlankNodeRef::NamedNode(node)),
        TermRef::BlankNode(node) => Some(NamedOrBlankNodeRef::BlankNode(node)),
        _ => None,
    }
}

pub fn get_quality_measurement_value(
    distribution: NamedOrBlankNodeRef,
    metric: NamedNodeRef,
    store: &Store,
) -> Option<QualityMeasurementValue> {
    let measurement = store
        .quads_for_pattern(
            Some(distribution.into()),
            Some(dqv::HAS_QUALITY_MEASUREMENT),
            None,
            None,
        )
        .filter_map(|quad| match quad {
            Ok(Quad {
                object: Term::BlankNode(quality_measurement),
                ..
            }) => store
                .quads_for_pattern(
                    Some(quality_measurement.as_ref().into()),
                    Some(dqv::IS_MEASUREMENT_OF),
                    Some(metric.into()),
                    None,
                )
                .next(),
            _ => None,
        })
        .next();

    match measurement {
        Some(Ok(Quad {
            object: Term::BlankNode(m),
            ..
        })) => {
            return store
                .quads_for_pattern(Some(m.as_ref().into()), Some(dqv::VALUE), None, None)
                .next()
                .map_or(None, |q| match q {
                    Ok(Quad {
                        object: Term::Literal(value),
                        ..
                    }) => convert_literal_to_quality_measurement_value(value),
                    _ => None,
                })
        }
        _ => None,
    }
}

fn convert_literal_to_quality_measurement_value(value: Literal) -> Option<QualityMeasurementValue> {
    match value.datatype() {
        xsd::STRING => Some(QualityMeasurementValue::String(value.value().to_string())),
        xsd::BOOLEAN => Some(QualityMeasurementValue::Bool(
            value.value().to_string() == "true",
        )),
        xsd::INTEGER => Some(QualityMeasurementValue::Int(
            value.value().parse().unwrap_or(0),
        )),
        _ => None,
    }
}
