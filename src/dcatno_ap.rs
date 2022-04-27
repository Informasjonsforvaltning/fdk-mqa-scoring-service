use crate::vocab::{dcat_mqa, dqv};
use oxigraph::{
    io::GraphFormat,
    model::{vocab::rdf, GraphNameRef, NamedNode, NamedNodeRef, Quad, Subject, Term},
    store::{LoaderError, StorageError},
};
use std::{collections::HashMap, fs, io};

struct DcatapMqaStore(oxigraph::store::Store);

impl DcatapMqaStore {
    fn load() -> Result<Self, LoaderError> {
        let fnames = vec![
            "dcatno-mqa-vocabulary.ttl",
            "dcatno-mqa-vocabulary-default-score-values.ttl",
        ];
        match load_files(fnames) {
            Ok(graphs) => parse_graphs(graphs),
            Err(e) => Err(LoaderError::Storage(StorageError::Io(e))),
        }
    }

    fn dimensions(&self) -> Result<Vec<NamedNode>, StorageError> {
        self.0
            .quads_for_pattern(None, Some(rdf::TYPE), Some(dqv::DIMENSION.into()), None)
            .filter_map(named_quad_subject)
            .collect()
    }

    fn metrics(&self, dimension: NamedNodeRef) -> Result<Vec<NamedNode>, StorageError> {
        let metrics = self
            .0
            .quads_for_pattern(None, None, Some(dqv::METRIC.into()), None)
            .filter_map(named_quad_subject)
            .collect::<Result<Vec<NamedNode>, StorageError>>()?;

        self.0
            .quads_for_pattern(
                None,
                Some(dqv::IN_DIMENSION.into()),
                Some(dimension.into()),
                None,
            )
            .filter_map(named_quad_subject)
            .filter(|result| match result {
                Ok(node) => metrics.contains(node),
                _ => true,
            })
            .collect()
    }

    fn metric_scores(&self) -> Result<HashMap<NamedNode, u64>, StorageError> {
        self.0
            .quads_for_pattern(None, Some(dcat_mqa::TRUE_SCORE.into()), None, None)
            .filter_map(|result| match result {
                Ok(Quad {
                    subject: metric,
                    object: Term::Literal(value),
                    ..
                }) => {
                    // Only fetch named nodes with parsable scores
                    // TODO: fail on non-parsable score
                    match (metric, value.value().to_string().parse::<u64>()) {
                        (Subject::NamedNode(node), Ok(score)) => Some(Ok((node, score))),
                        _ => None,
                    }
                }
                Err(e) => Some(Err(e)),
                _ => None,
            })
            .collect()
    }
}

fn named_quad_subject(
    result: Result<Quad, StorageError>,
) -> Option<Result<NamedNode, StorageError>> {
    match result {
        Ok(quad) => match quad.subject {
            Subject::NamedNode(node) => Some(Ok(node)),
            _ => None,
        },
        Err(e) => Some(Err(e)),
    }
}

fn load_files(fnames: Vec<&str>) -> Result<Vec<String>, io::Error> {
    fnames
        .into_iter()
        .map(|fname| fs::read_to_string(fname))
        .collect()
}

fn parse_graphs(graphs: Vec<String>) -> Result<DcatapMqaStore, LoaderError> {
    let store = oxigraph::store::Store::new()?;
    for graph in graphs {
        store.load_graph(
            graph.as_ref(),
            GraphFormat::Turtle,
            GraphNameRef::DefaultGraph,
            None,
        )?;
    }
    Ok(DcatapMqaStore(store))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store() {
        let store = DcatapMqaStore::load();
        assert!(store.is_ok());
    }

    #[test]
    fn test_scores() {
        let store = DcatapMqaStore::load().unwrap();
        let scores = store.metric_scores().unwrap();
        assert!(scores.len() > 0);
    }

    #[test]
    fn test_dimension_metrics() {
        let store = DcatapMqaStore::load().unwrap();
        let scores = store.metric_scores().unwrap();

        let dimensions = store.dimensions().unwrap();
        assert!(dimensions.len() > 0);

        for dim in dimensions {
            let metrics = store.metrics(dim.as_ref()).unwrap();
            assert!(metrics.len() > 0);

            for metric in metrics {
                let score = scores.get(&metric);
                //assert!(score.is_some());

                match score {
                    Some(_) => (),
                    None => println!("Missing score: {}", metric),
                }
            }
        }
    }
}
