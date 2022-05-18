#[macro_export]
macro_rules! n {
    ($iri:expr) => {
        oxigraph::model::NamedNodeRef::new_unchecked($iri)
    };
}

type N = oxigraph::model::NamedNodeRef<'static>;

pub mod dcat {
    use super::N;

    pub const DATASET: N = n!("http://www.w3.org/ns/dcat#Dataset");
    pub const DISTRIBUTION: N = n!("http://www.w3.org/ns/dcat#distribution");
}

pub mod dqv {
    use super::N;

    pub const DIMENSION: N = n!("http://www.w3.org/ns/dqv#Dimension");
    pub const IN_DIMENSION: N = n!("http://www.w3.org/ns/dqv#inDimension");
    pub const QUALITY_MEASUREMENT_CLASS: N = n!("http://www.w3.org/ns/dqv#QualityMeasurement");
    pub const HAS_QUALITY_MEASUREMENT: N = n!("http://www.w3.org/ns/dqv#hasQualityMeasurement");
    pub const IS_MEASUREMENT_OF: N = n!("http://www.w3.org/ns/dqv#isMeasurementOf");
    pub const COMPUTED_ON: N = n!("http://www.w3.org/ns/dqv#computedOn");
    pub const VALUE: N = n!("http://www.w3.org/ns/dqv#value");
    pub const METRIC: N = n!("http://www.w3.org/ns/dqv#Metric");
}

pub mod dcat_mqa {
    use super::N;

    pub const TRUE_SCORE: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#trueScore");
    pub const SCORE: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#score");
    pub const SCORING: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#scoring");
    pub const ACCESS_URL_STATUS_CODE: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#mqa:accessUrlStatusCode");
    pub const DOWNLOAD_URL_STATUS_CODE: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#mqa:downloadUrlStatusCode");
}

pub mod rdf_syntax {
    use super::N;

    pub const TYPE: N = n!("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
}
