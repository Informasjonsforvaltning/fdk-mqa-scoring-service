#[macro_export]
macro_rules! n {
    ($iri:expr) => {
        oxigraph::model::NamedNodeRef::new_unchecked($iri)
    };
}

type N = oxigraph::model::NamedNodeRef<'static>;

pub mod dcat {
    use super::N;

    pub const DISTRIBUTION: N = n!("http://www.w3.org/ns/dcat#distribution");
}

pub mod dqv {
    use super::N;

    pub const DIMENSION: N = n!("http://www.w3.org/ns/dqv#Dimension");
    pub const IN_DIMENSION: N = n!("http://www.w3.org/ns/dqv#inDimension");
    pub const HAS_QUALITY_MEASUREMENT: N = n!("http://www.w3.org/ns/dqv#hasQualityMeasurement");
    pub const IS_MEASUREMENT_OF: N = n!("http://www.w3.org/ns/dqv#isMeasurementOf");
    pub const COMPUTED_ON: N = n!("http://www.w3.org/ns/dqv#computedOn");
    pub const VALUE: N = n!("http://www.w3.org/ns/dqv#value");
    pub const METRIC: N = n!("http://www.w3.org/ns/dqv#Metric");
}

pub mod dcat_mqa {
    use super::N;

    pub const TRUE_SCORE: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#trueScore");
    pub const SCORING: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#scoring");
}
