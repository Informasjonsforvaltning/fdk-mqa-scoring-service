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

    pub const DIMENSION_CLASS: N = n!("http://www.w3.org/ns/dqv#Dimension");
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

    pub const ASSESSMENT_OF: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf");
    pub const DATASET_ASSESSMENT_CLASS: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#DatasetAssessment");
    pub const HAS_DISTRIBUTION_ASSESSMENTS: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#hasDistributionAssessment");
    pub const DISTRIBUTION_ASSESSMENT_CLASS: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#DistributionAssessment");
    pub const CONTAINS_QUALITY_MEASUREMENT: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement");
    pub const TRUE_SCORE: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#trueScore");
    pub const SCORE: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#score");
    pub const SCORING: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#scoring");
    pub const ACCESS_URL_STATUS_CODE: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode");
    pub const DOWNLOAD_URL_STATUS_CODE: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlStatusCode");
}

pub mod dcat_terms {
    use super::N;

    pub const MODIFIED: N = n!("http://purl.org/dc/terms/modified");
}

pub mod rdf_syntax {
    use super::N;

    pub const TYPE: N = n!("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
}
