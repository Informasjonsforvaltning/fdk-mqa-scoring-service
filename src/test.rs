use oxigraph::model::NamedNode;

pub const MEASUREMENT_GRAPH: &str = r#"
    <https://dataset.foo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
    <https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.a>  .
    <https://dataset.foo> <http://www.w3.org/ns/dcat#distribution> <https://distribution.b>  .
    <https://distribution.a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
    <https://distribution.b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
    <https://dataset.foo> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:a .
    <https://distribution.a> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:b .
    <https://distribution.a> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:c .
    <https://distribution.b> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:d .
    _:a <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
    _:a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
    _:a <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability> .
    _:b <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
    _:b <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
    _:b <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessUrlStatusCode> .
    _:c <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
    _:c <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
    _:c <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability> .
    _:d <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
    _:d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
    _:d <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability> .
    "#;

pub const METRIC_GRAPH: &str = r#"
    @prefix dcatno-mqa: <https://data.norge.no/vocabulary/dcatno-mqa#> .
    @prefix dqv:        <http://www.w3.org/ns/dqv#> .
    dcatno-mqa:accessibility
        a                       dqv:Dimension .
    dcatno-mqa:interoperability
        a                       dqv:Dimension .
    dcatno-mqa:accessUrlStatusCode
        a                       dqv:Metric ;
        dqv:inDimension         dcatno-mqa:accessibility .
    dcatno-mqa:downloadUrlAvailability
        a                       dqv:Metric ;
        dqv:inDimension         dcatno-mqa:accessibility .
    dcatno-mqa:formatAvailability
        a                       dqv:Metric ;
        dqv:inDimension         dcatno-mqa:interoperability .
    "#;

pub const SCORE_GRAPH: &str = r#"
    @prefix dcatno-mqa: <https://data.norge.no/vocabulary/dcatno-mqa#> .
    @prefix xsd:        <http://www.w3.org/2001/XMLSchema#> .
    dcatno-mqa:accessUrlStatusCode
        dcatno-mqa:trueScore            "50"^^xsd:integer .
    dcatno-mqa:downloadUrlAvailability
        dcatno-mqa:trueScore            "20"^^xsd:integer .
    dcatno-mqa:formatAvailability
        dcatno-mqa:trueScore            "20"^^xsd:integer .
    "#;

pub fn node(name: &str) -> NamedNode {
    NamedNode::new_unchecked(name)
}

pub fn mqa_node(name: &str) -> NamedNode {
    NamedNode::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#".to_string() + name)
}
