use log::{info, error};
use schema_registry_converter::blocking::schema_registry::{post_schema, SrSettings};
use schema_registry_converter::schema_registry_common::{SchemaType, SuppliedSchema};
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub enum MQAEventType {
    #[serde(rename = "PROPERTIES_CHECKED")]
    PropertiesChecked,
    #[serde(rename = "URLS_CHECKED")]
    UrlsChecked,
    #[serde(rename = "DCAT_COMPLIANCE_CHECKED")]
    DcatComplienceChecked,
}

#[derive(Debug, Serialize)]
pub struct MQAEvent {
    #[serde(rename = "type")]
    pub event_type: MQAEventType,
    #[serde(rename = "fdkId")]
    pub fdk_id: String,
    pub graph: String,
    pub timestamp: i64,
}
