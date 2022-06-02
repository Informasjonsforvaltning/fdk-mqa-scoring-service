use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum MQAEventType {
    #[serde(rename = "PROPERTIES_CHECKED")]
    PropertiesChecked,
    #[serde(rename = "URLS_CHECKED")]
    UrlsChecked,
    #[serde(rename = "DCAT_COMPLIANCE_CHECKED")]
    DcatComplienceChecked,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MQAEvent {
    #[serde(rename = "type")]
    pub event_type: MQAEventType,
    #[serde(rename = "fdkId")]
    pub fdk_id: String,
    pub graph: String,
    pub timestamp: i64,
}
