use serde::{Deserialize, Serialize};

pub enum Event {
    MqaEvent(MqaEvent),
    Unknown { namespace: String, name: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MqaEvent {
    #[serde(rename = "type")]
    pub event_type: MqaEventType,
    #[serde(rename = "fdkId")]
    pub fdk_id: String,
    pub graph: String,
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MqaEventType {
    #[serde(rename = "PROPERTIES_CHECKED")]
    PropertiesChecked,
    #[serde(rename = "URLS_CHECKED")]
    UrlsChecked,
    #[serde(rename = "DCAT_COMPLIANCE_CHECKED")]
    DcatComplienceChecked,
    #[serde(other)]
    Unknown,
}
