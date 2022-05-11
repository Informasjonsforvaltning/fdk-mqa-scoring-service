use crate::error::MqaError;
use oxigraph::model::{vocab::xsd, Literal};

#[derive(Debug, PartialEq)]
pub enum MeasurementValue {
    Bool(bool),
    Int(i64),
    String(String),
    Unknown(String),
}

impl TryFrom<Literal> for MeasurementValue {
    type Error = MqaError;

    /// Try to parse quality measurement value from graph store literal.
    fn try_from(value: Literal) -> Result<Self, Self::Error> {
        match value.datatype() {
            xsd::STRING => Ok(Self::String(value.value().to_string())),
            xsd::BOOLEAN => Ok(Self::Bool(value.value().to_string() == "true")),
            xsd::INTEGER => match value.value().parse() {
                Ok(value) => Ok(Self::Int(value)),
                Err(_) => Err(format!(
                    "unable to parse quality measurement int: {}",
                    value.value()
                )
                .into()),
            },
            _ => Ok(Self::Unknown(value.value().to_string())),
        }
    }
}

impl MeasurementValue {
    // Whether a measurement value is considered ok and should be given positive score.
    pub fn acceptable(&self) -> bool {
        match self {
            MeasurementValue::Int(code) => 200 <= code.clone() && code.clone() < 300,
            MeasurementValue::Bool(bool) => bool.clone(),
            _ => false,
        }
    }
}
