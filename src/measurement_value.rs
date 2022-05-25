use oxigraph::model::{vocab::xsd, Literal};

use crate::error::MqaError;

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
            xsd::BOOLEAN => Ok(Self::Bool(value.value().parse().map_err(|_| {
                format!("unable to parse measurement bool: {}", value.value())
            })?)),
            xsd::INTEGER => Ok(Self::Int(value.value().parse().map_err(|_| {
                format!("unable to parse measurement int: {}", value.value())
            })?)),
            _ => Ok(Self::Unknown(value.value().to_string())),
        }
    }
}
