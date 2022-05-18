use regex::Regex;

use crate::error::MqaError;

/// Replaces all blank nodes with named nodes.
/// Enables SPARQL query with (previously) blank nodes as identifiers.
pub fn name_blank_nodes(graph: String) -> Result<String, MqaError> {
    let replaced = Regex::new(r"_:(?P<id>[0-9a-f]+) ")
        .map(|re| re.replace_all(&graph, "<http://blank.node#${id}> "))?;
    Ok(replaced.to_string())
}

// Undoes replacement of all blank nodes with named nodes.
pub fn undo_name_blank_nodes(graph: String) -> Result<String, MqaError> {
    let replaced = Regex::new(r"<http://blank.node#(?P<id>[0-9a-f]+)> ")
        .map(|re| re.replace_all(&graph, "_:${id} "))?;
    Ok(replaced.to_string())
}
