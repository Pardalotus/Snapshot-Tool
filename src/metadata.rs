use serde_json::Value;

pub(crate) fn get_doi_from_record(record: &Value) -> Option<String> {
    // Crossref DOI
    if let Some(doi) = record.get("DOI").and_then(|doi| doi.as_str()) {
        return Some(String::from(doi));
    }

    // DataCite DOI
    if let Some(doi) = record.get("doi").and_then(|doi| doi.as_str()) {
        return Some(String::from(doi));
    }

    return None;
}
