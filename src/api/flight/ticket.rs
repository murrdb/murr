use serde::{Deserialize, Serialize};

use crate::api::fetch::JsonFetchRequest;

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchTicket {
    pub table: String,
    #[serde(flatten)]
    pub request: JsonFetchRequest,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetch_ticket_parses_flat_json() {
        let json = r#"{"table": "features", "keys": {"id": ["a", "b"]}, "columns": ["score"]}"#;
        let ticket: FetchTicket = serde_json::from_str(json).unwrap();
        assert_eq!(ticket.table, "features");
        assert_eq!(ticket.request.keys["id"].len(), 2);
        assert_eq!(ticket.request.columns, vec!["score"]);
    }
}
