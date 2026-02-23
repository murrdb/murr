use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchTicket {
    pub table: String,
    pub keys: Vec<String>,
    pub columns: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetch_ticket_round_trip() {
        let ticket = FetchTicket {
            table: "features".to_string(),
            keys: vec!["a".to_string(), "b".to_string()],
            columns: vec!["score".to_string()],
        };
        let bytes = serde_json::to_vec(&ticket).unwrap();
        let decoded: FetchTicket = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.table, "features");
        assert_eq!(decoded.keys, vec!["a", "b"]);
        assert_eq!(decoded.columns, vec!["score"]);
    }
}
