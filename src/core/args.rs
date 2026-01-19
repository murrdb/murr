use clap::Parser;
use log::kv::{ToValue, Value};

#[derive(Parser, Debug, PartialEq)]
#[command(version, about)]
pub struct CliArgs {
    #[arg(short, long)]
    pub config: Option<String>,
}

impl ToValue for CliArgs {
    fn to_value(&self) -> Value<'_> {
        Value::from_debug(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parsing() {
        let args = CliArgs::parse_from(["self", "--config", "foo"]);
        assert_eq!(
            args,
            CliArgs {
                config: Some("foo".to_string())
            }
        );
    }
}
