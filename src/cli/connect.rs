use crate::cli::ReplCommand;
use crate::ReplContext;
use clap::{ArgMatches, Parser};

#[derive(Debug, Parser)]
pub struct ConnectOpts {
    #[arg(value_parser = conn_parser, help = "Connection string, support postgresql, parquet, json file")]
    pub conn: DataSetConn,
    #[arg(short, long, help = "Table name, if postgres")]
    pub table: String,
    #[arg(short, long, help = "Dataset name")]
    pub name: String,
}

#[derive(Debug, Clone)]
pub enum DataSetConn {
    Postgres(String),
    Parquet(String),
    Json(String),
}

fn conn_parser(s: &str) -> Result<DataSetConn, reedline_repl_rs::Error> {
    let s = s.to_lowercase();
    if s.starts_with("postgresql://") {
        Ok(DataSetConn::Postgres(s))
    } else if s.ends_with(".parquet") {
        Ok(DataSetConn::Parquet(s))
    } else if s.ends_with(".json") {
        Ok(DataSetConn::Json(s))
    } else {
        Err(reedline_repl_rs::Error::UnknownCommand(
            "unknown connection type".to_string(),
        ))
    }
}

pub fn connect(
    args: ArgMatches,
    context: &mut ReplContext,
) -> reedline_repl_rs::Result<Option<String>> {
    let conn = args
        .get_one::<DataSetConn>("conn")
        .expect("conn not found")
        .to_owned();
    let table = args
        .get_one::<String>("table")
        .expect("table not found")
        .to_owned();
    let name = args
        .get_one::<String>("name")
        .expect("dataset name not found")
        .to_owned();

    let cmd = ConnectOpts::new(conn, table, name);
    context.send(cmd.into());

    Ok(None)
}

impl From<ConnectOpts> for ReplCommand {
    fn from(value: ConnectOpts) -> Self {
        ReplCommand::Connect(value)
    }
}

impl ConnectOpts {
    pub fn new(conn: DataSetConn, table: String, name: String) -> Self {
        Self { conn, table, name }
    }
}
