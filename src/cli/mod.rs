pub(crate) mod connect;
pub(crate) mod describe;
pub(crate) mod head;
pub(crate) mod list;
pub(crate) mod schema;
pub(crate) mod sql;

pub use crate::cli::{
    connect::ConnectOpts, describe::DescribeOpts, head::HeadOpts, list::ListOpts,
    schema::SchemaOpts, sql::SqlOpts,
};
use clap::Parser;
use enum_dispatch::enum_dispatch;

#[derive(Debug, Parser)]
#[enum_dispatch(CmdExecutor)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to DF(data-forge-rs)"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "List all registered datasets")]
    List(ListOpts),
    #[command(name = "schema", about = "Get the schema of a dataset")]
    Schema(SchemaOpts),
    #[command(name = "describe", about = "Describe a dataset")]
    Describe(DescribeOpts),
    #[command(name = "head", about = "Take the first n rows of a dataset")]
    Head(HeadOpts),
    #[command(name = "sql", about = "Run a SQL query on a dataset")]
    Sql(SqlOpts),
}
