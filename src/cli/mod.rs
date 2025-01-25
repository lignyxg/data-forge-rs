pub(crate) mod connect;
pub(crate) mod describe;
pub(crate) mod head;
pub(crate) mod list;
pub(crate) mod sql;

use crate::cli::connect::ConnectOpts;
use crate::cli::describe::DescribeOpts;
use crate::cli::head::HeadOpts;
use crate::cli::sql::SqlOpts;
use clap::Parser;

#[derive(Debug, Parser)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to DF(data-forge-rs)"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "List all registered datasets")]
    List,
    #[command(name = "describe", about = "Describe a dataset")]
    Describe(DescribeOpts),
    #[command(name = "head", about = "Take the first n rows of a dataset")]
    Head(HeadOpts),
    #[command(name = "sql", about = "Run a SQL query on a dataset")]
    Sql(SqlOpts),
}
