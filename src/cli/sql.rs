use crate::ReplContext;
use clap::{ArgMatches, Parser};

#[derive(Debug, Parser)]
pub struct SqlOpts {
    #[arg(long, help = "SQL query")]
    pub sql: String,
}

impl From<SqlOpts> for super::ReplCommand {
    fn from(value: SqlOpts) -> Self {
        super::ReplCommand::Sql(value)
    }
}

impl SqlOpts {
    pub fn new(sql: String) -> Self {
        Self { sql }
    }
}

pub fn sql(
    args: ArgMatches,
    context: &mut ReplContext,
) -> reedline_repl_rs::Result<Option<String>> {
    let sql = args
        .get_one::<String>("sql")
        .expect("sql not found")
        .to_owned();

    let cmd = SqlOpts::new(sql);
    context.send(cmd.into());
    Ok(None)
}
