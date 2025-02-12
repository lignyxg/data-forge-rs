use crate::cli::ReplCommand;
use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};

#[derive(Debug, Parser)]
pub struct SqlOpts {
    #[arg(help = "SQL query")]
    pub sql: String,
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

    let cmd = ReplCommand::Sql(SqlOpts::new(sql));
    let (msg, rx) = ReplMsg::new(cmd);
    Ok(context.send(msg, rx))
}

impl CmdExecutor for SqlOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.sql(self).await?;
        df.display().await
    }
}
