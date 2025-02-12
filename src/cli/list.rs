use crate::cli::ReplCommand;
use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};

#[derive(Debug, Parser)]
pub struct ListOpts;

pub fn list(
    _args: ArgMatches,
    context: &mut ReplContext,
) -> reedline_repl_rs::Result<Option<String>> {
    let cmd = ReplCommand::List(ListOpts);
    let (msg, rx) = ReplMsg::new(cmd);
    Ok(context.send(msg, rx))
}

impl CmdExecutor for ListOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.list().await?;
        df.display().await
    }
}
