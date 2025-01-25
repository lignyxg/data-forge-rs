use crate::ReplContext;
use clap::ArgMatches;

pub fn list(
    args: ArgMatches,
    context: &mut ReplContext,
) -> reedline_repl_rs::Result<Option<String>> {
    let cmd = super::ReplCommand::List;
    context.send(cmd);
    Ok(None)
}
