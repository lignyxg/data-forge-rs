use crate::cli::ReplCommand;
use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};
use clap::Parser;
use reedline_repl_rs::clap::ArgMatches;

#[derive(Debug, Parser)]
pub struct DescribeOpts {
    #[arg(help = "Dataset name")]
    pub name: String,
}

impl DescribeOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

pub fn describe(
    args: ArgMatches,
    context: &mut ReplContext,
) -> reedline_repl_rs::Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("name not found")
        .to_owned();

    let cmd = ReplCommand::Describe(DescribeOpts::new(name));
    let (msg, rx) = ReplMsg::new(cmd);
    Ok(context.send(msg, rx))
}

impl CmdExecutor for DescribeOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.describe(self).await?;
        df.display().await
    }
}
