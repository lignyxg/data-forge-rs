use crate::{Backend, CmdExecutor, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};

#[derive(Debug, Parser)]
pub struct HeadOpts {
    #[arg(help = "Dataset name")]
    pub name: String,
    #[arg(long, help = "Number of rows")]
    pub size: Option<usize>,
}

impl HeadOpts {
    pub fn new(name: String, size: Option<usize>) -> Self {
        Self { name, size }
    }
}

pub fn head(
    args: ArgMatches,
    context: &mut ReplContext,
) -> reedline_repl_rs::Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("dataset name not found")
        .to_owned();
    let num = args.get_one::<usize>("size").copied();

    // let cmd = ReplCommand::Head(HeadOpts::new(name, num));
    let (msg, rx) = ReplMsg::new(HeadOpts::new(name, num));
    Ok(context.send(msg, rx))
}

impl CmdExecutor for HeadOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.head(self).await?;
        df.display().await
    }
}
