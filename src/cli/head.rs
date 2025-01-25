use crate::ReplContext;
use clap::{ArgMatches, Parser};

#[derive(Debug, Parser)]
pub struct HeadOpts {
    #[arg(short, long, help = "Dataset name")]
    pub name: String,
    #[arg(short, long, help = "Number of rows")]
    pub n: Option<usize>,
}

impl HeadOpts {
    pub fn new(name: String, n: Option<usize>) -> Self {
        Self { name, n }
    }
}

impl From<HeadOpts> for super::ReplCommand {
    fn from(value: HeadOpts) -> Self {
        super::ReplCommand::Head(value)
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
    let num = args.get_one::<usize>("n").copied();

    let cmd = HeadOpts::new(name, num);
    context.send(cmd.into());
    Ok(None)
}
