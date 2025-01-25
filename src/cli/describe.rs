use crate::ReplContext;
use clap::Parser;
use reedline_repl_rs::clap::ArgMatches;

#[derive(Debug, Parser)]
pub struct DescribeOpts {
    #[arg(short, long, help = "Dataset name")]
    pub name: String,
}

impl DescribeOpts {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl From<DescribeOpts> for super::ReplCommand {
    fn from(value: DescribeOpts) -> Self {
        super::ReplCommand::Describe(value)
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

    let cmd = DescribeOpts::new(name);
    context.send(cmd.into());
    Ok(None)
}
