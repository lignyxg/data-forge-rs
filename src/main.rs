use data_forge_rs::cli::ReplCommand;
use data_forge_rs::ReplContext;
use reedline_repl_rs::Repl;

fn main() -> reedline_repl_rs::Result<()> {
    let ctx = ReplContext::new();
    let callbacks = data_forge_rs::get_callbacks();

    let mut repl = Repl::new(ctx)
        .with_banner("Welcome to Data Forge, your data exploration companion")
        .with_derived::<ReplCommand>(callbacks);

    repl.run()
}
