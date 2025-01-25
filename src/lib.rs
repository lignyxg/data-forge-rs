pub mod cli;

use crate::cli::ReplCommand;
use crossbeam_channel as mpsc;
use reedline_repl_rs::CallBackMap;
use std::thread;

pub struct ReplContext {
    pub tx: mpsc::Sender<ReplCommand>,
}

impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded();
        thread::spawn(move || {
            while let Ok(cmd) = rx.recv() {
                println!("{:?}", cmd);
            }
        });
        Self { tx }
    }

    pub fn send(&self, cmd: ReplCommand) {
        match self.tx.send(cmd) {
            Ok(_) => {}
            Err(e) => {
                println!("error sending command: {}", e);
                std::process::exit(1);
            }
        }
    }
}

pub fn get_callbacks() -> CallBackMap<ReplContext, reedline_repl_rs::Error> {
    let mut map = CallBackMap::new();
    map.insert("connect".to_string(), cli::connect::connect);
    map.insert("list".to_string(), cli::list::list);
    map.insert("describe".to_string(), cli::describe::describe);
    map.insert("head".to_string(), cli::head::head);
    map.insert("sql".to_string(), cli::sql::sql);
    map
}
