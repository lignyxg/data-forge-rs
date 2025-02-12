pub mod backend;
pub mod cli;

use crate::backend::data_fusion::DataFusionBackend;
use crate::cli::*;
use crossbeam_channel as mpsc;
use enum_dispatch::enum_dispatch;
use reedline_repl_rs::CallBackMap;
use std::thread;
pub struct ReplContext {
    pub tx: mpsc::Sender<ReplMsg>,
}

pub struct ReplMsg {
    cmd: ReplCommand,
    tx: oneshot::Sender<String>,
}

impl ReplMsg {
    pub fn new(cmd: impl Into<ReplCommand>) -> (Self, oneshot::Receiver<String>) {
        let (tx, rx) = oneshot::channel();
        let msg = Self {
            cmd: cmd.into(),
            tx,
        };
        (msg, rx)
    }
}

#[enum_dispatch]
pub(crate) trait CmdExecutor {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String>;
}

pub(crate) trait Backend {
    type DataFrame: ReplDisplay; // 不希望Backend和某个特定的数据结构绑定，同时希望数据的展示能在CmdExecutor这一层的实现中完成
    async fn connect(&mut self, opts: &ConnectOpts) -> anyhow::Result<()>;
    async fn list(&self) -> anyhow::Result<Self::DataFrame>;
    async fn schema(&self, opts: SchemaOpts) -> anyhow::Result<Self::DataFrame>;
    async fn describe(&self, opts: DescribeOpts) -> anyhow::Result<Self::DataFrame>;
    async fn head(&self, opts: HeadOpts) -> anyhow::Result<Self::DataFrame>;
    async fn sql(&self, opts: SqlOpts) -> anyhow::Result<Self::DataFrame>;
}

pub(crate) trait ReplDisplay {
    async fn display(self) -> anyhow::Result<String>;
}

impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded::<ReplMsg>();
        let mut backend = DataFusionBackend::new();
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        thread::spawn(move || {
            while let Ok(msg) = rx.recv() {
                if let Err(e) = rt.block_on(async {
                    let ret = msg.cmd.execute(&mut backend).await?;
                    msg.tx.send(ret)?;
                    Ok::<_, anyhow::Error>(())
                }) {
                    println!("Err: {}", e);
                }
            }
        });
        Self { tx }
    }

    pub fn send(&self, cmd: ReplMsg, rx: oneshot::Receiver<String>) -> Option<String> {
        match self.tx.send(cmd) {
            Ok(_) => {}
            Err(e) => {
                println!("error sending command: {}", e);
                std::process::exit(1);
            }
        }
        match rx.recv() {
            Ok(s) => Some(s),
            Err(e) => {
                println!("error receiving command: {}", e);
                None
            }
        }
    }
}

pub fn get_callbacks() -> CallBackMap<ReplContext, reedline_repl_rs::Error> {
    let mut map = CallBackMap::new();
    map.insert("connect".to_string(), cli::connect::connect);
    map.insert("list".to_string(), cli::list::list);
    map.insert("schema".to_string(), cli::schema::schema);
    map.insert("describe".to_string(), cli::describe::describe);
    map.insert("head".to_string(), cli::head::head);
    map.insert("sql".to_string(), cli::sql::sql);
    map
}
