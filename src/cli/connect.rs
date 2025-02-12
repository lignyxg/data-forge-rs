use crate::cli::ReplCommand;
use crate::{Backend, CmdExecutor, ReplContext, ReplMsg};
use clap::{ArgMatches, Parser};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;

#[derive(Debug, Parser)]
pub struct ConnectOpts {
    #[arg(value_parser = conn_parser, help = "Connection string, support postgresql, parquet, json file")]
    pub conn: DataSetConn,
    #[arg(short, long, help = "Table name, if postgres")]
    pub table: Option<String>,
    #[arg(short, long, help = "Dataset name")]
    pub name: String,
}

#[derive(Debug, Clone)]
pub enum DataSetConn {
    Postgres(String),
    Parquet(String),
    Json(FileOpts),
    Csv(FileOpts),
}

#[derive(Debug, Clone)]
pub struct FileOpts {
    pub(crate) filename: String,
    pub(crate) ext: String,
    pub(crate) compression: FileCompressionType,
}
impl FileOpts {
    pub fn new(filename: String, ext: String, compression: FileCompressionType) -> Self {
        Self {
            filename,
            ext,
            compression,
        }
    }
}

fn conn_parser(s: &str) -> Result<DataSetConn, String> {
    let s = s.to_lowercase();
    if s.starts_with("postgresql://") {
        Ok(DataSetConn::Postgres(s))
    } else {
        let exts = s.rsplit('.').collect::<Vec<_>>();
        let len = exts.len();
        let mut exts = exts.into_iter().take(len - 1);
        match (exts.next(), exts.next()) {
            (Some(ext1), Some(ext2)) => {
                let compression = match ext1 {
                    "gz" => FileCompressionType::GZIP,
                    "bz2" => FileCompressionType::BZIP2,
                    "xz" => FileCompressionType::XZ,
                    "zstd" => FileCompressionType::ZSTD,
                    v => return Err(format!("Invalid compression type: {}", v)),
                };
                let opts = FileOpts::new(s.clone(), ext2.to_string(), compression);
                match ext2 {
                    "csv" => Ok(DataSetConn::Csv(opts)),
                    "json" | "ndjson" | "jsonl" => Ok(DataSetConn::Json(opts)),
                    v => Err(format!("Invalid file type: {}", v)),
                }
            }
            (Some(ext1), None) => {
                let opts = FileOpts::new(
                    s.clone(),
                    ext1.to_string(),
                    FileCompressionType::UNCOMPRESSED,
                );
                match ext1 {
                    "csv" => Ok(DataSetConn::Csv(opts)),
                    "json" | "ndjson" | "jsonl" => Ok(DataSetConn::Json(opts)),
                    "parquet" => Ok(DataSetConn::Parquet(s)),
                    v => Err(format!("Invalid file type: {}", v)),
                }
            }
            _ => Err("failed to parse file type".to_string()),
        }
    }
}

pub fn connect(
    args: ArgMatches,
    context: &mut ReplContext,
) -> reedline_repl_rs::Result<Option<String>> {
    let conn = args
        .get_one::<DataSetConn>("conn")
        .expect("conn not found")
        .to_owned();
    let table = args.get_one::<String>("table").map(|s| s.to_string());
    let name = args
        .get_one::<String>("name")
        .expect("dataset name not found")
        .to_owned();

    let cmd = ReplCommand::Connect(ConnectOpts::new(conn, table, name));
    let (msg, rx) = ReplMsg::new(cmd);
    Ok(context.send(msg, rx))
}

impl ConnectOpts {
    pub fn new(conn: DataSetConn, table: Option<String>, name: String) -> Self {
        Self { conn, table, name }
    }
}

impl CmdExecutor for ConnectOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        backend.connect(&self).await?;
        Ok(format!("connected to dataset: {}", self.name))
    }
}
