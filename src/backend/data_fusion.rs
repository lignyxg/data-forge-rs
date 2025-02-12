use crate::cli::connect::DataSetConn;
use crate::cli::{ConnectOpts, DescribeOpts, HeadOpts, SchemaOpts, SqlOpts};
use crate::{Backend, ReplDisplay};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::{
    CsvReadOptions, NdJsonReadOptions, ParquetReadOptions, SessionConfig, SessionContext,
};
use std::ops::Deref;

pub struct DataFusionBackend(SessionContext);

impl DataFusionBackend {
    pub fn new() -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.information_schema = true;
        let ctx = SessionContext::new_with_config(config);
        Self(ctx)
    }
}

impl Default for DataFusionBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for DataFusionBackend {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Backend for DataFusionBackend {
    type DataFrame = DataFrame;
    async fn connect(&mut self, opts: &ConnectOpts) -> anyhow::Result<()> {
        match &opts.conn {
            DataSetConn::Postgres(_s) => {
                // self.register_postgres(opts.table, opts.name).await
                println!(
                    "postgres not supported for now: {:?} {}",
                    opts.table, opts.name
                );
            }
            DataSetConn::Parquet(filename) => {
                self.register_parquet(&opts.name, &filename, ParquetReadOptions::new())
                    .await?;
            }
            DataSetConn::Csv(file_opts) => {
                let csv_opts = CsvReadOptions {
                    file_extension: &file_opts.ext,
                    file_compression_type: file_opts.compression,
                    ..Default::default()
                };
                self.register_csv(&opts.name, &file_opts.filename, csv_opts)
                    .await?;
            }
            DataSetConn::Json(file_opts) => {
                let json_opt = NdJsonReadOptions {
                    file_extension: &file_opts.ext,
                    file_compression_type: file_opts.compression,
                    ..Default::default()
                };
                self.register_json(&opts.name, &file_opts.filename, json_opt)
                    .await?;
            }
        }
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Self::DataFrame> {
        let df = self
            .0
            .sql("select table_name, table_type from information_schema.tables where table_schema='public'")
            .await?;
        Ok(df)
    }

    async fn schema(&self, opts: SchemaOpts) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(&format!("DESCRIBE {}", opts.name)).await?;
        Ok(df)
    }

    async fn describe(&self, opts: DescribeOpts) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(&format!("select * from {}", opts.name)).await?;
        let df = df.describe().await?;
        Ok(df)
    }

    async fn head(&self, opts: HeadOpts) -> anyhow::Result<Self::DataFrame> {
        let df = self
            .0
            .sql(&format!(
                "SELECT * FROM {} LIMIT {}",
                opts.name,
                opts.size.unwrap_or(5)
            ))
            .await?;
        Ok(df)
    }

    async fn sql(&self, opts: SqlOpts) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(&opts.sql).await?;
        Ok(df)
    }
}

impl ReplDisplay for DataFrame {
    async fn display(self) -> anyhow::Result<String> {
        let batches = self.collect().await?;
        let ret = pretty_format_batches(&batches)?;
        Ok(ret.to_string())
    }
}
