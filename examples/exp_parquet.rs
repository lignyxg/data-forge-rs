use arrow::array::AsArray as _;
use datafusion::arrow::array::AsArray;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use polars::prelude::*;
use polars::sql::SQLContext;
use std::fs::File;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = "assets/sample.parquet";
    println!("------ read with parquet ------");
    read_with_parquet(file)?;

    println!("------ read with datafusion ------");
    read_with_datafusion(file).await?;

    println!("------ read with datafusion2 ------");
    read_with_datafusion2(file).await?;

    println!("------ read with polars ------");
    read_with_polars(file)?;

    println!("------ read with polars2 ------");
    read_with_polars2(file)?;
    Ok(())
}

fn read_with_parquet(file: &str) -> anyhow::Result<()> {
    let file = File::open(file)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .with_limit(3)
        .build()?;

    for batch in reader {
        let batch = batch?;
        let record = batch.column(0).as_string::<i32>();
        for r in record {
            println!("{:?}", r.unwrap());
        }
    }
    Ok(())
}

async fn read_with_datafusion(file: &str) -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    let df = ctx.read_parquet(file, ParquetReadOptions::new()).await?;
    let mut stream = df.execute_stream().await?;
    // println!("stream size: {:?}", stream.size_hint()); // size = 1
    // let mut stream = stream.take(2);
    while let Some(Ok(batch)) = stream.next().await {
        let emails = batch.column(0).as_string_view();
        for email in emails.iter().take(3) {
            println!("{:?}", email.unwrap());
        }
    }
    Ok(())
}

async fn read_with_datafusion2(file: &str) -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet("stats", file, ParquetReadOptions::new())
        .await?;
    let df = ctx
        .sql("SELECT email, name FROM stats limit 3")
        .await?
        .collect()
        .await?;
    // println!("len: {:?}", df.len());
    for rb in df {
        let rr = rb.column(0);
        println!("{:?}", rr);
    }
    Ok(())
}

fn read_with_polars(file: &str) -> anyhow::Result<()> {
    // let frame = LazyFrame::scan_parquet(file, Default::default())?;
    // let da = frame.select([
    //     col("email"),
    //     col("name"),
    //     col("gender"),
    // ]).limit(3).collect()?;
    // println!("{:?}", da);

    let file = File::open(file)?;
    let df = ParquetReader::new(file)
        .with_columns(Some(vec!["email".to_string(), "name".to_string()]))
        .finish()?;
    let df = df.head(Some(3));
    println!("frame: {:?}", df);

    Ok(())
}

fn read_with_polars2(file: &str) -> anyhow::Result<()> {
    let frame = LazyFrame::scan_parquet(file, Default::default())?;
    let mut ctx = SQLContext::new();
    ctx.register("stats", frame);
    let df = ctx
        .execute("SELECT email, name FROM stats limit 3")?
        .collect()?;
    println!("{:?}", df);
    Ok(())
}
