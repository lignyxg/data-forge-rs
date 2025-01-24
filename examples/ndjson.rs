use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::json::ReaderBuilder;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

// ndjson格式是每个数据一行
fn main() -> anyhow::Result<()> {
    /*
    CREATE TYPE gender AS ENUM ('male', 'female', 'unknown');

    CREATE TABLE user_stats(
      email varchar(128) NOT NULL PRIMARY KEY,
      name varchar(64) NOT NULL,
      gender gender DEFAULT 'unknown',
      created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
      last_visited_at timestamptz,
      last_watched_at timestamptz,
      recent_watched int[],
      viewed_but_not_started int[],
      started_but_not_finished int[],
      finished int[],
      last_email_notification timestamptz,
      last_in_app_notification timestamptz,
      last_sms_notification timestamptz
    );
    */
    let schema = Schema::new(vec![
        Field::new("email", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("gender", DataType::Utf8, false),
        Field::new("created_at", DataType::Date64, false),
        Field::new("last_visited_at", DataType::Date64, true),
        Field::new("last_watched_at", DataType::Date64, true),
        Field::new(
            "recent_watched",
            DataType::List(Arc::new(Field::new(
                "recent_watched",
                DataType::Int32,
                false,
            ))),
            true,
        ),
        Field::new(
            "viewed_but_not_started",
            DataType::List(Arc::new(Field::new(
                "viewed_but_not_started",
                DataType::Int32,
                false,
            ))),
            true,
        ),
        Field::new(
            "started_but_not_finished",
            DataType::List(Arc::new(Field::new(
                "started_but_not_finished",
                DataType::Int32,
                false,
            ))),
            true,
        ),
        Field::new(
            "finished",
            DataType::List(Arc::new(Field::new("finished", DataType::Int32, false))),
            true,
        ),
        Field::new("last_email_notification", DataType::Date64, false),
        Field::new("last_in_app_notification", DataType::Date64, false),
        Field::new("last_sms_notification", DataType::Date64, false),
    ]);

    let buf_reader = BufReader::new(File::open("assets/users.ndjson")?);
    let reader = ReaderBuilder::new(Arc::new(schema)).build(buf_reader)?;
    for batch in reader {
        let batch = batch?;
        for column in batch.columns() {
            println!("{}", column.len());
        }
    }
    Ok(())
}
