use anyhow::anyhow;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::dataframe::DataFrame;
use datafusion::functions_aggregate::expr_fn::{
    approx_percentile_cont, avg, count, max, median, min, stddev, sum,
};
use datafusion::logical_expr::{case, cast, col, is_null, lit};
use datafusion::prelude::{array_length, length};
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug)]
pub struct Describer {
    original: DataFrame,
    transformed: DataFrame,
    aggregator: Vec<Aggregator>,
}

#[derive(Debug, Clone)]
pub enum Aggregator {
    Count,
    NullCount,
    Mean,
    StdDev,
    Min,
    Max,
    Median,
    Percentile(u8),
}

macro_rules! aggregate_method {
    () => {};
    ($name:ident, $method:ident) => {
        fn $name(&self) -> anyhow::Result<DataFrame> {
            let fields = self.transformed.schema().fields().iter();
            let df = self.transformed.clone().aggregate(
                vec![],
                fields
                    .map(|f| $method(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            )?;
            Ok(df)
        }
    };
}

impl Describer {
    pub fn try_new(df: DataFrame, agg: Option<Vec<Aggregator>>) -> anyhow::Result<Self> {
        let fields = df.schema().fields().iter();
        let exprs = fields
            .map(|f| {
                let dt = f.data_type();
                match dt {
                    t if t.is_numeric() => col(f.name()),
                    t if t.is_temporal() => cast(col(f.name()), DataType::Float64).alias(f.name()),
                    DataType::List(_) | DataType::LargeList(_) => {
                        array_length(col(f.name())).alias(f.name())
                    }
                    _ => length(cast(col(f.name()), DataType::Utf8)).alias(f.name()),
                }
            })
            .collect::<Vec<_>>();
        let transformed = df.clone().select(exprs)?;
        let agg = match agg {
            None => vec![
                Aggregator::Count,
                Aggregator::NullCount,
                Aggregator::Mean,
                Aggregator::StdDev,
                Aggregator::Min,
                Aggregator::Max,
                Aggregator::Median,
                Aggregator::Percentile(25),
            ],
            Some(a) => {
                let _ = a.iter().map(|m| match m {
                    Aggregator::Percentile(p) if p > &100 => {
                        return Err(anyhow!("percentile should be [0, 100], but got {}", p))
                    }
                    _ => Ok::<_, anyhow::Error>(()),
                });
                a
            }
        };

        Ok(Self {
            original: df,
            transformed,
            aggregator: agg,
        })
    }

    pub async fn describe(&self) -> anyhow::Result<DataFrame> {
        let df = self
            .aggregator
            .iter()
            .fold(None, |acc, m| {
                let d = match m {
                    Aggregator::Count => self.count().unwrap(),
                    Aggregator::NullCount => self.null_count().unwrap(),
                    Aggregator::Mean => self.mean().unwrap(),
                    Aggregator::StdDev => self.stddev().unwrap(),
                    Aggregator::Min => self.min().unwrap(),
                    Aggregator::Max => self.max().unwrap(),
                    Aggregator::Median => self.median().unwrap(),
                    Aggregator::Percentile(p) => self.percentile(p.clone()).unwrap(),
                };
                let mut select_expr = vec![lit(m.to_string()).alias("describe")];
                select_expr.extend(d.schema().fields().iter().map(|f| col(f.name())));
                let d = d.select(select_expr).unwrap();
                if acc.is_none() {
                    Some(d)
                } else {
                    Some(acc.unwrap().union(d).unwrap())
                }
            })
            .unwrap();
        let df = self.cast_back(df)?;
        Ok(df)
    }

    fn cast_back(&self, df: DataFrame) -> anyhow::Result<DataFrame> {
        let describe = Arc::new(Field::new("describe", DataType::Utf8, false));
        let mut fields = vec![&describe];
        let original = self.original.schema().fields().iter();
        fields.extend(original);
        let exprs = fields
            .iter()
            .map(|f| {
                let dt = f.data_type();
                if dt.is_temporal() {
                    cast(col(f.name()), dt.clone())
                } else {
                    col(f.name())
                }
            })
            .collect::<Vec<_>>();
        let df = df.sort(vec![col("describe").sort(true, false)])?;
        Ok(df.select(exprs)?)
    }

    aggregate_method!(mean, avg);
    aggregate_method!(stddev, stddev);
    aggregate_method!(min, min);
    aggregate_method!(max, max);
    aggregate_method!(median, median);
    aggregate_method!(count, count);

    fn null_count(&self) -> anyhow::Result<DataFrame> {
        let fields = self.transformed.schema().fields().iter();
        let df = self.transformed.clone().aggregate(
            vec![],
            fields
                .map(|f| {
                    sum(case(is_null(col(f.name())))
                        .when(lit(true), lit(1))
                        .otherwise(lit(0))
                        .unwrap())
                    .alias(f.name())
                })
                .collect::<Vec<_>>(),
        )?;
        Ok(df)
    }

    fn percentile(&self, p: u8) -> anyhow::Result<DataFrame> {
        let fields = self.transformed.schema().fields().iter();
        let df = self.transformed.clone().aggregate(
            vec![],
            fields
                .map(|f| approx_percentile_cont(col(f.name()), lit(p / 100), None).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(df)
    }
}

impl Display for Aggregator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Aggregator::Count => write!(f, "count"),
            Aggregator::NullCount => write!(f, "null_count"),
            Aggregator::Mean => write!(f, "mean"),
            Aggregator::StdDev => write!(f, "stddev"),
            Aggregator::Min => write!(f, "min"),
            Aggregator::Max => write!(f, "max"),
            Aggregator::Median => write!(f, "median"),
            Aggregator::Percentile(p) => write!(f, "percentile({})", p),
        }
    }
}
