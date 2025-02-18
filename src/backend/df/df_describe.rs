use anyhow::anyhow;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::compute::{cast, concat};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::functions_aggregate::average::avg;
use datafusion::functions_aggregate::count::count;
use datafusion::functions_aggregate::expr_fn::{max, median, min, stddev, sum};
use datafusion::logical_expr::{case, col, is_null, lit};
use std::sync::Arc;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct DescribeDataFrame {
    df: DataFrame,
    schema: SchemaRef,
    functions: Vec<&'static str>,
}

#[allow(unused)]
impl DescribeDataFrame {
    pub fn new(df: DataFrame) -> Self {
        let functions = vec!["count", "null_count", "mean", "std", "min", "max", "median"];
        let original_schema_fields = df.schema().fields().iter();

        //define describe column
        let mut describe_schemas = vec![Field::new("describe", DataType::Utf8, false)];
        describe_schemas.extend(original_schema_fields.clone().map(|field| {
            if field.data_type().is_numeric() {
                Field::new(field.name(), DataType::Float64, true)
            } else {
                Field::new(field.name(), DataType::Utf8, true)
            }
        }));

        let schema = Arc::new(Schema::new(describe_schemas));
        Self {
            df,
            schema,
            functions,
        }
    }

    pub(crate) async fn to_record_batch(&self) -> anyhow::Result<RecordBatch> {
        let supported_describe_functions = self.functions.clone();
        let original_schema_fields = self.df.schema().fields().iter();
        let dfs = vec![
            self.count(),
            self.null_count(),
            self.mean(),
            self.stddev(),
            self.min(),
            self.max(),
            self.median(),
        ];

        let mut describe_col_vec: Vec<ArrayRef> =
            vec![Arc::new(StringArray::from(supported_describe_functions))];
        for field in original_schema_fields {
            let mut array_data = vec![]; // 每个 array 是一列
            for result in dfs.iter() {
                let array_ref = match result {
                    Ok(df) => {
                        let batches = df.clone().collect().await;
                        match batches {
                            Ok(batches)
                                if batches.len() == 1
                                    && batches[0].column_by_name(field.name()).is_some() =>
                            {
                                let column = batches[0].column_by_name(field.name()).unwrap();

                                if column.data_type().is_null() {
                                    Arc::new(StringArray::from(vec!["null"]))
                                } else if field.data_type().is_numeric() {
                                    cast(column, &DataType::Float64)?
                                } else {
                                    cast(column, &DataType::Utf8)?
                                }
                            }
                            _ => Arc::new(StringArray::from(vec!["null"])),
                        }
                    }
                    //Handling error when only boolean/binary column, and in other cases
                    Err(err)
                        if err.to_string().contains(
                            "Error during planning: \
                                            Aggregate requires at least one grouping \
                                            or aggregate expression",
                        ) =>
                    {
                        Arc::new(StringArray::from(vec!["null"]))
                    }
                    Err(e) => return Err(anyhow!("{}", e)),
                };
                array_data.push(array_ref);
            }
            describe_col_vec.push(concat(
                array_data
                    .iter()
                    .map(|af| af.as_ref())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?);
        }

        let batch = RecordBatch::try_new(self.schema.clone(), describe_col_vec)?;
        Ok(batch)
    }

    // count aggregation
    fn count(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        self.df
            .clone()
            .aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .map(|f| count(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| e.into())
    }

    // null_count aggregation
    fn null_count(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        self.df
            .clone()
            .aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .map(|f| {
                        sum(case(is_null(col(f.name())))
                            .when(lit(true), lit(1))
                            .otherwise(lit(0))
                            .unwrap())
                        .alias(f.name())
                    })
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| e.into())
    }

    // mean aggregation
    fn mean(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        self.df
            .clone()
            .aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| avg(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| e.into())
    }

    // std aggregation
    fn stddev(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        self.df
            .clone()
            .aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| stddev(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| e.into())
    }

    // min aggregation
    fn min(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        self.df
            .clone()
            .aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                    .map(|f| min(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| e.into())
    }

    // max aggregation
    fn max(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        self.df
            .clone()
            .aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                    .map(|f| max(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| e.into())
    }

    // median aggregation
    fn median(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        self.df
            .clone()
            .aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| median(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| e.into())
    }
}
