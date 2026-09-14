import snowflake.connector
from pyspark.sql import DataFrame, SparkSession

from scd2_pipeline.config import SnowflakeConfig

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"


def read_table(spark: SparkSession, config: SnowflakeConfig, table: str) -> DataFrame:
    return (
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**config.as_spark_options())
        .option("dbtable", table)
        .load()
    )


def write_table(df: DataFrame, config: SnowflakeConfig, table: str, mode: str = "append") -> None:
    (
        df.write.format(SNOWFLAKE_SOURCE)
        .options(**config.as_spark_options())
        .option("dbtable", table)
        .mode(mode)
        .save()
    )


def run_sql(config: SnowflakeConfig, statement: str) -> None:
    conn = snowflake.connector.connect(**config.as_connector_kwargs())
    try:
        conn.cursor().execute(statement)
    finally:
        conn.close()
