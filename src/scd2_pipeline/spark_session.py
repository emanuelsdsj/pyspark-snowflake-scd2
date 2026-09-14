from pyspark.sql import SparkSession

# Check the compatibility matrix before bumping these:
# https://docs.snowflake.com/en/user-guide/spark-connector-release-notes
SNOWFLAKE_JDBC_PACKAGE = "net.snowflake:snowflake-jdbc:3.16.1"
SNOWFLAKE_SPARK_PACKAGE = "net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4"


def build_spark_session(app_name: str = "scd2-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", f"{SNOWFLAKE_JDBC_PACKAGE},{SNOWFLAKE_SPARK_PACKAGE}")
        .getOrCreate()
    )
