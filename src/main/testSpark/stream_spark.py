import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType


spark = (
    SparkSession.builder
    .appName("TrainDelayStreaming")
    .master("local[*]")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("train_id", StringType(), True),
    StructField("station", StringType(), True),
    StructField("route", StringType(), True),
    StructField("delay_minutes", IntegerType(), True),
    StructField("is_cancelled", IntegerType(), True),
])

stream_df = (
    spark.readStream
    .option("header", "true")
    .schema(schema)
    .csv("./data/stream_input")
)

result = (
    stream_df
    .groupBy(
        window(col("event_time"), "10 minutes"),
        col("station")
    )
    .agg(
        count("*").alias("event_count"),
        avg("delay_minutes").alias("avg_delay"),
        spark_sum("is_cancelled").alias("cancellation_count")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "station",
        "event_count",
        "avg_delay",
        "cancellation_count"
    )
)

def write_batch(batch_df, batch_id):
    print(f"Writing batch {batch_id} to PostgreSQL")

    (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/trains")
        .option("dbtable", "train_delay_stats")
        .option("user", "postgres")
        .option("password", "postgres")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

query = (
    result.writeStream
    .outputMode("complete")
    .foreachBatch(write_batch)
    .option("checkpointLocation", "./data/checkpoint")
    .trigger(processingTime="5 seconds")
    .start()
)

print("JAVA_HOME =", os.environ.get("JAVA_HOME"))

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping streaming query...")
    query.stop()
    spark.stop()