import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("JAVA_HOME =", os.environ.get("JAVA_HOME"))

spark = (
    SparkSession.builder
    .appName("TrainMovementBatchTest")
    .getOrCreate()
)

def load_raw_data():
    df = spark.read.option("multiLine","true").json("../../../data/raw/*.json")

    df.select(
        col("header.msg_type").alias("msg_type"),
        col("header.msg_queue_timestamp").alias("queue_timestamp"),
        col("header.source_system_id").alias("source_system_id")
    ).show(truncate=False)

    df.groupBy(col("header.msg_type")).count().show()
    return df

def extract_cancellations(df):
    cancellation_df = (df.filter(col("header.msg_type") == "0002").select(
        col("body.train_id").alias("train_id"),
        col("body.train_service_code").alias("train_service_code"),
        col("body.toc_id").alias("toc_id"),
        col("body.loc_stanox").alias("loc_stanox"),
        col("body.canx_timestamp").cast("long").alias("canx_timestamp"),
        col("body.canx_reason_code").alias("canx_reason_code"),
        col("body.canx_type").alias("canx_type")
    ))
    cancellation_df.show(truncate=False)
    return cancellation_df

def extract_movements(df):
    movement_df = (df.filter(col("header.msg_type") == "0003").select(
        col("body.train_id").alias("train_id"),
        col("body.loc_stanox").alias("loc_stanox"),
        col("body.actual_timestamp").alias("actual_timestamp"),
        col("body.planned_timestamp").alias("planned_timestamp"),
        col("body.timetable_variation").alias("timetable_variation"),
        col("body.variation_status").alias("variation_status"),
        col("body.event_type").alias("event_type")
    ))
    movement_df.show(truncate=False)
    return movement_df

def calculate_delay(movement_df):
    delay_df = movement_df.select(
        col("train_id"),
        col("loc_stanox"),
        col("event_type"),
        col("planned_timestamp").cast("long"),
        col("actual_timestamp").cast("long"),
        col("timetable_variation").cast("int"),
        col("variation_status")
    )
    delay_df.groupBy(col("event_type")).count().show()

    delay_df = delay_df.withColumn(
        "delay_seconds",
        (col("actual_timestamp") - col("planned_timestamp")) / 1000
    )

    delay_df.select(
        "train_id",
        "loc_stanox",
        "event_type",
        "delay_seconds",
        "timetable_variation",
        "variation_status"
    ).show(truncate=False)

raw_df = load_raw_data()

print("Cancellation information:")
cancellation_df = extract_cancellations(raw_df)
cancellation_df.groupBy("canx_reason_code").count().show()
cancellation_df.groupBy("loc_stanox").count().show()

print("Movement information:")
movement_df = extract_movements(raw_df)
movement_df.groupBy("variation_status").count().show()

calculate_delay(movement_df)

spark.stop()