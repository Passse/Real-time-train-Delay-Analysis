import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, trim, when
from pyspark.sql.types import StructType, StructField, StringType

print("JAVA_HOME =", os.environ.get("JAVA_HOME"))

spark = (
    SparkSession.builder
    .appName("TrainMovementBatchTest")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

# body schema
movement_body_schema = StructType([
    StructField("train_id", StringType(), True),
    StructField("train_service_code", StringType(), True),
    StructField("toc_id", StringType(), True),
    StructField("loc_stanox", StringType(), True),

    # 0002 cancellation field
    StructField("canx_timestamp", StringType(), True),
    StructField("canx_reason_code", StringType(), True),
    StructField("canx_type", StringType(), True),

    # 0003 movement fields
    StructField("actual_timestamp", StringType(), True),
    StructField("planned_timestamp", StringType(), True),
    StructField("timetable_variation", StringType(), True),
    StructField("variation_status", StringType(), True),
    StructField("event_type", StringType(), True),
])

# header schema
raw_schema = StructType([
    StructField("header", StructType([
        StructField("msg_type", StringType(), True),
        StructField("msg_queue_timestamp", StringType(), True),
        StructField("source_system_id", StringType(), True)
    ]), True),
    StructField("body", movement_body_schema, True),
])

# For batch testing
def load_raw_data():
    df = spark.read.option("multiLine", "true").schema(raw_schema).json("../../../data/raw_real/*.json")

    df.select(
        col("header.msg_type").alias("msg_type"),
        col("header.msg_queue_timestamp").alias("queue_timestamp"),
        col("header.source_system_id").alias("source_system_id")
    ).show(truncate=False)

    df.groupBy(col("header.msg_type")).count().show()
    return df

# Spark Streaming
def load_raw_stream():
    return (
        spark.readStream
        .option("multiLine", "true")
        .option("maxFilesPerTrigger", 10)
        .schema(raw_schema)
        .json("../../../data/raw_real")
    )

# Read from static dataset
def load_corpus():
    corpus_raw = spark.read.option("multiLine", "true").json("../../../data/static/CORPUSExtract.json")

    corpus_df = corpus_raw.select(
        explode(col("TIPLOCDATA")).alias("loc")
    ).select(
        trim(col("loc.STANOX")).alias("stanox"),
        trim(col("loc.TIPLOC")).alias("tiploc"),
        trim(col("loc.3ALPHA")).alias("alpha3"),
        trim(col("loc.NLCDESC")).alias("local_name"),
        trim(col("loc.NLCDESC16")).alias("local_name_short")
    ).filter(
        col("stanox").isNotNull() & (col("stanox") != "")
    )

    # filter the column, which has alpha3 value. Avoid multi calculation with the same train_id.
    # Because one STANOX can have many operational location records
    corpus_df = corpus_df.withColumn(
        "has_alpha3",
        when(col("alpha3").isNotNull() & (col("alpha3") != ""), 1).otherwise(0)
    ).orderBy(
        col("stanox"),
        col("has_alpha3").desc()
    ).dropDuplicates(["stanox"]).drop("has_alpha3")

    return corpus_df