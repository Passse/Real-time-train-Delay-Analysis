from pyspark.sql.functions import col

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
    # cancellation_df.show(truncate=False)
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
    # movement_df.show(truncate=False)
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

    delay_df = delay_df.withColumn(
        "delay_seconds",
        (col("actual_timestamp") - col("planned_timestamp")) / 1000
    )

    return delay_df

def add_location_names(df, corpus_df):
    return df.join(
        corpus_df,
        df.loc_stanox == corpus_df.stanox,
        "left"
    )