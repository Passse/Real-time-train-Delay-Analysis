from pyspark.sql.functions import col, count, avg


def show_statistics(cancellation_df, movement_df, delay_df):
    print("Cancellation information:")
    cancellation_df.show(truncate=False)
    cancellation_df.groupBy("canx_reason_code").count().show()
    cancellation_df.groupBy("local_name").count().show()

    print("Movement information:")
    movement_df.show(truncate=False)
    movement_df.groupBy("variation_status").count().show()

    print("Delay information:")
    delay_df.select(
        "train_id",
        "local_name",
        "event_type",
        "delay_seconds",
        "timetable_variation",
        "variation_status"
    ).show(truncate=False)
    delay_df.groupBy(col("event_type")).count().show()

    # The number of every event for every location
    delay_df.groupBy("local_name", "variation_status").count().show()

    # Which location mostly occurs late movement
    (
        delay_df.filter(col("variation_status") == "LATE")
                .groupBy("local_name")
                .count()
                .orderBy(col("count").desc())
                .show(20, truncate=False)
    )

    # Average delay by location
    (
        delay_df.filter(col("variation_status") == "LATE").groupBy("local_name").agg(
            count("*").alias("late_count"),
            avg("delay_seconds").alias("avg_delay_seconds")
        ).orderBy(
            col("avg_delay_seconds").desc()
        ).show(20, truncate=False)
    )