import os

from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DRIVER = "org.postgresql.Driver"

def _check_postgres_config():
    if not POSTGRES_URL or not POSTGRES_USER or not POSTGRES_PASSWORD:
        raise Exception("Missing Postgres variables in .env.")


def _write_to_postgres(df, table_name):
    _check_postgres_config()

    print("Writing to Postgres...")

    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .mode("append")
        .save()
    )


def write_movements_to_postgres(delay_with_location):
    movement_output = delay_with_location.select(
        "train_id",
        "loc_stanox",
        "tiploc",
        "alpha3",
        "local_name",
        "event_type",
        "planned_timestamp",
        "actual_timestamp",
        "variation_status",
        "delay_seconds"
    ).dropDuplicates([
        "train_id",
        "loc_stanox",
        "event_type",
        "planned_timestamp",
        "actual_timestamp"
    ])

    _write_to_postgres(movement_output, "raw_train_movements")


def write_cancellations_to_postgres(cancellation_with_location):
    cancellation_output = cancellation_with_location.select(
        "train_id",
        "train_service_code",
        "toc_id",
        "loc_stanox",
        "tiploc",
        "alpha3",
        "local_name",
        "canx_timestamp",
        "canx_reason_code",
        "canx_type"
    ).dropDuplicates([
        "train_id",
        "loc_stanox",
        "canx_timestamp",
        "canx_type"
    ])

    _write_to_postgres(cancellation_output, "raw_train_cancellations")