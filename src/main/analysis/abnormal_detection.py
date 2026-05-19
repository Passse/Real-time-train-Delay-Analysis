import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

MOVEMENTS_TABLE = "raw_train_movements"
CANCELLATIONS_TABLE = "raw_train_cancellations"
ABNORMAL_TABLE = "abnormal_events"

DELAY_THRESHOLD_SECONDS = 30 * 60
Z_SCORE_THRESHOLD = 3.0
CANCELLATION_SPIKE_THRESHOLD = 5


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def read_table(table_name: str) -> pd.DataFrame:
    try:
        with get_connection() as conn:
            query = f"SELECT * FROM {table_name};"
            return pd.read_sql_query(query, conn)

    except Exception as e:
        print(f"Could not read table {table_name}: {e}")
        return pd.DataFrame()


def create_abnormal_table():
    query = f"""
    CREATE TABLE IF NOT EXISTS {ABNORMAL_TABLE} (
        id SERIAL PRIMARY KEY,
        event_source TEXT,
        train_id TEXT,
        loc_stanox TEXT,
        local_name TEXT,
        event_type TEXT,
        event_timestamp TIMESTAMP,
        delay_seconds DOUBLE PRECISION,
        anomaly_type TEXT,
        anomaly_score DOUBLE PRECISION,
        explanation TEXT,
        detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()


def detect_large_delays(movements: pd.DataFrame) -> pd.DataFrame:
    if movements.empty or "delay_seconds" not in movements.columns:
        return pd.DataFrame()

    df = movements.copy()
    df["delay_seconds"] = pd.to_numeric(df["delay_seconds"], errors="coerce")
    df["event_timestamp"] = pd.to_datetime(df["actual_timestamp"], unit="ms", errors="coerce")

    abnormal = df[df["delay_seconds"] >= DELAY_THRESHOLD_SECONDS].copy()

    if abnormal.empty:
        return pd.DataFrame()

    result = pd.DataFrame({
        "event_source": "movement",
        "train_id": abnormal["train_id"],
        "loc_stanox": abnormal["loc_stanox"],
        "local_name": abnormal["local_name"],
        "event_type": abnormal["event_type"],
        "event_timestamp": abnormal["event_timestamp"],
        "delay_seconds": abnormal["delay_seconds"],
        "anomaly_type": "large_delay",
        "anomaly_score": abnormal["delay_seconds"],
        "explanation": "Train delay is at least 15 minutes."
    })

    return result


def detect_station_delay_outliers(movements: pd.DataFrame) -> pd.DataFrame:
    if movements.empty or "delay_seconds" not in movements.columns or "local_name" not in movements.columns:
        return pd.DataFrame()

    df = movements.copy()
    df["delay_seconds"] = pd.to_numeric(df["delay_seconds"], errors="coerce")
    df["event_timestamp"] = pd.to_datetime(df["actual_timestamp"], unit="ms", errors="coerce")

    df = df.dropna(subset=["delay_seconds", "local_name"])

    station_stats = (
        df.groupby("local_name")["delay_seconds"]
        .agg(["mean", "std", "count"])
        .reset_index()
    )

    df = df.merge(station_stats, on="local_name", how="left")
    df = df[(df["std"] > 0) & (df["count"] >= 5)].copy()

    df["z_score"] = (df["delay_seconds"] - df["mean"]) / df["std"]

    abnormal = df[df["z_score"] >= Z_SCORE_THRESHOLD].copy()

    if abnormal.empty:
        return pd.DataFrame()

    result = pd.DataFrame({
        "event_source": "movement",
        "train_id": abnormal["train_id"],
        "loc_stanox": abnormal["loc_stanox"],
        "local_name": abnormal["local_name"],
        "event_type": abnormal["event_type"],
        "event_timestamp": abnormal["event_timestamp"],
        "delay_seconds": abnormal["delay_seconds"],
        "anomaly_type": "station_delay_outlier",
        "anomaly_score": abnormal["z_score"],
        "explanation": "Delay is unusually high compared with other delays at the same station."
    })

    return result


def detect_cancellation_spikes(cancellations: pd.DataFrame) -> pd.DataFrame:
    if cancellations.empty or "canx_timestamp" not in cancellations.columns:
        return pd.DataFrame()

    df = cancellations.copy()
    df["event_timestamp"] = pd.to_datetime(df["canx_timestamp"], unit="ms", errors="coerce")
    df = df.dropna(subset=["event_timestamp"])

    station_col = "local_name" if "local_name" in df.columns else "loc_stanox"

    grouped = (
        df.set_index("event_timestamp")
        .groupby(station_col)
        .resample("1h")
        .size()
        .reset_index(name="cancel_count")
    )

    abnormal_windows = grouped[grouped["cancel_count"] >= CANCELLATION_SPIKE_THRESHOLD].copy()

    if abnormal_windows.empty:
        return pd.DataFrame()

    result = pd.DataFrame({
        "event_source": "cancellation",
        "train_id": None,
        "loc_stanox": None,
        "local_name": abnormal_windows[station_col],
        "event_type": "CANCELLATION_SPIKE",
        "event_timestamp": abnormal_windows["event_timestamp"],
        "delay_seconds": None,
        "anomaly_type": "cancellation_spike",
        "anomaly_score": abnormal_windows["cancel_count"],
        "explanation": "Unusually many cancellations occurred at the same station within one hour."
    })

    return result


def save_abnormal_events(abnormal_events: pd.DataFrame):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {ABNORMAL_TABLE};")
        conn.commit()

    if abnormal_events.empty:
        print("No abnormal events detected.")
        return

    columns = [
        "event_source",
        "train_id",
        "loc_stanox",
        "local_name",
        "event_type",
        "event_timestamp",
        "delay_seconds",
        "anomaly_type",
        "anomaly_score",
        "explanation",
    ]

    abnormal_events = abnormal_events[columns]

    insert_query = f"""
    INSERT INTO {ABNORMAL_TABLE} (
        event_source,
        train_id,
        loc_stanox,
        local_name,
        event_type,
        event_timestamp,
        delay_seconds,
        anomaly_type,
        anomaly_score,
        explanation
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    rows = [
        tuple(None if pd.isna(value) else value for value in row)
        for row in abnormal_events.to_numpy()
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_query, rows)
        conn.commit()

    print(f"Saved {len(abnormal_events)} abnormal events to {ABNORMAL_TABLE}.")

def _deduplicate_movements(movements: pd.DataFrame) -> pd.DataFrame:
    if movements.empty:
        return movements

    deduplicate_cols = [
        "train_id",
        "loc_stanox",
        "event_type",
        "planned_timestamp",
        "actual_timestamp",
        "variation_status",
        "delay_seconds",
    ]

    existing_cols = [col for col in deduplicate_cols if col in movements.columns]

    if not existing_cols:
        return movements.drop_duplicates().copy()

    return movements.drop_duplicates(subset=existing_cols).copy()

def _deduplicate_cancellations(cancellations: pd.DataFrame) -> pd.DataFrame:
    if cancellations.empty:
        return cancellations

    deduplicate_cols = [
        "train_id",
        "loc_stanox",
        "tiploc",
        "canx_timestamp",
        "canx_reason_code",
        "canx_type",
    ]

    existing_cols = [col for col in deduplicate_cols if col in cancellations.columns]

    if not existing_cols:
        return cancellations.drop_duplicates().copy()

    return cancellations.drop_duplicates(subset=existing_cols).copy()


def run_abnormal_detection():
    # print("Creating abnormal_events table if needed...")
    create_abnormal_table()

    # print("Reading movement data...")
    movements = read_table(MOVEMENTS_TABLE)

    # print("Reading cancellation data...")
    cancellations = read_table(CANCELLATIONS_TABLE)

    movements = _deduplicate_movements(movements)
    cancellations = _deduplicate_cancellations(cancellations)

    # print(f"Movement rows after deduplication: {len(movements)}")
    # print(f"Cancellation rows after deduplication: {len(cancellations)}")

    large_delays = detect_large_delays(movements)
    station_outliers = detect_station_delay_outliers(movements)
    cancellation_spikes = detect_cancellation_spikes(cancellations)

    abnormal_events = pd.concat(
        [large_delays, station_outliers, cancellation_spikes],
        ignore_index=True
    )

    save_abnormal_events(abnormal_events)

    # print("Abnormal detection finished successfully.")
if __name__ == "__main__":
    run_abnormal_detection()