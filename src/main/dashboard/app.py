import os
import time

import pandas as pd
import psycopg2
import streamlit as st
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

STATION_COL = "local_name"
STATUS_COL = "variation_status"
DELAY_COL = "delay_seconds"
MOVEMENT_TIME_COL = "actual_timestamp"
CANCELLATION_TIME_COL = "canx_timestamp"
CANCELLATION_REASON_COL = "canx_reason_code"
ABNORMAL_TIME_COL = "event_timestamp"
ANOMALY_TYPE_COL = "anomaly_type"

ANOMALY_TYPE_LABELS = {
    "large_delay": "Large Delay",
    "station_delay_outlier": "Station Delay Outlier",
    "cancellation_spike": "Cancellation Spike",
}

def _get_connection():
    return psycopg2.connect(**DB_CONFIG)

@st.cache_data(ttl=10)
def _read_table(table_name: str) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name};"
    with _get_connection() as conn:
        return pd.read_sql(query, conn)

def prepare_movements(movements: pd.DataFrame) -> pd.DataFrame:
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
    movements = movements.drop_duplicates(subset=existing_cols).copy()

    if DELAY_COL in movements.columns:
        movements["delay_minutes"] = pd.to_numeric(movements[DELAY_COL], errors="coerce") / 60.0

    if MOVEMENT_TIME_COL in movements.columns:
        movements[MOVEMENT_TIME_COL] = pd.to_datetime(
            movements[MOVEMENT_TIME_COL],
            unit="ms",
            errors="coerce",
        )

    return movements

def prepare_cancellations(cancellations: pd.DataFrame) -> pd.DataFrame:
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
    cancellations = cancellations.drop_duplicates(subset=existing_cols).copy()

    if CANCELLATION_TIME_COL in cancellations.columns:
        cancellations[CANCELLATION_TIME_COL] = pd.to_datetime(
            cancellations[CANCELLATION_TIME_COL],
            unit="ms",
            errors="coerce",
        )

    return cancellations

def prepare_abnormal_events(abnormal_events: pd.DataFrame) -> pd.DataFrame:
    if abnormal_events.empty:
        return abnormal_events

    if ABNORMAL_TIME_COL in abnormal_events.columns:
        abnormal_events[ABNORMAL_TIME_COL] = pd.to_datetime(
            abnormal_events[ABNORMAL_TIME_COL],
            errors="coerce",
        )

    deduplicate_cols = [
        "event_source",
        "train_id",
        "loc_stanox",
        "local_name",
        "event_type",
        "event_timestamp",
        "anomaly_type",
    ]

    existing_cols = [col for col in deduplicate_cols if col in abnormal_events.columns]
    abnormal_events = abnormal_events.drop_duplicates(subset=existing_cols).copy()

    return abnormal_events

# The delay/cancellation/abnormal over time
def count_by_time(df: pd.DataFrame, time_col: str, count_col: str, frequency: str) -> pd.DataFrame:
    if df.empty or time_col not in df.columns:
        return pd.DataFrame(columns=[time_col, count_col])

    # Calculate the events over time with specified frequency
    result = (
        df.dropna(subset=[time_col])
        .set_index(time_col)
        .resample(frequency)
        .size()
        .reset_index(name=count_col)
    )

    return result[result[count_col] > 0]

def main():
    st.set_page_config(
        page_title="Train Delay Dashboard",
        page_icon="🚄",
        layout="wide",
    )

    st.title("Real-Time Train Delay Dashboard")
    st.caption('Near real-time visualization of UK Network Rail movement and cancellation data.')

    st.sidebar.header("Settings")
    # The refresh time setting
    refresh_seconds = st.sidebar.slider("Refresh interval", min_value=5, max_value=60, value=10)
    auto_refresh = st.sidebar.checkbox("Auto refresh", value=True)
    aggregation_level = st.sidebar.selectbox(
        "Time aggregation",
        options=["10min", "1h", "1D"],
        index=1,
    )

    try:
        movements = prepare_movements(_read_table(MOVEMENTS_TABLE))
        cancellations = prepare_cancellations(_read_table(CANCELLATIONS_TABLE))
        abnormal_events = prepare_abnormal_events(_read_table(ABNORMAL_TABLE))
    except Exception as e:
        st.error(f"Could not read data from PostgreSQL: {e}")
        st.stop()

    if STATUS_COL in movements.columns:
        delayed = movements[movements[STATUS_COL].fillna("").str.lower() == "late"].copy()
    else:
        delayed = pd.DataFrame()

    avg_delay = movements["delay_minutes"].dropna().mean() if "delay_minutes" in movements.columns else None

    # The overall event status
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Movement events", f"{len(movements):,}")
    col2.metric("Delayed events", f"{len(delayed):,}")
    col3.metric("Cancellation events", f"{len(cancellations):,}")
    col4.metric("Abnormal events", f"{len(abnormal_events):,}")
    col5.metric("Average delay", f"{avg_delay:.2f} min" if avg_delay is not None and pd.notna(avg_delay) else "N/A")

    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader("Variation Status Distribution")
        if STATUS_COL in movements.columns and not movements.empty:
            status_distribution = movements[STATUS_COL].fillna("unknown").value_counts()
            st.bar_chart(status_distribution)
        else:
            st.info("No variation status data available.")

    with right:
        st.subheader("Top Delayed Stations")
        if STATION_COL in delayed.columns and not delayed.empty:
            top_delayed_stations = delayed[STATION_COL].fillna("unknown").value_counts().head(10)
            st.bar_chart(top_delayed_stations)
        else:
            st.info("No delayed station data available.")

    left, right = st.columns(2)

    with left:
        st.subheader("Top Stations by Average Delay")
        if STATION_COL in movements.columns and "delay_minutes" in movements.columns and not movements.empty:
            top_avg_delay = (
                movements.dropna(subset=["delay_minutes"])
                .groupby(STATION_COL)["delay_minutes"]
                .mean()
                .sort_values(ascending=False)
                .head(10)
            )
            st.bar_chart(top_avg_delay)
        else:
            st.info("No average delay data available.")

    with right:
        st.subheader("Cancellation Reason Distribution")
        if CANCELLATION_REASON_COL in cancellations.columns and not cancellations.empty:
            reason_distribution = cancellations[CANCELLATION_REASON_COL].fillna("unknown").value_counts()
            st.bar_chart(reason_distribution)
        else:
            st.info("No cancellation reason data available.")

    left, right = st.columns(2)

    with left:
        st.subheader("Abnormal Events Type Distribution")
        if ANOMALY_TYPE_COL in abnormal_events.columns and not abnormal_events.empty:
            anomaly_distribution = (
                abnormal_events[ANOMALY_TYPE_COL]
                .fillna("unknown")
                .replace(ANOMALY_TYPE_LABELS)
                .value_counts()
            )
            st.bar_chart(anomaly_distribution)
        else:
            st.info("No abnormal event data available.")

    with right:
        st.subheader("Top Stations by Abnormal Events")
        if STATION_COL in abnormal_events.columns and not abnormal_events.empty:
            top_abnormal_stations = abnormal_events[STATION_COL].fillna("unknown").value_counts().head(10)
            st.bar_chart(top_abnormal_stations)
        else:
            st.info("No station-level abnormal event data available.")

    st.divider()

    st.subheader("Delayed Events Over Time")
    delay_over_time = count_by_time(delayed, MOVEMENT_TIME_COL, "delay_count", aggregation_level)
    if not delay_over_time.empty:
        st.line_chart(delay_over_time, x=MOVEMENT_TIME_COL, y="delay_count")
    else:
        st.info("No delayed events available.")

    st.subheader("Cancellation Events Over Time")
    cancellations_over_time = count_by_time(
        cancellations,
        CANCELLATION_TIME_COL,
        "cancel_count",
        aggregation_level,
    )
    if not cancellations_over_time.empty:
        st.line_chart(cancellations_over_time, x=CANCELLATION_TIME_COL, y="cancel_count")
    else:
        st.info("No cancellation events available.")

    st.subheader("Abnormal Events Over Time")
    abnormal_over_time = count_by_time(
        abnormal_events,
        ABNORMAL_TIME_COL,
        "abnormal_count",
        aggregation_level,
    )
    if not abnormal_over_time.empty:
        st.line_chart(abnormal_over_time, x=ABNORMAL_TIME_COL, y="abnormal_count")
    else:
        st.info("No abnormal events available.")

    with st.expander("Preview movement data"):
        st.dataframe(movements.head(100), use_container_width=True)

    with st.expander("Preview cancellation data"):
        st.dataframe(cancellations.head(100), use_container_width=True)

    with st.expander("Preview abnormal events"):
        st.dataframe(abnormal_events.head(100), use_container_width=True)

    st.caption(f"Last refresh: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if auto_refresh:
        time.sleep(refresh_seconds)
        st.rerun()

if __name__ == "__main__":
    main()