import os
import time

import pandas as pd
import psycopg2
import pydeck as pdk
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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATION_COORDINATE = os.path.normpath(
    os.path.join(BASE_DIR, "../../../data/static/uk-train-stations.json")
)

def _get_connection():
    return psycopg2.connect(**DB_CONFIG)

@st.cache_data(ttl=300)
def _read_station_coordinates() -> pd.DataFrame:
    if not os.path.exists(STATION_COORDINATE):
        return pd.DataFrame(columns=["alpha3", "station_name", "latitude", "longitude"])

    coordinates = pd.read_json(STATION_COORDINATE)

    if "3alpha" in coordinates.columns:
        coordinates = coordinates.rename(columns={"3alpha": "alpha3"})

    required_cols = ["alpha3", "station_name", "latitude", "longitude"]
    existing_cols = [col for col in required_cols if col in coordinates.columns]
    coordinates = coordinates[existing_cols].copy()

    coordinates["latitude"] = pd.to_numeric(coordinates["latitude"], errors="coerce")
    coordinates["longitude"] = pd.to_numeric(coordinates["longitude"], errors="coerce")
    coordinates = coordinates.dropna(subset=["alpha3", "latitude", "longitude"])
    coordinates = coordinates.drop_duplicates(subset=["alpha3"])

    return coordinates

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

def prepare_station_map_data(movements: pd.DataFrame, coordinates: pd.DataFrame, abnormal_events: pd.DataFrame) -> pd.DataFrame:
    required_movements_cols = {"alpha3", "delay_minutes"}
    required_coordinates_cols = {"alpha3", "latitude", "longitude"}

    if movements.empty or coordinates.empty:
        return pd.DataFrame()
    if not required_movements_cols.issubset(movements.columns):
        return pd.DataFrame()
    if not required_coordinates_cols.issubset(coordinates.columns):
        return pd.DataFrame()

    station_stats = (
        movements.dropna(subset=["alpha3"])
        .groupby("alpha3")
        .agg(
            event_count=("alpha3", "count"),
            avg_delay=("delay_minutes", "mean"),
            large_delay_count=("delay_minutes", lambda x: (x >= 30).sum())
        )
        .reset_index()
    )

    abnormal_counts = prepare_station_abnormal_counts(abnormal_events)
    map_data = station_stats.merge(coordinates, on="alpha3", how="inner")
    map_data = map_data.merge(abnormal_counts, on="alpha3", how="left")
    if map_data.empty:
        return map_data

    map_data["avg_delay"] = map_data["avg_delay"].fillna(0.0)
    map_data["abnormal_count"] = map_data["abnormal_count"].fillna(0).astype(int)
    map_data["marker_size"] = (map_data["event_count"].clip(lower=1, upper=100) ** 0.5) * 1000
    map_data["abnormal_marker_size"] = (map_data["abnormal_count"].clip(lower=1, upper=20) ** 0.5) * 900
    map_data["delay_color"] = map_data["avg_delay"].clip(lower=0, upper=60).apply(
        lambda delay: [
            80 + int(delay / 60 * 175),
            180 - int(delay / 60 * 140),
            80,
            80
        ]
    )
    map_data["abnormal_color"] = map_data["abnormal_count"].apply(
        lambda count: [255, 220, 0, 230] if count > 0 else [0, 0, 0, 0]
    )
    map_data["tooltip"] = (
        map_data["station_name"].fillna(map_data["alpha3"])
        + "\nAlpha3: " + map_data["alpha3"]
        + "\nEvents: " + map_data["event_count"].astype(str)
        + "\nAverage: " + map_data["avg_delay"].round(2).astype(str) + " min"
        + "\nLarge delays: " + map_data["large_delay_count"].astype(str)
        + "\nAbnormal events: " + map_data["abnormal_count"].astype(str)
    )

    return map_data

def prepare_station_abnormal_counts(abnormal_events: pd.DataFrame) -> pd.DataFrame:
    station_abnormal_events = filter_station_abnormal_events(abnormal_events)
    if station_abnormal_events.empty or "alpha3" not in station_abnormal_events.columns:
        return pd.DataFrame(columns=["alpha3", "abnormal_count"])

    return (
        station_abnormal_events
        .groupby("alpha3", as_index=False)
        .agg(abnormal_count=("alpha3", "count"))
    )

def filter_station_movements(movements: pd.DataFrame) -> pd.DataFrame:
    if movements.empty or "alpha3" not in movements.columns:
        return pd.DataFrame()

    station_movements = movements.copy()
    station_movements = station_movements[
        station_movements["alpha3"].notna()
        & (station_movements["alpha3"] != "")
    ].copy()

    return station_movements

def filter_station_abnormal_events(abnormal_events: pd.DataFrame) -> pd.DataFrame:
    if abnormal_events.empty or "alpha3" not in abnormal_events.columns:
        return pd.DataFrame()

    station_abnormal_events = abnormal_events.copy()
    station_abnormal_events = station_abnormal_events[
        station_abnormal_events["alpha3"].notna()
        & (station_abnormal_events["alpha3"] != "")
    ].copy()

    return station_abnormal_events

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
    abnormal_min_count = st.sidebar.slider(
        "Minimum abnormal events on map",
        min_value=1,
        max_value=20,
        value=3
    )

    try:
        movements = prepare_movements(_read_table(MOVEMENTS_TABLE))
        cancellations = prepare_cancellations(_read_table(CANCELLATIONS_TABLE))
        abnormal_events = prepare_abnormal_events(_read_table(ABNORMAL_TABLE))
        station_coordinates = _read_station_coordinates()
    except Exception as e:
        st.error(f"Could not read data from PostgreSQL or station coordinates fil: {e}")
        st.stop()

    # Clean the data, which without alpha3 value
    station_movements = filter_station_movements(movements)
    # Clean the data, which without alpha3 value
    station_abnormal_events = filter_station_abnormal_events(abnormal_events)

    if STATUS_COL in movements.columns:
        delayed = movements[movements[STATUS_COL].fillna("").str.lower() == "late"].copy()
    else:
        delayed = pd.DataFrame()

    if STATUS_COL in station_movements.columns:
        station_delayed = station_movements[station_movements[STATUS_COL].str.lower() == "late"].copy()
    else:
        station_delayed = pd.DataFrame()

    avg_delay = movements["delay_minutes"].dropna().mean() if "delay_minutes" in movements.columns else None
    avg_station_delayed = station_movements["delay_minutes"].dropna().mean() if "delay_minutes" in station_movements.columns else None

    # The overall event status
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Movement events", f"{len(movements):,}")
    col2.metric("Station delayed events", f"{len(station_delayed):,}")
    col3.metric("Cancellation events", f"{len(cancellations):,}")
    col4.metric("Station abnormal events", f"{len(station_abnormal_events):,}")
    col5.metric("Average station delay", f"{avg_station_delayed:.2f} min" if avg_station_delayed is not None and pd.notna(avg_station_delayed) else "N/A")

    st.divider()

    # Train information presented by map
    st.subheader("Station Delay Map")
    station_map_data = prepare_station_map_data(station_movements, station_coordinates, abnormal_events)
    abnormal_map_data = station_map_data[
        station_map_data["abnormal_count"] >= abnormal_min_count
    ]

    # Map debug
    # with st.expander("Map data debug"):
    #     st.write("Station coordinate file:", STATION_COORDINATE)
    #     st.write("Coordinate rows:", len(station_coordinates))
    #     st.write("Station movement rows:", len(station_movements))
    #     st.write("Station map rows:", len(station_map_data))
    #     st.write("Station movement columns:", list(station_movements.columns))
    #     st.write("Coordinate columns:", list(station_coordinates.columns))
    #     if "alpha3" in station_movements.columns and "alpha3" in station_coordinates.columns:
    #         st.write("Sample movement alpha3:", station_movements["alpha3"].dropna().astype(str).head(10).tolist())
    #         st.write("Sample coordinate alpha3:", station_coordinates["alpha3"].dropna().astype(str).head(10).tolist())

    if not station_map_data.empty:
        view_state = pdk.ViewState(
            latitude=54.0,
            longitude=-2.5,
            zoom=5,
            pitch=0,
        )
        delay_layer = pdk.Layer(
            "ScatterplotLayer",
            data=station_map_data,
            get_position="[longitude, latitude]",
            get_radius="marker_size",
            get_fill_color="delay_color",
            pickable=True,
            auto_highlight=True,
        )
        abnormal_layer = pdk.Layer(
            "ScatterplotLayer",
            data=abnormal_map_data,
            get_position="[longitude, latitude]",
            get_radius="abnormal_marker_size",
            stroked=True,
            filled=True,
            get_fill_color="abnormal_color",
            pickable=True,
            auto_highlight=True,
        )
        deck = pdk.Deck(
            layers=[delay_layer, abnormal_layer],
            initial_view_state=view_state,
            tooltip={"text": "{tooltip}"}
        )
        st.pydeck_chart(deck, use_container_width=True)
    else:
        st.info("No map data available.")

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
        if STATION_COL in station_delayed.columns and not station_delayed.empty:
            top_delayed_stations = station_delayed[STATION_COL].fillna("unknown").value_counts().head(10)
            st.bar_chart(top_delayed_stations)
        else:
            st.info("No delayed station data available.")

    left, right = st.columns(2)

    with left:
        st.subheader("Top Stations by Average Delay")
        if STATION_COL in station_movements.columns and "delay_minutes" in station_movements.columns and not station_movements.empty:
            top_avg_delay = (
                station_movements.dropna(subset=["delay_minutes"])
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
        if STATION_COL in station_abnormal_events.columns and not station_abnormal_events.empty:
            top_abnormal_stations = station_abnormal_events[STATION_COL].fillna("unknown").value_counts().head(10)
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

    with st.expander("Preview station movement data"):
        st.dataframe(station_movements.head(100), use_container_width=True)

    with st.expander("Preview cancellation data"):
        st.dataframe(cancellations.head(100), use_container_width=True)

    with st.expander("Preview abnormal events"):
        st.dataframe(station_abnormal_events.head(100), use_container_width=True)

    with st.expander("Preview station map data"):
        st.dataframe(station_map_data.head(100), use_container_width=True)

    st.caption(f"Last refresh: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if auto_refresh:
        time.sleep(refresh_seconds)
        st.rerun()

if __name__ == "__main__":
    main()