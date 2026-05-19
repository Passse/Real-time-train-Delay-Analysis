import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from matplotlib import pyplot as plt

load_dotenv()

FIGURES_DIR = Path("figures")
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

def _get_connection():
    return psycopg2.connect(**DB_CONFIG)

def read_table(table_name: str) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name};"
    with _get_connection() as conn:
        return pd.read_sql(query, conn)

def save_bar_chart(data: pd.Series, title: str, x_label: str, y_label: str, filename: str):
    if data.empty:
        print(f"Skip {filename}: no data available")
        return

    plt.figure(figsize=(10, 6 ))
    data.plot(kind="bar")
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    # plt.show()
    plt.savefig(FIGURES_DIR / f"{filename}.pdf")
    plt.close()
    print(f"Saved {filename} to {FIGURES_DIR}")

def save_line_chart(df: pd.DataFrame, x_col: str, y_col: str, title: str, x_label: str, y_label: str,filename: str):
    if df.empty:
        print(f"Skip {filename}: no data available")
        return

    plt.figure(figsize=(10, 6))
    plt.plot(df[x_col], df[y_col], marker="o")
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    # plt.show()
    plt.savefig(FIGURES_DIR / f"{filename}.pdf")
    plt.close()
    print(f"Saved {filename} to {FIGURES_DIR}")

def visualize_movements():
    movements = read_table("raw_train_movements")

    if movements.empty:
        print("No movement data found in the table: raw_train_movements")
        return

    movements = movements.drop_duplicates()
    print(f"Loaded {len(movements)} movement rows")

    station_col = "local_name"
    status_col = "variation_status"
    delay_col = "delay_seconds"
    time_col = "actual_timestamp"

    if station_col not in movements.columns:
        print(f"Skip {station_col}: no data available")
        return

    if status_col in movements.columns:
        status_distribution = movements[status_col].fillna("unknown").value_counts()
        save_bar_chart(
            status_distribution,
            "Distribution of Variation Status",
            "Variation Status",
            "Number of Events",
            "variation_status_distribution"
        )

        delayed = movements[movements[status_col].fillna("").str.lower() == "late"]
        top_delayed_stations = delayed[station_col].fillna("unknown").value_counts().head(10)
        save_bar_chart(
            top_delayed_stations,
            "Top 10 Delayed Stations",
            "Station",
            "Delayed events",
            "top_delayed_stations"
        )
    else:
        print(f"Skip delay count charts: missing column {status_col}")
        delayed = pd.DataFrame()

    if delay_col in movements.columns:
        movements["delay_minutes"] = pd.to_numeric(movements[delay_col].fillna(""), errors="coerce") / 60.0
        avg_delay_by_station = (
            movements.dropna(subset=["delay_minutes"])
            .groupby(station_col)["delay_minutes"]
            .mean()
            .sort_values(ascending=False)
            .head(10)
        )
        save_bar_chart(
            avg_delay_by_station,
            "Top 10 Stations by Average Delay",
            "Station",
            "Average delay in minutes",
            "top_avg_delay_by_stations"
        )
    else:
        print(f"Skip average delay chart: missing column {delay_col}")

    # Delay overtime
    if time_col in movements.columns and not delayed.empty:
        delayed = delayed.copy()
        delayed[time_col] = pd.to_datetime(delayed[time_col], unit="ms", errors="coerce")
        delayed = delayed.dropna(subset=[time_col])

        delay_over_time = (
            delayed.set_index(time_col)
            .resample("1h")
            .size()
            .reset_index(name="delay_count")
        )
        # delay_over_time = delay_over_time[delay_over_time["delay_count"] > 0]
        save_line_chart(
            delay_over_time,
            time_col,
            "delay_count",
            "Delay Events Over Time",
            "Time",
            "Delayed events",
            "delay_over_time"
        )
    else:
        print(f"Skip delay_over_time chart: missing column {time_col} or no delayed rows")

def visualize_cancellation():
    try:
        cancellations = read_table("raw_train_cancellations")
    except Exception as e:
        print(f"Skip cancellation charts: {e}")
        return

    if cancellations.empty:
        print("No cancellation data found in the table: raw_train_cancellations")
        return

    print(f"Loaded {len(cancellations)} cancellation rows")
    print("Cancellation columns:", list(cancellations.columns))

    station_col = "local_name"
    time_col = "canx_timestamp"
    canx_reason_col = "canx_reason_code"

    if station_col in cancellations.columns:
        top_cancelled_stations = cancellations[station_col].fillna("unknown").value_counts().head(10)
        save_bar_chart(
            top_cancelled_stations,
            "Top 10 Stations by Number of Cancellations",
            "Station",
            "Cancellation events",
            "top_cancelled_stations"
        )
    else:
        print(f"Skip cancellation count chart: missing column {station_col}")

    if canx_reason_col in cancellations.columns:
        reason_distribution = cancellations[canx_reason_col].fillna("unknown").value_counts()
        save_bar_chart(
            reason_distribution,
            "Cancellation Reason Distribution",
            "Reason code",
            "Number of cancellations",
            "cancellation_reason_distribution"
        )
    else:
        print(f"Skip cancellation reason distribution chart: missing column {canx_reason_col}")

    if time_col in cancellations.columns:
        cancellations = cancellations.copy()
        cancellations[time_col] = pd.to_datetime(cancellations[time_col], unit="ms", errors="coerce")
        cancellations = cancellations.dropna(subset=[time_col])

        cancellations_over_time = (
            cancellations.set_index(time_col)
            .resample("1h")
            .size()
            .reset_index(name="cancel_count")
        )
        # cancellations_over_time = cancellations_over_time[cancellations_over_time["cancel_count"] > 0]
        save_line_chart(
            cancellations_over_time,
            time_col,
            "cancel_count",
            "Cancellation Events Over Time",
            "Time",
            "Cancellation events",
            "cancellations_over_time"
        )
    else:
        print(f"Skip cancellation-over-time chart: missing column {time_col}")

def main():
    visualize_movements()
    visualize_cancellation()
    print(f"All figures are saved in: {FIGURES_DIR.resolve()}")

if __name__ == "__main__":
    main()