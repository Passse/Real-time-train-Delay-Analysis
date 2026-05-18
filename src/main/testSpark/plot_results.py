import glob

import pandas as pd
from matplotlib import pyplot as plt

files = glob.glob("./data/stream_output/batch_*/*.csv")

latest_file = sorted(files)[-1]

df = pd.read_csv(latest_file)

df["window_start"] = pd.to_datetime(df["window_start"])
df["window_end"] = pd.to_datetime(df["window_end"])

# average delay by every station
station_delay = (
    df.groupby("station")["avg_delay"]
    .mean()
    .sort_values(ascending=False)
)

plt.figure()
station_delay.plot(kind="bar")
plt.xlabel("Station")
plt.ylabel("Average Delay (ms)")
plt.title("Average Delay by Station")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.show()

# total number of cancellation by every station
station_cancel = (
    df.groupby("station")["cancellation_count"]
    .sum()
    .sort_values(ascending=False)
)

plt.figure()
station_cancel.plot(kind="bar")
plt.xlabel("Station")
plt.ylabel("Cancellation Count")
plt.title("Cancellation Count by Station")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.show()

# event count over time
time_events = (
    df.groupby("window_start")["event_count"]
    .sum()
    .sort_index()
)

plt.figure()
time_events.plot(kind="line", marker="o")
plt.xlabel("Time Window")
plt.ylabel("Number of Events")
plt.title("Number of Events over Time")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.show()