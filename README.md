# Real-Time Train Delay Analysis with Apache Spark

This project analyzes real-time train movement data from the UK rail network using Apache Spark Structured Streaming.

## Project Overview

The goal of this project is to process real-time railway movement events and analyze delay and disruption patterns across stations and time windows.
The pipeline ingests train movement events, cleans and enriches the data with static location information, performs window-based aggregation, and stores the results in PostgreSQL for further analysis and visualization.

## Data Sources

- Network Rail MOVEMENT feed: real-time train movement events
- Static SCHEDULE / CORPUS data: planned timetable and location reference data

## Technologies

- Python
- Apache Spark / PySpark
- Spark Structured Streaming
- PostgreSQL
- Docker

## Current Features

- Parse real-time movement events
- Extract relevant train, time, and location attributes
- Join movement events with static station/location data
- Compute simple aggregation statistics by station and time window
- Store aggregated results in PostgreSQL
- Abnormal delay and disruption detection
- Dashboard for visualizing delay patterns

## Planned Features

- Optional clustering of stations with similar disruption behavior

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/Passse/Real-time-train-Delay-Analysis.git
cd Real-time-train-Delay-Analysis
```

### 2. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root directory.

Example:

```env
NR_USERNAME=your_username
NR_PASSWORD=your_password

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=rail_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
```

The `.env` file contains private credentials and must not be uploaded to GitHub.

## PostgreSQL Setup

Start PostgreSQL before running the streaming pipeline.

If PostgreSQL is running in Docker:

```bash
docker start <postgres_container_name>
```

If using Docker Compose:

```bash
docker compose up -d
```

Make sure the required database exists before running the pipeline.

Example:

```sql
CREATE DATABASE rail_db;
```

## How to Run

The pipeline should be started in the following order:

1. Start PostgreSQL.
2. Run the movement collector to collect raw real-time movement messages.
3. Run the Spark Structured Streaming pipeline to process and aggregate the collected data.
4. Start the dashboard to visualize the processed results.

### 1. Start PostgreSQL

If PostgreSQL is managed with Docker Compose, start it from the project root directory:

```bash
docker compose up -d
```

You can check whether the PostgreSQL container is running with:

```bash
docker ps
```

### 2. Run the movement collector

The movement collector subscribes to the Network Rail MOVEMENT feed and stores raw movement messages under `data/raw_real/`.

Run it with:

```bash
python src/main/pySpark/movement_collector.py
```

The collector keeps running and continuously writes incoming messages to local JSON files.

To stop the collector, press:

```bash
Ctrl + C
```

### 3. Run the Spark Structured Streaming pipeline

After raw movement messages are being collected, run the Spark processing pipeline:

```bash
python src/main/pySpark/structured_streaming_pipeline.py
```
### 4. Start the dashboard

After PostgreSQL contains processed aggregation and abnormal event results, start the dashboard:

```bash
streamlit run src/main/dashboard/app.py
```

The dashboard reads the processed results form PostgreSQL and visualized delay patterns, station-level statistics, and abnormal delay events.

The Spark pipeline will:

1. Read raw movement messages.
2. Process each micro-batch.
3. Extract movement and cancellation events.
4. Calculate delay information.
5. Add station or location names using CORPUS data.
6. Write the processed results to PostgreSQL.

To stop the Spark pipeline, press:

```bash
Ctrl + C
```

## Authors

S.Qie and Ashraf Jafarli

University of Bassel
Distributed Information Systems, Spring 2026

## Pipeline

```text
MOVEMENT stream
       ↓
Spark Structured Streaming
       ↓
clean + enrich with CORPUS
       ↓
window aggregation
       ↓
PostgreSQL table: station_window_stats
       ↓
Pandas / Python abnormal detection
       ↓
PostgreSQL table: abnormal_events
       ↓
dashboard / report