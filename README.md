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

## Planned Features

- Abnormal delay and disruption detection
- Dashboard for visualizing delay patterns
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
OPEN_RAIL_USERNAME=your_username
OPEN_RAIL_PASSWORD=your_password
OPEN_RAIL_HOST=your_host
OPEN_RAIL_PORT=your_port
OPEN_RAIL_TOPIC=your_topic

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

Run the Spark Structured Streaming pipeline:

```bash
python src/main/pySpark/structured_streaming_pipeline.py
```

The pipeline will:

1. Load the raw movement stream.
2. Process each micro-batch every 30 seconds.
3. Extract movement and cancellation events.
4. Calculate delay information.
5. Add station or location names using CORPUS data.
6. Write the processed results to PostgreSQL.

To stop the streaming pipeline, press:

```bash
Ctrl + C
```

## Output Tables

The processed data is written to PostgreSQL.

Main output tables include:

```text
movements
cancellations
```

These tables can later be used for:

- station-based aggregation

- delay frequency analysis

- cancellation counting

- abnormal disruption detection

- dashboard visualization

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