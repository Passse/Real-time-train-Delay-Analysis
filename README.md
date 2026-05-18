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