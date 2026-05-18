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