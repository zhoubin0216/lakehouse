# Week 1 Urban Data Lakehouse

Course project for ID2221 Week 1: build a reusable urban data integration platform.

## Minimal Platform

- Python
- PySpark
- Delta Lake
- YAML configuration
- Local filesystem lakehouse layout

## Project Layout

```text
configs/config.yaml     Dataset and storage configuration
src/pipeline.py         Bronze, silver, and gold pipeline
src/benchmark.py        Task 6 storage strategy benchmark
tests/                  Lightweight tests
docs/report_notes.md    Notes for the final report
data/                   Raw data and generated Delta tables, ignored by Git
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python -m src.pipeline bronze
python -m src.pipeline silver
python -m src.pipeline gold
python -m src.benchmark
```

Or run the whole main pipeline:

```bash
python -m src.pipeline all
```

The pipeline expects raw datasets under:

```text
data/raw/
  yellow_taxi_trips/
  taxi_zone_lookup.csv
  weather.csv
  air_quality/hourly_88101_2024.csv
```
