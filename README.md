# Urban Data Lakehouse Platform

Course project for ID2221 Week 1: build a reusable urban data integration platform.

## Platform

- Python
- PySpark
- Delta Lake
- YAML configuration
- Local filesystem lakehouse layout

## Project Layout

```text
configs/          Dataset and platform configuration
src/              Source code for ingestion, transformation, integration, and benchmark
tests/            Unit tests
docs/             Design report, benchmark report, and architecture diagram
data/             Local raw data and generated Delta tables, ignored by Git
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python -m src.pipelines.ingest_all
python -m src.pipelines.build_integrated
python -m src.benchmark.run_benchmark
```

The pipeline expects raw datasets under `data/raw/`.
