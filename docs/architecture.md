# Project Architecture

## Overall Architecture

```mermaid
flowchart LR
    subgraph Sources["Source Data"]
        TaxiFiles["NYC Yellow Taxi<br/>yellow_tripdata_2024-*.parquet"]
        ZoneFile["Taxi Zone Lookup<br/>taxi_zone_lookup.csv"]
        WeatherFile["Weather Hourly<br/>weather.csv"]
        AirQualityFile["Air Quality Hourly<br/>hourly_88101_2024.csv"]
    end

    subgraph Config["Configuration"]
        ConfigYaml["configs/config.yaml<br/>paths, schemas, column mappings,<br/>ingestion settings"]
    end

    subgraph Runtime["Local Runtime"]
        Pipeline["src/pipeline.py<br/>step orchestration"]
        Common["src/common.py<br/>Spark, Delta IO, config helpers"]
        Viewer["src/view_table.py<br/>Delta table preview"]
    end

    subgraph Modules["Pipeline Modules"]
        Consumption["data_consumption<br/>raw table ingestion"]
        Cleaning["data_cleaning<br/>raw -> normal"]
        Integration["data_integration<br/>normal -> integrated"]
        Aggregation["data_aggregation<br/>integrated -> aggregates"]
        Analysis["data_analysis<br/>benchmark and analysis"]
    end

    subgraph Metadata["Metadata"]
        Registry["source_file_registry<br/>consumed file state"]
        Runs["ingestion_runs<br/>run status and counts"]
    end

    subgraph Lakehouse["Local Delta Lakehouse"]
        Raw["data/lakehouse/raw<br/>raw Delta tables"]
        Normal["data/lakehouse/normal<br/>cleaned Delta tables"]
        Integrated["data/lakehouse/integrated<br/>analysis-ready wide tables"]
        Aggregate["data/lakehouse/aggregate<br/>summary tables"]
        Benchmark["data/lakehouse/benchmark<br/>storage strategy comparison"]
    end

    TaxiFiles --> Consumption
    ZoneFile --> Consumption
    WeatherFile --> Consumption
    AirQualityFile --> Consumption

    ConfigYaml --> Pipeline
    ConfigYaml --> Consumption
    Common --> Pipeline
    Pipeline --> Consumption
    Pipeline --> Cleaning
    Pipeline --> Integration
    Pipeline --> Aggregation
    Pipeline --> Analysis

    Consumption --> Raw
    Consumption --> Registry
    Consumption --> Runs
    Raw --> Cleaning
    Cleaning --> Normal
    Normal --> Integration
    Integration --> Integrated
    Integrated --> Aggregation
    Integrated --> Analysis
    Aggregation --> Aggregate
    Analysis --> Benchmark

    Viewer --> Raw
    Viewer --> Normal
    Viewer --> Integrated
    Viewer --> Aggregate
    Viewer --> Benchmark
```

## Pipeline Flow

```mermaid
flowchart TD
    Start["Run command<br/>python -m src.pipeline STEP"] --> LoadConfig["Load configs/config.yaml"]
    LoadConfig --> CreateSpark["Create local SparkSession<br/>with Delta Lake support"]
    CreateSpark --> SelectStep{"STEP"}

    SelectStep -->|raw| RawStep["build_raw_tables()"]
    SelectStep -->|normal| NormalStep["build_normal_tables()"]
    SelectStep -->|integrated| IntegratedStep["build_integrated_tables()"]
    SelectStep -->|aggregate| AggregateStep["build_aggregate_tables()"]
    SelectStep -->|benchmark| BenchmarkStep["run_benchmark()"]
    SelectStep -->|all| AllSteps["Run raw -> normal -> integrated -> aggregate -> benchmark"]

    RawStep --> RawOutput["Raw Delta tables<br/>plus ingestion metadata"]
    NormalStep --> NormalOutput["Normal Delta tables"]
    IntegratedStep --> IntegratedOutput["Integrated taxi trips table"]
    AggregateStep --> AggregateOutput["Aggregated summary tables"]
    BenchmarkStep --> BenchmarkOutput["Benchmark tables and results"]
    AllSteps --> RawOutput
    RawOutput --> NormalOutput
    NormalOutput --> IntegratedOutput
    IntegratedOutput --> AggregateOutput
    AggregateOutput --> BenchmarkOutput
```

## Storage Layout

```text
data/
  raw/                         Source files copied from the assignment dataset
  lakehouse/
    raw/                       Delta tables produced by data consumption
    normal/                    Cleaned and standardized Delta tables
    integrated/                Joined analysis-ready Delta tables
    aggregate/                 Aggregated Delta tables
    benchmark/                 Tables for storage strategy comparison
  metadata/
    source_file_registry/      File-level incremental ingestion state
    ingestion_runs/            Per-run status, row counts, and errors
```

## Current Week 1 Scope

The current implemented data path is:

```text
data/raw -> src/data_consumption -> data/lakehouse/raw + data/metadata
```

The later steps already have separate module folders and pipeline hooks, but their
business logic is intentionally kept minimal until the cleaning, integration,
aggregation, and benchmark tasks are implemented.
