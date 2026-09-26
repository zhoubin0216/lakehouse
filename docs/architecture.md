# Project Architecture

## Week 3 Incremental Path

```mermaid
flowchart LR
    Files["Update files + versioned manifest"] --> Updates["pipeline updates"]
    Updates --> RawMerge["Deduplicate + append raw revisions"]
    RawMerge --> NormalMerge["Clean affected keys / hours; MERGE normal"]
    NormalMerge --> IntegratedMerge["Rejoin affected trips; MERGE integrated"]
    IntegratedMerge --> Aggregates["Replace affected aggregate groups"]
    IntegratedMerge --> Products["Refresh dependent products by date / month"]
    State["Durable stage checkpoints + replay staging"] --- Updates
    Products --> Catalog["Product catalog"]
    Benchmark["Independent benchmark entrypoint"] -. reads .-> IntegratedMerge
    Updates --> Validation["Rule validation + quarantine"]
    RawMerge --> Validation
    NormalMerge --> Validation
    IntegratedMerge --> Validation
    Validation --> Summary["Validation rule summary"]
    Evaluation["Task 5 isolated evaluation"] -. measures .-> Updates
    Evaluation -. measures .-> Products
    Evaluation -. measures .-> Validation
```

The Week 1 manual full-build commands remain available. Week 3 updates use the
scoped path above; analytical product maintenance runs automatically, but query
timing/optimization benchmarks and HTML report export remain independent.

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
        ConfigYaml["configs/config.yaml<br/>current schema pointers, version history,<br/>column mappings and ingestion settings"]
    end

    subgraph Runtime["Local Runtime"]
        Pipeline["src/pipeline.py<br/>conditional step orchestration"]
        AnalysisEntry["src/data_analysis/__main__.py<br/>independent analysis entrypoint"]
        Common["src/common.py<br/>Spark, Delta IO, config helpers"]
        Viewer["src/view_table.py<br/>Delta table preview"]
    end

    subgraph Modules["Pipeline Modules"]
        Consumption["data_consumption<br/>raw table ingestion"]
        Cleaning["data_cleaning<br/>raw -> normal"]
        Integration["data_integration<br/>normal -> integrated"]
        Aggregation["data_aggregation<br/>integrated -> aggregates"]
        Analysis["data_analysis<br/>queries, products, and benchmarks"]
    end

    subgraph Metadata["Metadata"]
        Registry["source_file_registry<br/>file state and schema version"]
        Runs["ingestion_runs<br/>status, counts, and schema version"]
    end

    subgraph Lakehouse["Local Delta Lakehouse"]
        Raw["data/lakehouse/raw<br/>raw Delta tables"]
        Normal["data/lakehouse/normal<br/>cleaned tables with source versions"]
        Integrated["data/lakehouse/integrated<br/>wide tables with per-source versions"]
        Aggregate["data/lakehouse/aggregate<br/>summaries with version sets"]
        Benchmark["data/lakehouse/benchmark<br/>strategies and version snapshots"]
        Products["data/lakehouse/analysis/products<br/>four reusable Delta products"]
        ProductCatalog["analysis/product_catalog<br/>refresh and product metadata"]
        HtmlReport["data/reports<br/>standalone Task 4 HTML report"]
        Rejected["data/lakehouse/rejected<br/>consumption, cleaning,<br/>deduplication and reference rejects"]
        ValidationSummary["data/lakehouse/validation<br/>rule-level failure summary"]
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
    AnalysisEntry --> Analysis

    Consumption --> Raw
    Consumption --> Rejected
    Consumption --> Registry
    Consumption --> Runs
    Raw --> Cleaning
    Cleaning --> Rejected
    Integration --> Rejected
    Rejected --> ValidationSummary
    Cleaning --> Normal
    Normal --> Integration
    Integration --> Integrated
    Integrated --> Aggregation
    Integrated --> Analysis
    Aggregation --> Aggregate
    Aggregate --> Analysis
    Analysis --> Products
    Analysis --> ProductCatalog
    Analysis --> Benchmark
    Products --> HtmlReport

    Viewer --> Raw
    Viewer --> Normal
    Viewer --> Integrated
    Viewer --> Aggregate
    Viewer --> Benchmark
    Viewer --> Products
    Viewer --> ProductCatalog
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
    SelectStep -->|all| ConditionalRaw["Run raw consumption"]
    ConditionalRaw --> NewData{"New accepted rows?"}
    NewData -->|yes| AllSteps["Run normal -> integrated -> aggregate"]
    NewData -->|no| Stop["Stop; downstream tables unchanged"]

    BenchmarkStart["Run command<br/>python -m src.data_analysis.benchmark"] --> BenchmarkStep["run_benchmark()"]

    RawStep --> RawOutput["Raw Delta tables<br/>plus ingestion metadata"]
    NormalStep --> NormalOutput["Normal Delta tables"]
    IntegratedStep --> IntegratedOutput["Integrated taxi trips table"]
    AggregateStep --> AggregateOutput["Aggregated summary tables"]
    BenchmarkStep --> BenchmarkOutput["Benchmark tables and results"]
    ConditionalRaw --> RawOutput
    AllSteps --> NormalOutput
    NormalOutput --> IntegratedOutput
    IntegratedOutput --> AggregateOutput
```

## Storage Layout

```text
data/
  raw/                         Source files copied from the assignment dataset
  lakehouse/
    raw/                       Delta tables with row-level schema-version lineage
    normal/                    Cleaned and standardized Delta tables
    integrated/                Joined analysis-ready Delta tables
    aggregate/                 Aggregated Delta tables
    benchmark/                 Tables for storage strategy comparison
    analysis/
      products/                Four reusable analytical Delta products
      product_catalog/         Product contracts, refresh metrics, and lineage
    rejected/
      consumption/             Row-level source type conversion failures
      cleaning/                Business-rule and required-field failures
      deduplication/           Exact duplicates and conflicting revisions
      reference/               Missing lookup references
    validation/
      rule_summary/            Counts by dataset, stage, rule, and category
  metadata/
    source_file_registry/      File state and last successful schema version
    ingestion_runs/            Per-run status, schema version, counts, and errors
  reports/
    task4_data_products_report.html
                               Standalone browser visualization of Task 4 products
```

## Schema-Version Semantics

`current_schema_version` selects one definition from each dataset's
`schema_versions` mapping. The selected version is stored as `_schema_version`
when a raw record is ingested. Moving the pointer does not count as a source-file
change, so it does not automatically rebuild history. New or changed files
receive the current version; an explicit backfill is required when historical
records must be reinterpreted.

Normal, integrated, aggregate, and benchmark tables are derived products. In
particular, one integrated row may depend on several source datasets with
different schema versions, so the pipeline does not label it with one ambiguous
global version. Normal tables retain their source versions; integrated tables
use source-specific version columns; aggregate tables collect distinct version
sets; and benchmark results store a JSON version snapshot. Raw tables and
ingestion metadata remain the authoritative lineage sources.

Task 4 products are independently refreshed from a pinned Integrated Delta
version. Product rows retain their own schema version, the source Delta version,
the source schema-version snapshot, and creation/refresh timestamps. The product
catalog additionally records users, grain, materialization rationale, row count,
active Delta storage, refresh duration, and partition strategy.

Source column names and Parquet physical types are validated against the active
schema contract during consumption. CSV values are converted using declared
`column_types`; conversion failures are retained in rejected Delta tables with
their original values and reasons. Cleaning applies semantic validation and
writes its own rejected records instead of silently dropping them. Missing or
unexpected source columns are file-level contract failures and fail the run.
