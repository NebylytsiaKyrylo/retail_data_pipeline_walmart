# Walmart Retail Data Pipeline

## 1. Context

Walmart is the biggest retail store in the United States, with e-commerce representing a roaring $80 billion in sales by
the end of 2022 (13% of total sales). One of the main factors affecting these sales is public holidays (e.g., the Super
Bowl, Labour Day, Thanksgiving, and Christmas).

This project focuses on building a **Data Engineering pipeline (ETL)** to unify, clean, and aggregate supply and
demand data around these holidays. It bridges two distinct data sources—a PostgreSQL database and a Parquet file—to
conduct a preliminary analysis of the data and output business-ready datasets.

## 2. Objective

Develop a Python-based ETL pipeline to deliver clean data and aggregated insights:

- **Extract**: Connect to a local PostgreSQL database running in Docker and load a supplementary `.parquet` file.
- **Transform**: Merge datasets, handle missing values via mean imputation, extract date features, enforce schemas, and
  filter for high-performing sales weeks.
- **Load**: Export the cleaned dataset and a monthly aggregated sales report into structured CSV files for downstream
  analytics.

## 3. Architecture and Tech Stack

The project relies on a modern data stack running locally:

- **PostgreSQL 17 (Docker)**: Hosts the raw transactional data. Containerization ensures a deterministic and
  reproducible environment.
- **Python 3**: The core engine for the ETL process.
- **Pandas & PyArrow**: Used for in-memory data manipulation and high-performance Parquet file reading.
- **SQLAlchemy**: Serves as the database connection engine.
- **Poetry**: Manages dependencies and virtual environments reliably.

## 4. Data Catalog

### **Source 1: `grocery_sales` (PostgreSQL)**

Contains transactional sales records.

| Column         | Data Type | Description                                           |
|----------------|-----------|-------------------------------------------------------|
| `index`        | INT       | Unique ID of the row (used as the primary merge key). |  
| `Store_ID`     | INT       | The store number.                                     |
| `Date`         | TEXT      | The week of sales (requires datetime conversion).     |
| `Dept`         | INT       | Department Number in each store.                      |
| `Weekly_Sales` | NUMERIC   | Sales for the given store and department.             |

### **Source 2: `extra_data.parquet` (Local File)**

Contains complementary demographic and economic data.

| Column          | Data Type | Description                                                     |
|-----------------|-----------|-----------------------------------------------------------------|
| `IsHoliday`     | INT       | Whether the week contains a public holiday (1 if yes, 0 if no). |
| `Temperature`   | FLOAT     | Temperature on the day of sale.                                 |
| `Fuel_Price`    | FLOAT     | Cost of fuel in the region.                                     |
| `CPI`           | FLOAT     | Prevailing consumer price index.                                |
| `Unemployment`  | FLOAT     | The prevailing unemployment rate.                               |
| `MarkDown1-4`   | FLOAT     | Number of promotional markdowns.                                |
| `Size` / `Type` | INT       | Store size.                                                     |
| `Type`          | INT       | Store classification type.                                      |

## 5. The ETL Pipeline (`pipeline/`)

The pipeline is split into dedicated modules, each fully typed and logged:

| Phase           | Module / Function                               | Focus                                                                                                                                                                                                                 |
|:----------------|:------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Extract**     | `extract.py` — `extract()`                      | Connects to Postgres via SQLAlchemy, reads the Parquet file via PyArrow, and performs an `INNER JOIN` on the `index` column.                                                                                          |
| **Transform**   | `transform.py` — `transform()`                  | Cleans data: converts dates, extracts the `Month`, imputes missing numerical values (`CPI`, `Weekly_Sales`, `Unemployment`) using the column mean, enforces the final schema, and filters for `Weekly_Sales > 10000`. |
| **Aggregate**   | `aggregate.py` — `avg_weekly_sales_per_month()` | Groups the cleaned data by `Month` and calculates the average `Weekly_Sales`.                                                                                                                                         |
| **Load**        | `load.py` — `load()`                            | Saves the processed DataFrames into `clean_data.csv` and `agg_data.csv` locally.                                                                                                                                      |
| **Validate**    | `validation.py` — `validation()`                | Confirms successful file creation using the `os` module.                                                                                                                                                              |
| **Orchestrate** | `main.py`                                       | Entry point that chains all phases in order.                                                                                                                                                                          |

## 6. Project Structure

```text
.
|-- data/                                # Data folder
|   |-- raw_data/
|   |   `-- extra_data.parquet           # Complementary Parquet data source
|   `-- output/
|       |-- clean_data.csv               # Transformed and filtered dataset
|       `-- agg_data.csv                 # Aggregated monthly sales report
|
|-- pipeline/                            # ETL modules
|   |-- extract.py
|   |-- transform.py
|   |-- aggregate.py
|   |-- load.py
|   |-- validation.py
|   `-- main.py                          # Pipeline entry point
|
|-- tests/                               # Unit tests
|   |-- test_extract.py
|   |-- test_transform.py
|   |-- test_aggregate.py
|   |-- test_load.py
|   `-- test_validation.py
|
|-- init_db/                             # Database initialization
|   `-- 01_init_database.sql             # Creates the schema and base table
|
|-- docs/                                # Documentation
|   `-- requirements.md
|
|-- config.py                            # Centralized configuration
|-- docker-compose.yml                   # Infrastructure definition (Postgres)
|-- pyproject.toml / poetry.lock         # Dependency management
`-- .env                                 # Environment variables for DB credentials
```

## 7 How to Run the Project

### 7.1 Prerequisites

- Docker Desktop installed and running
- Python 3 installed
- Poetry installed for dependency management

### 7.2 Setup

Clone repository:

```bash
git clone https://github.com/NebylytsiaKyrylo/retail_data_pipeline_walmart.git
cd retail_data_pipeline_walmart
```

Create env file:

```bash
cp .env.example .env
```

Start the PostgreSQL container. The database schema and initial data will load automatically via the initialization
script.

```bash
docker compose up -d
```

Confirm the container is running and healthy:

```bash
docker ps
```

Install dependencies.

```bash
poetry install
```

To perform a clean reset of the database:

```bash
docker compose down -v
docker compose up -d
```

