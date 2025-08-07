# Tutorial: Spark + Ducklake Integration

This tutorial demonstrates how to integrate Apache Spark with Ducklake for both reading and writing data operations.

## 🏗️ Demo Architecture

- **Remote Metadata Storage**: PostgreSQL database hosted on **Supabase** (cloud)
  - Stores Ducklake catalog metadata, table schemas, and transaction logs
  - Requires `.env` configuration with Supabase credentials
  - Accessed via JDBC connection from Spark

- **Local Data Storage**: Parquet files in local filesystem (`./datalake` folder)
  - Contains actual table data in columnar format
  - Created and managed automatically by Ducklake
  - Optimized for analytics workloads with Spark

## 🐳 Container Requirements

**IMPORTANT**: This tutorial should be run inside a container environment. The local paths and Ducklake folder structure are configured for demo purposes and assume a containerized environment.

### Prerequisites

**Required External Services:**
- **Supabase Account**: You need a Supabase project with PostgreSQL access
  - Sign up at [supabase.com](https://supabase.com) if you don't have an account
  - Create a new project and note your connection details
  - **CRITICAL**: Configure your `.env` file with Supabase credentials before running any scripts

**Required Local Setup:**
- Container environment (DevContainer, Docker, etc.)
- Write access to local filesystem for `./datalake` folder creation

## 📁 Project Structure

```
tutorial-spark-ducklake/
├── data/                          # Generated CSV sample data
│   ├── sales_data.csv            # Main dataset (1000 records)
│   └── additional_sales_data.csv # Additional data for append demos (100 records)
├── datalake/                     # Local Ducklake storage (created dynamically)
│   └── [parquet files]          # Generated when writing data
├── bootstrap_ducklake.py         # Setup Ducklake with sample TPC-H data
├── spark_dataframe_read.py       # Read data using Spark DataFrame API
├── spark_sql_read.py             # Read data using Spark SQL
├── spark_dataframe_write.py      # Write data using Spark DataFrame API  
├── generate_sample_data.py       # Generate CSV sample data
└── README.md                     # This file
--- .env.example                  # example of .env to fill
```

## 🚀 Getting Started

### 1. Environment Setup

First, copy the environment template and configure your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your actual Supabase credentials:
```env
SUPABASE_HOST=your-supabase-host.com
SUPABASE_PORT=6543
SUPABASE_DATABASE=postgres
SUPABASE_USER=postgres.your_project_id
SUPABASE_PASSWORD=your_actual_password
```

### 2. Bootstrap Ducklake

First, set up the Ducklake environment with sample TPC-H data:

```bash
uv run python bootstrap_ducklake.py
```

This script:
- Creates a Ducklake catalog with PostgreSQL metadata storage
- Generates TPC-H sample data (scale factor 0.1)
- Stores data locally in `./datalake` folder
- Creates the `lineitem` table for reading examples


### 3. Read Operations

#### DataFrame API Reading
```bash
uv run python spark_dataframe_read.py
```

Features:
- Direct JDBC connection with custom partitioning
- Automatic partition detection based on file indexes
- Performance optimizations for large datasets

#### SQL-based Reading
```bash
uv run python spark_sql_read.py
```

Features:
- CREATE TABLE statements in Spark catalog
- SQL-based querying approach
- Table discovery and metadata inspection

### 4. Write Operations

#### DataFrame API Writing
```bash
uv run python spark_dataframe_write.py
```
