#!/usr/bin/env python3
"""
Spark DataFrame Reader for Ducklake
- Uses DataFrame API with explicit partitioning
- More control over performance
- Direct JDBC connection to Ducklake
"""

import pyspark
import os
from loguru import logger
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get connection configuration from environment
host = os.getenv('SUPABASE_HOST')
port = os.getenv('SUPABASE_PORT', '6543')
database = os.getenv('SUPABASE_DATABASE', 'postgres')
user = os.getenv('SUPABASE_USER')
password = os.getenv('SUPABASE_PASSWORD')

# Check for required environment variables
if not all([host, user, password]):
    logger.error("Missing required environment variables. Please check your .env file.")
    logger.info("Required: SUPABASE_HOST, SUPABASE_USER, SUPABASE_PASSWORD")
    raise ValueError("Missing required environment variables")

# Build ducklake URI
ducklake_uri = f"postgres:host={host} port={port} dbname={database} user={user} password={password}"
table_name = 'lineitem'

logger.info("🚀 SPARK DATAFRAME READER: Direct JDBC Reader with Partitioning")
logger.info("="*70)

# Create Spark session
spark = (pyspark.sql.SparkSession.builder
    .appName("Strategy1-DirectJDBC")
    .config('spark.jars.packages', 'org.duckdb:duckdb_jdbc:1.3.2.0')
    .config('spark.driver.defaultJavaOptions', '-Djava.security.manager=allow')
    .getOrCreate())

def jdbc_setup():
    """Setup JDBC connection to Ducklake."""
    return (spark.read.format('jdbc')
        .option('driver', 'org.duckdb.DuckDBDriver')
        .option('url', f'jdbc:duckdb:ducklake:{ducklake_uri}'))

logger.info(f"📡 Connecting to: {host}")
logger.info(f"📊 Reading table: {table_name}")

try:
    # Step 1: Get partitioning information
    logger.info("\n📈 Step 1: Getting partitioning information...")
    index_col_name = '__ducklake_file_index'
    
    partitioning_info = (
        jdbc_setup().option('query', f'''
            SELECT 
                min(file_index::BIGINT)::STRING min_index, 
                (max(file_index::BIGINT)+1)::STRING max_index, 
                count(DISTINCT file_index::BIGINT)::STRING num_files 
            FROM "{table_name}"''').load().collect()[0])
    
    logger.info(f"   Min index: {partitioning_info['min_index']}")
    logger.info(f"   Max index: {partitioning_info['max_index']}")
    logger.info(f"   Number of partitions: {partitioning_info['num_files']}")
    
    # Step 2: Read table with partitioning
    logger.info("\n📊 Step 2: Reading table with custom partitioning...")
    table_df = (jdbc_setup()
        .option('dbtable', f'(SELECT *, file_index::BIGINT {index_col_name} FROM "{table_name}") "{table_name}"')
        .option('partitionColumn', index_col_name)
        .option('lowerBound', partitioning_info['min_index'])
        .option('upperBound', partitioning_info['max_index'])
        .option('numPartitions', partitioning_info['num_files'])
        .load())
    
    # Step 3: Display basic info
    logger.info(f"\n📋 Step 3: Table information:")
    logger.info(f"   Row count: {table_df.count():,}")
    logger.info(f"   Columns: {len(table_df.columns)}")
    logger.info(f"   Spark partitions: {table_df.rdd.getNumPartitions()}")
    
    # Step 4: Show sample data
    logger.info(f"\n📄 Step 4: Sample data (first 5 rows):")
    table_df.show(5)
    
    # Step 5: Create temp view and run simple query
    logger.info(f"\n🔍 Step 5: Running aggregation query...")
    table_df.createOrReplaceTempView(table_name)
    
    result = spark.sql(f"""
        SELECT 
            l_returnflag,
            l_linestatus,
            COUNT(*) as count,
            AVG(l_quantity) as avg_quantity,
            AVG(l_extendedprice) as avg_price
        FROM {table_name}
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """)
    
    logger.info("📊 Query results:")
    result.show()

except Exception as e:
    logger.error(f"❌ Error: {e}")
finally:
    spark.stop()
    logger.success("\n✅ Spark DataFrame Read complete!")
