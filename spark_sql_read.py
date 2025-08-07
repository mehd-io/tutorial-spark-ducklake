#!/usr/bin/env python3
"""
Spark SQL Reader for Ducklake
- Uses CREATE TABLE with JDBC options
- Table management in Spark catalog
- SQL-based querying approach
"""

import pyspark
import os
from pyspark.sql import SparkSession
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

# Build ducklake URI and JDBC URL
ducklake_uri = f"postgres:host={host} port={port} dbname={database} user={user} password={password}"
duckdb_url = f"jdbc:duckdb:ducklake:{ducklake_uri}"

logger.info("🚀 SPARK SQL READER: Spark SQL Tables")
logger.info("="*60)

# Create Spark session
spark = (SparkSession.builder
    .appName("Strategy2-SQLTables")
    .config('spark.jars.packages', 'org.duckdb:duckdb_jdbc:1.3.2.0')
    .config('spark.driver.defaultJavaOptions', '-Djava.security.manager=allow')
    .getOrCreate())

logger.info(f"📡 Connecting to: {host}")
logger.info(f"🔗 JDBC URL: {duckdb_url}")

try:
    # Step 1: Create database and use it
    logger.info("\n📁 Step 1: Setting up Spark database...")
    spark.sql("CREATE DATABASE IF NOT EXISTS ducklake_db")
    spark.sql("USE ducklake_db")
    
    # Step 2: Discover available tables
    logger.info("\n🔍 Step 2: Discovering available tables...")
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW ducklake_tables
        USING jdbc
        OPTIONS (
            url "{duckdb_url}",
            driver "org.duckdb.DuckDBDriver",
            dbtable "information_schema.tables"
        )
    """)
    
    available_tables = spark.sql("""
        SELECT table_name 
        FROM ducklake_tables 
        WHERE table_catalog = 'ducklake_catalog'
    """).collect()
    
    logger.info("📋 Available tables:")
    for row in available_tables:
        logger.info(f"   - {row[0]}")
    
    # Step 3: Create lineitem table
    logger.info(f"\n📊 Step 3: Creating lineitem table in Spark catalog...")
    spark.sql("DROP TABLE IF EXISTS lineitem")
    spark.sql(f"""
        CREATE TABLE lineitem
        USING jdbc
        OPTIONS (
            url "{duckdb_url}",
            driver "org.duckdb.DuckDBDriver",
            dbtable "lineitem"
        )
    """)
    
    # Step 4: Show tables in current database
    logger.info(f"\n📋 Step 4: Tables in Spark catalog:")
    spark.sql("SHOW TABLES").show()
    
    # Step 5: Get table info
    logger.info(f"\n📊 Step 5: Table information:")
    row_count = spark.sql("SELECT COUNT(*) FROM lineitem").collect()[0][0]
    logger.info(f"   Row count: {row_count:,}")
    
    # Get table schema
    logger.info(f"\n📄 Table schema:")
    spark.sql("DESCRIBE lineitem").show()
    
    # Step 6: Show sample data
    logger.info(f"\n📄 Step 6: Sample data (first 5 rows):")
    spark.sql("SELECT * FROM lineitem LIMIT 5").show()
    
    # Step 7: Run aggregation query
    logger.info(f"\n🔍 Step 7: Running aggregation query...")
    result = spark.sql("""
        SELECT 
            l_returnflag,
            l_linestatus,
            COUNT(*) as count,
            AVG(l_quantity) as avg_quantity,
            AVG(l_extendedprice) as avg_price
        FROM lineitem
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """)
    
    logger.info("📊 Query results:")
    result.show()
    
    # Step 8: Show execution plan
    logger.info(f"\n📈 Step 8: Query execution plan:")
    result.explain()

except Exception as e:
    logger.error(f"❌ Error: {e}")
finally:
    spark.stop()
    logger.success("\n✅ Spark SQL Read complete!")
