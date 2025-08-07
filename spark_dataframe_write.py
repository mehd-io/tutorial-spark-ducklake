#!/usr/bin/env python3
"""
Spark DataFrame Writer for Ducklake
- Uses DataFrame API for write operations
- Reads sample data from CSV and writes to Ducklake
- Better control over write performance and error handling
"""

import pyspark
import os
from pyspark.sql import SparkSession
from loguru import logger
import subprocess
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

logger.info("🚀 SPARK DATAFRAME WRITER: Direct JDBC Writer")
logger.info("="*70)

# Create Spark session
spark = (SparkSession.builder
    .appName("DucklakeWriter")
    .config('spark.jars.packages', 'org.duckdb:duckdb_jdbc:1.3.2.0')
    .config('spark.driver.defaultJavaOptions', '-Djava.security.manager=allow')
    .getOrCreate())

def get_jdbc_writer(dataframe):
    """Setup JDBC connection for writing DataFrame to Ducklake."""
    return (dataframe.write.format('jdbc')
        .option('driver', 'org.duckdb.DuckDBDriver')
        .option('url', f'jdbc:duckdb:ducklake:{ducklake_uri}'))

def ensure_sample_data():
    """Ensure sample data exists by generating it if needed."""
    csv_path = "./data/sales_data.csv"
    if not os.path.exists(csv_path):
        logger.info("📊 Sample data not found, generating...")
        try:
            # Run the data generation script
            result = subprocess.run(["python", "generate_sample_data.py"], 
                                 capture_output=True, text=True, check=True)
            logger.info("Sample data generation output:")
            logger.info(result.stdout)
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to generate sample data: {e}")
            logger.error(f"Error output: {e.stderr}")
            raise
    else:
        logger.info(f"✅ Sample data found at {csv_path}")
    return csv_path

def load_sales_data_from_csv(csv_path="./data/sales_data.csv"):
    """Load sales data from CSV file."""
    logger.info(f"📄 Loading sales data from {csv_path}...")
    
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")  # Let Spark infer the schema automatically
          .csv(csv_path))
    
    record_count = df.count()
    logger.success(f"✅ Loaded {record_count:,} sales records from CSV")
    return df

def write_to_ducklake(dataframe, table_name, mode='overwrite'):
    """Write DataFrame to Ducklake using JDBC."""
    logger.info(f"✍️  Writing {dataframe.count():,} records to table '{table_name}'...")
    logger.info(f"   Mode: {mode}")
    
    try:
        # Write to Ducklake using DataFrame.write (not SparkSession.write!)
        (get_jdbc_writer(dataframe)
            .option('dbtable', table_name)
            .mode(mode)  # 'overwrite', 'append', 'ignore', 'error'
            .save())
        
        logger.success(f"✅ Successfully wrote to table '{table_name}'")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to write to Ducklake: {e}")
        logger.info("📄 Sample data that failed to write:")
        dataframe.show(5)
        return False

def check_parquet_files():
    """Check what parquet files were created in the datalake folder."""
    import glob
    data_path = os.getenv('DATA_PATH', '/workspaces/tutorial-spark-ducklake/datalake')
    
    logger.info(f"📁 Checking parquet files in: {data_path}")
    
    # List all parquet files
    parquet_files = glob.glob(f"{data_path}/**/*.parquet", recursive=True)
    
    if parquet_files:
        logger.success(f"✅ Found {len(parquet_files)} parquet file(s):")
        for file in sorted(parquet_files):
            rel_path = file.replace(data_path, "")
            file_size = os.path.getsize(file)
            logger.info(f"   {rel_path} ({file_size:,} bytes)")
    else:
        logger.warning("❌ No parquet files found")
    
    # Also check for any ducklake metadata files
    all_files = glob.glob(f"{data_path}/**/*", recursive=True)
    non_parquet = [f for f in all_files if os.path.isfile(f) and not f.endswith('.parquet')]
    
    if non_parquet:
        logger.info(f"\n📋 Other files in datalake:")
        for file in sorted(non_parquet):
            rel_path = file.replace(data_path, "")
            logger.info(f"   {rel_path}")

def read_and_verify(table_name):
    """Read back the data to verify the write."""
    logger.info(f"🔍 Verifying write by reading back from '{table_name}'...")
    
    try:
        # Read using the same JDBC connection
        df = (spark.read.format('jdbc')
            .option('driver', 'org.duckdb.DuckDBDriver')
            .option('url', f'jdbc:duckdb:ducklake:{ducklake_uri}')
            .option('dbtable', table_name)
            .load())
        
        row_count = df.count()
        logger.success(f"✅ Verification successful: {row_count:,} records found")
        
        # Show sample data
        logger.info("📄 Sample data from Ducklake:")
        df.show(5)
        
        # Show some aggregations
        logger.info("📊 Data summary:")
        df.createOrReplaceTempView('temp_sales')
        
        summary = spark.sql("""
            SELECT 
                COUNT(*) as total_orders,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(final_amount) as total_revenue,
                AVG(final_amount) as avg_order_value,
                COUNT(DISTINCT region) as regions_covered
            FROM temp_sales
        """)
        summary.show()
        
        # Check the actual parquet files created
        logger.info(f"\n📁 Checking physical files created:")
        check_parquet_files()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
        return False

def demonstrate_append_mode():
    """Demonstrate appending additional data."""
    logger.info("\n" + "="*60)
    logger.info("📈 DEMONSTRATING APPEND MODE")
    logger.info("="*60)
    
    # Load additional data from CSV
    additional_csv = "./data/additional_sales_data.csv"
    if not os.path.exists(additional_csv):
        logger.warning(f"Additional data file not found: {additional_csv}")
        logger.info("Using main dataset for append demo...")
        additional_csv = "./data/sales_data.csv"
    
    additional_data = load_sales_data_from_csv(additional_csv)
    
    # Write in append mode
    if write_to_ducklake(additional_data, 'spark_sales_data', mode='append'):
        logger.success("✅ Append operation successful")
        read_and_verify('spark_sales_data')

if __name__ == "__main__":
    logger.info(f"📡 Connecting to: {host}")
    logger.info(f"🔗 JDBC URL: jdbc:duckdb:ducklake:{ducklake_uri}")
    
    try:
        # Step 1: Ensure sample data exists and load it
        logger.info(f"\n📊 Step 1: Loading sample sales data...")
        csv_path = ensure_sample_data()
        sales_df = load_sales_data_from_csv(csv_path)
        
        # Show schema and sample
        logger.info("\n📋 Schema:")
        sales_df.printSchema()
        
        logger.info("\n📄 Sample data:")
        sales_df.show(5)
        
        # Step 2: Write to Ducklake
        logger.info(f"\n✍️  Step 2: Writing to Ducklake...")
        if write_to_ducklake(sales_df, 'spark_sales_data', mode='overwrite'):
            
            # Step 3: Verify the write
            logger.info(f"\n🔍 Step 3: Verifying write...")
            if read_and_verify('spark_sales_data'):
                
                # Step 4: Demonstrate append mode
                demonstrate_append_mode()
        
        logger.success(f"\n🎉 All operations completed!")
        
    except Exception as e:
        logger.error(f"❌ Pipeline error: {e}")
    finally:
        spark.stop()
        logger.info("🔌 Spark session closed")
        logger.success("✅ Write pipeline complete!")
