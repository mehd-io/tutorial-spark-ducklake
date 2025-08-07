#!/usr/bin/env python3

import duckdb
import os
from loguru import logger
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def create_ducklake_with_data(data_path=None):
    """Create a simple Ducklake with PostgreSQL metadata and local data storage."""
    
    # Use default data path if not specified
    if data_path is None:
        data_path = os.getenv('DATA_PATH', '/workspaces/tutorial-spark-ducklake/datalake')
    
    logger.info(f"🦆 Creating Ducklake with data path: {data_path}")
    
    # Ensure data path exists
    os.makedirs(data_path, exist_ok=True)
    
    # Create DuckDB connection
    conn = duckdb.connect()
    
    # Install required extensions
    logger.info("📦 Installing extensions...")
    conn.execute("INSTALL ducklake;")
    conn.execute("INSTALL postgres;")
    conn.execute("INSTALL tpch;")
    
    # Create PostgreSQL secret for metadata storage
    logger.info("🔐 Setting up PostgreSQL secret...")
    
    # Get configuration from environment variables
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
    
    conn.execute(f"""
        CREATE SECRET (
            TYPE postgres,
            HOST '{host}',
            PORT {port},
            DATABASE {database},
            USER '{user}',
            PASSWORD '{password}'
        );
    """)
    
    # Create ducklake with postgres metadata and local data storage
    logger.info("🔗 Creating Ducklake catalog with PostgreSQL metadata...")
    
    conn.execute(f"""
        ATTACH 'ducklake:postgres:dbname=postgres' AS ducklake_catalog (
            DATA_PATH '{data_path}'
        );
    """)
    
    # First, generate TPC-H data in memory
    logger.info("📊 Generating TPC-H data in memory (scale factor 0.1)...")
    conn.execute("USE memory;")
    conn.execute("CALL dbgen(sf = 0.1);")
    
    # Show what was created in memory
    tables = conn.execute("SHOW TABLES;").fetchall()
    logger.info(f"Tables created in memory: {tables}")
    
    # Copy tables from memory to ducklake
    logger.info("📋 Copying tables from memory to Ducklake...")
    conn.execute("USE ducklake_catalog;")
    
    conn.execute(f"CREATE TABLE ducklake_catalog.lineitem AS SELECT * FROM memory.lineitem;")
    
    # Show what was created in ducklake
    tables = conn.execute("SHOW TABLES;").fetchall()
    logger.info(f"Tables created in ducklake: {tables}")
    
    # Show lineitem count
    lineitem_count = conn.execute("SELECT COUNT(*) FROM lineitem;").fetchone()[0]
    logger.info(f"📦 lineitem table has {lineitem_count:,} records")
    
    conn.close()
    logger.success("✅ Ducklake setup complete!")

if __name__ == "__main__":
    create_ducklake_with_data()
