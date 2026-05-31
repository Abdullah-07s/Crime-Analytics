"""
run_batch.py - Single entry point for all Spark batch analytics.
Runs locally using PySpark, connects to Dockerized PostgreSQL.

Usage:
    python spark/run_batch.py
"""
import os
import sys
import yaml
import time

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Ensure spark/ is on the path so submodules can import each other
SPARK_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SPARK_DIR)

from pyspark.sql import SparkSession
from sqlalchemy import create_engine

from preprocessing.preprocessing import (
    load_crimes, load_arrests, load_violence,
    load_sex_offenders, load_police_stations
)
from analytics.batch_analysis import (
    crime_trend_analysis, arrest_rate_analysis,
    violence_analysis, sex_offender_analysis,
    cross_dataset_correlation
)
from ml.hotspots import detect_hotspots

# Load config
config_path = os.path.join(SPARK_DIR, "..", "config", "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)


def get_pg_engine():
    """Create SQLAlchemy engine for pandas-based PostgreSQL writes."""
    pg = config['postgres']
    url = f"postgresql://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}"
    return create_engine(url)


def main():
    start = time.time()

    print("=" * 60)
    print("  Chicago Crime Analytics - Spark Batch Processing")
    print("=" * 60)

    # Create Spark session (local mode)
    # Using JDK 17 for compatibility (set JAVA_HOME before running)
    spark_tmp = os.path.join(SPARK_DIR, "..", "spark_tmp")
    os.makedirs(spark_tmp, exist_ok=True)

    spark = SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .master(config['spark']['master']) \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.local.dir", os.path.abspath(spark_tmp)) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Create PostgreSQL engine (pandas fallback - no JDBC jar needed)
    engine = get_pg_engine()

    try:
        # -- Load all datasets --
        print("\n" + "-" * 50)
        print("  PHASE 1: Loading & Preprocessing Datasets")
        print("-" * 50)

        crimes_df = load_crimes(spark)
        # Note: NOT caching to avoid disk space issues with large dataset

        arrests_df = load_arrests(spark)
        violence_df = load_violence(spark)
        sex_offenders_df = load_sex_offenders(spark)
        police_stations_df = load_police_stations(spark)

        # -- Run all analytics --
        print("\n" + "-" * 50)
        print("  PHASE 2: Running Analytics")
        print("-" * 50)

        # 1. Crime Trends
        crime_trend_analysis(crimes_df, engine=engine)

        # 2. Arrest Rates
        arrest_rate_analysis(crimes_df, arrests_df, engine=engine)

        # 3. Violence & Gunshot
        violence_analysis(violence_df, engine=engine)

        # 4. Sex Offender Proximity
        sex_offender_analysis(sex_offenders_df, police_stations_df, engine=engine)

        # 5. Cross-Dataset Correlation
        cross_dataset_correlation(crimes_df, violence_df, arrests_df, engine=engine)

        # 6. K-Means Hotspots
        print("\n" + "-" * 50)
        print("  PHASE 3: Machine Learning (K-Means Hotspots)")
        print("-" * 50)
        detect_hotspots(spark, engine=engine)

    except Exception as e:
        print(f"\n!!! ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        elapsed = time.time() - start
        print(f"\n{'=' * 60}")
        print(f"  Batch processing complete in {elapsed:.1f} seconds")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
