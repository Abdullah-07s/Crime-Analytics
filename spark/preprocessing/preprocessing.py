"""
Preprocessing module: Loads all 5 datasets with explicit schemas,
handles nulls, type casting, and column standardization.
"""
import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, trim, upper, lower, when, lit
)
from pyspark.sql.types import DoubleType, IntegerType

# Add parent to path for schema import
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from schemas.schemas import (
    crime_schema, police_station_schema, arrest_schema,
    violence_schema, sex_offender_schema
)


def get_data_dir():
    """Return absolute path to data directory."""
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "..", "data"
    )


def load_crimes(spark: SparkSession) -> DataFrame:
    """Load and preprocess Crime dataset."""
    data_dir = get_data_dir()
    path = os.path.join(data_dir, "Crimes_2001_to_Present.csv")
    print(f"Loading crimes from: {path}")

    df = spark.read.csv(path, schema=crime_schema, header=True, mode="DROPMALFORMED")

    # Parse Date string -> timestamp  (format: MM/dd/yyyy hh:mm:ss a)
    df = df.withColumn("ParsedDate", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))

    # Clean Arrest column -> uppercase string
    df = df.withColumn("Arrest", upper(trim(col("Arrest"))))

    # Drop rows with null case number
    df = df.filter(col("Case Number").isNotNull())

    count = df.count()
    print(f"  Crimes loaded: {count} rows")
    return df


def load_police_stations(spark: SparkSession) -> DataFrame:
    """Load and preprocess Police Stations dataset."""
    data_dir = get_data_dir()
    path = os.path.join(data_dir, "Police_Stations.csv")
    print(f"Loading police stations from: {path}")

    df = spark.read.csv(path, schema=police_station_schema, header=True, mode="DROPMALFORMED")
    df = df.filter(col("DISTRICT").isNotNull())

    count = df.count()
    print(f"  Police stations loaded: {count} rows")
    return df


def load_arrests(spark: SparkSession) -> DataFrame:
    """Load and preprocess Arrests dataset."""
    data_dir = get_data_dir()
    path = os.path.join(data_dir, "Arrests.csv")
    print(f"Loading arrests from: {path}")

    df = spark.read.csv(path, schema=arrest_schema, header=True, mode="DROPMALFORMED")

    # Parse arrest date
    df = df.withColumn("ParsedArrestDate",
                       to_timestamp(col("ARREST DATE"), "MM/dd/yyyy hh:mm:ss a"))

    df = df.filter(col("CASE NUMBER").isNotNull())

    count = df.count()
    print(f"  Arrests loaded: {count} rows")
    return df


def load_violence(spark: SparkSession) -> DataFrame:
    """Load and preprocess Violence Reduction dataset."""
    data_dir = get_data_dir()
    path = os.path.join(data_dir, "Violence_Reduction.csv")
    print(f"Loading violence data from: {path}")

    df = spark.read.csv(path, schema=violence_schema, header=True, mode="DROPMALFORMED")

    # Parse date (column is "DATE" in the CSV)
    df = df.withColumn("ParsedDate",
                       to_timestamp(col("DATE"), "MM/dd/yyyy hh:mm:ss a"))

    # Standardize gunshot injury flag
    df = df.withColumn("GUNSHOT_INJURY_I", upper(trim(col("GUNSHOT_INJURY_I"))))

    # Determine if homicide (has victim first name)
    df = df.withColumn("IS_HOMICIDE",
                       when(col("HOMICIDE_VICTIM_FIRST_NAME").isNotNull(), lit(1)).otherwise(lit(0)))

    count = df.count()
    print(f"  Violence records loaded: {count} rows")
    return df


def load_sex_offenders(spark: SparkSession) -> DataFrame:
    """Load and preprocess Sex Offenders dataset."""
    data_dir = get_data_dir()
    path = os.path.join(data_dir, "Sex_Offenders.csv")
    print(f"Loading sex offenders from: {path}")

    df = spark.read.csv(path, schema=sex_offender_schema, header=True, mode="DROPMALFORMED")

    # Standardize VICTIM MINOR flag
    df = df.withColumn("VICTIM MINOR", upper(trim(col("VICTIM MINOR"))))

    count = df.count()
    print(f"  Sex offenders loaded: {count} rows")
    return df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CrimePreprocessingTest") \
        .master("local[*]") \
        .getOrCreate()

    crimes = load_crimes(spark)
    crimes.printSchema()
    crimes.show(3, truncate=False)

    spark.stop()
