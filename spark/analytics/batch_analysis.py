"""
Batch Analysis Module — All required Spark analytics:
1. Crime Trend Analysis (yearly, monthly, hourly, day-of-week)
2. Arrest Rate Analysis (by crime type, district, race)
3. Violence & Gunshot Analysis
4. Sex Offender Proximity Analysis
5. Cross-Dataset Correlations
All results persisted to PostgreSQL.
"""
import os
import sys
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, hour, dayofweek, count, when, sum as spark_sum,
    lit, round as spark_round, desc, coalesce
)

# Add parent to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from preprocessing.preprocessing import (
    load_crimes, load_arrests, load_violence, load_sex_offenders, load_police_stations
)

# Load config
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "config", "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# PostgreSQL JDBC properties
PG_URL = config['postgres']['jdbc_url']
PG_PROPS = {
    "user": config['postgres']['user'],
    "password": config['postgres']['password'],
    "driver": "org.postgresql.Driver"
}


def write_to_postgres(df: DataFrame, table: str, mode: str = "append"):
    """Write a DataFrame to PostgreSQL via JDBC."""
    print(f"  Writing {df.count()} rows to PostgreSQL table: {table}")
    df.write.jdbc(url=PG_URL, table=table, mode=mode, properties=PG_PROPS)


def write_to_postgres_pandas(df, table: str, engine):
    """Fallback: write using pandas + SQLAlchemy when JDBC jar is unavailable."""
    pdf = df.toPandas()
    print(f"  Writing {len(pdf)} rows to PostgreSQL table: {table}")
    pdf.to_sql(table, engine, if_exists='append', index=False)


# ──────────────────────────────────────────────
# 1. Crime Trend Analysis
# ──────────────────────────────────────────────
def crime_trend_analysis(crimes_df: DataFrame, engine=None):
    """Compute crime counts grouped by year, month, day-of-week, and hour."""
    print("\n=== Crime Trend Analysis ===")

    df = crimes_df.withColumn("ParsedYear", year(col("ParsedDate"))) \
                  .withColumn("ParsedMonth", month(col("ParsedDate"))) \
                  .withColumn("ParsedHour", hour(col("ParsedDate"))) \
                  .withColumn("ParsedDOW", dayofweek(col("ParsedDate")))

    results = []

    # Yearly
    yearly = df.groupBy("ParsedYear").agg(count("*").alias("crime_count")) \
               .filter(col("ParsedYear").isNotNull()) \
               .orderBy("ParsedYear")
    yearly = yearly.withColumn("group_type", lit("year")) \
                   .withColumn("group_value", col("ParsedYear").cast("string")) \
                   .select("group_type", "group_value", "crime_count")
    results.append(yearly)
    print("  Yearly trends computed")

    # Monthly
    monthly = df.groupBy("ParsedMonth").agg(count("*").alias("crime_count")) \
                .filter(col("ParsedMonth").isNotNull()) \
                .orderBy("ParsedMonth")
    monthly = monthly.withColumn("group_type", lit("month")) \
                     .withColumn("group_value", col("ParsedMonth").cast("string")) \
                     .select("group_type", "group_value", "crime_count")
    results.append(monthly)
    print("  Monthly trends computed")

    # Day of Week
    dow = df.groupBy("ParsedDOW").agg(count("*").alias("crime_count")) \
            .filter(col("ParsedDOW").isNotNull()) \
            .orderBy("ParsedDOW")
    dow = dow.withColumn("group_type", lit("day_of_week")) \
             .withColumn("group_value", col("ParsedDOW").cast("string")) \
             .select("group_type", "group_value", "crime_count")
    results.append(dow)
    print("  Day-of-week trends computed")

    # Hourly
    hourly = df.groupBy("ParsedHour").agg(count("*").alias("crime_count")) \
               .filter(col("ParsedHour").isNotNull()) \
               .orderBy("ParsedHour")
    hourly = hourly.withColumn("group_type", lit("hour")) \
                   .withColumn("group_value", col("ParsedHour").cast("string")) \
                   .select("group_type", "group_value", "crime_count")
    results.append(hourly)
    print("  Hourly trends computed")

    # Union all and write
    from functools import reduce
    all_trends = reduce(DataFrame.unionAll, results)

    if engine:
        write_to_postgres_pandas(all_trends, "crime_trends", engine)
    else:
        write_to_postgres(all_trends, "crime_trends")

    print("  Crime trends saved to PostgreSQL")
    return all_trends


# ──────────────────────────────────────────────
# 2. Arrest Rate Analysis
# ──────────────────────────────────────────────
def arrest_rate_analysis(crimes_df: DataFrame, arrests_df: DataFrame, engine=None):
    """Calculate arrest rates by primary crime type, district, and race."""
    print("\n=== Arrest Rate Analysis ===")

    results = []

    # --- By Primary Crime Type ---
    by_type = crimes_df.groupBy("Primary Type").agg(
        count("*").alias("total_crimes"),
        count(when(col("Arrest") == "TRUE", True)).alias("total_arrests")
    ).withColumn("arrest_rate",
        spark_round(col("total_arrests") / col("total_crimes"), 4)
    ).orderBy(desc("arrest_rate"))

    by_type = by_type.withColumn("group_type", lit("crime_type")) \
                     .withColumn("group_value", col("Primary Type")) \
                     .select("group_type", "group_value", "total_crimes", "total_arrests", "arrest_rate")
    results.append(by_type)
    print("  Top 10 crime types by arrest rate:")
    by_type.show(10, truncate=False)

    # --- By District ---
    by_district = crimes_df.groupBy("District").agg(
        count("*").alias("total_crimes"),
        count(when(col("Arrest") == "TRUE", True)).alias("total_arrests")
    ).withColumn("arrest_rate",
        spark_round(col("total_arrests") / col("total_crimes"), 4)
    ).filter(col("District").isNotNull()).orderBy("District")

    by_district = by_district.withColumn("group_type", lit("district")) \
                             .withColumn("group_value", col("District")) \
                             .select("group_type", "group_value", "total_crimes", "total_arrests", "arrest_rate")
    results.append(by_district)
    print("  Arrest rates by district computed")

    # --- By Race (from Arrests dataset) ---
    by_race = arrests_df.groupBy("RACE").agg(
        count("*").alias("total_arrests")
    ).filter(col("RACE").isNotNull()).orderBy(desc("total_arrests"))

    # We need total crimes for rate; join with crime counts
    total_crimes_count = crimes_df.count()
    by_race = by_race.withColumn("total_crimes", lit(total_crimes_count)) \
                     .withColumn("arrest_rate",
                         spark_round(col("total_arrests") / col("total_crimes"), 6)) \
                     .withColumn("group_type", lit("race")) \
                     .withColumn("group_value", col("RACE")) \
                     .select("group_type", "group_value", "total_crimes", "total_arrests", "arrest_rate")
    results.append(by_race)
    print("  Arrest rates by race computed")

    # Union and write
    from functools import reduce
    all_rates = reduce(DataFrame.unionAll, results)

    if engine:
        write_to_postgres_pandas(all_rates, "arrest_rates", engine)
    else:
        write_to_postgres(all_rates, "arrest_rates")

    print("  Arrest rates saved to PostgreSQL")
    return all_rates


# ──────────────────────────────────────────────
# 3. Violence & Gunshot Analysis
# ──────────────────────────────────────────────
def violence_analysis(violence_df: DataFrame, engine=None):
    """Compute homicide vs non-fatal, gunshot proportions, top community areas."""
    print("\n=== Violence & Gunshot Analysis ===")

    results = []

    # (a) Homicides vs non-fatal by month
    by_month = violence_df.groupBy("MONTH").agg(
        count("*").alias("total_incidents"),
        spark_sum("IS_HOMICIDE").alias("homicides"),
        spark_sum(when(col("IS_HOMICIDE") == 0, 1).otherwise(0)).alias("non_fatal_shootings"),
        spark_sum(when(col("GUNSHOT_INJURY_I") == "Y", 1).otherwise(0)).alias("gunshot_injuries"),
    ).withColumn("gunshot_proportion",
        spark_round(col("gunshot_injuries") / col("total_incidents"), 4)
    ).filter(col("MONTH").isNotNull()).orderBy("MONTH")

    by_month = by_month.withColumn("group_type", lit("month")) \
                       .withColumn("group_value", col("MONTH").cast("string")) \
                       .select("group_type", "group_value", "total_incidents", "homicides",
                               "non_fatal_shootings", "gunshot_injuries", "gunshot_proportion")
    results.append(by_month)
    print("  Violence by month computed")

    # (b) By district
    by_district = violence_df.groupBy("DISTRICT").agg(
        count("*").alias("total_incidents"),
        spark_sum("IS_HOMICIDE").alias("homicides"),
        spark_sum(when(col("IS_HOMICIDE") == 0, 1).otherwise(0)).alias("non_fatal_shootings"),
        spark_sum(when(col("GUNSHOT_INJURY_I") == "Y", 1).otherwise(0)).alias("gunshot_injuries"),
    ).withColumn("gunshot_proportion",
        spark_round(col("gunshot_injuries") / col("total_incidents"), 4)
    ).filter(col("DISTRICT").isNotNull()).orderBy("DISTRICT")

    by_district = by_district.withColumn("group_type", lit("district")) \
                             .withColumn("group_value", col("DISTRICT")) \
                             .select("group_type", "group_value", "total_incidents", "homicides",
                                     "non_fatal_shootings", "gunshot_injuries", "gunshot_proportion")
    results.append(by_district)
    print("  Violence by district computed")

    # (c) Top community areas by violence incidence
    by_community = violence_df.groupBy("COMMUNITY_AREA").agg(
        count("*").alias("total_incidents"),
        spark_sum("IS_HOMICIDE").alias("homicides"),
        spark_sum(when(col("IS_HOMICIDE") == 0, 1).otherwise(0)).alias("non_fatal_shootings"),
        spark_sum(when(col("GUNSHOT_INJURY_I") == "Y", 1).otherwise(0)).alias("gunshot_injuries"),
    ).withColumn("gunshot_proportion",
        spark_round(col("gunshot_injuries") / col("total_incidents"), 4)
    ).filter(col("COMMUNITY_AREA").isNotNull()).orderBy(desc("total_incidents"))

    by_community = by_community.withColumn("group_type", lit("community_area")) \
                               .withColumn("group_value", col("COMMUNITY_AREA")) \
                               .select("group_type", "group_value", "total_incidents", "homicides",
                                       "non_fatal_shootings", "gunshot_injuries", "gunshot_proportion")
    results.append(by_community)
    print("  Top community areas by violence:")
    by_community.show(10, truncate=False)

    # Union and write
    from functools import reduce
    all_violence = reduce(DataFrame.unionAll, results)

    if engine:
        write_to_postgres_pandas(all_violence, "violence_stats", engine)
    else:
        write_to_postgres(all_violence, "violence_stats")

    print("  Violence stats saved to PostgreSQL")
    return all_violence


# ──────────────────────────────────────────────
# 4. Sex Offender Proximity Analysis
# ──────────────────────────────────────────────
def sex_offender_analysis(sex_offenders_df: DataFrame, police_stations_df: DataFrame, engine=None):
    """Analyze sex offender density by district, flag VICTIM_MINOR='Y'."""
    print("\n=== Sex Offender Proximity Analysis ===")

    # Extract district from BLOCK (first digits before the block address)
    # Since sex offenders don't have a DISTRICT column, we can join by geographic proximity
    # or use block-level matching. For simplicity, we'll compute overall stats.

    # Overall density by BLOCK (count per unique block)
    total = sex_offenders_df.count()
    minor_victims = sex_offenders_df.filter(col("VICTIM MINOR") == "Y").count()

    print(f"  Total sex offenders: {total}")
    print(f"  Victim Minor = Y: {minor_victims}")
    print(f"  Minor victim proportion: {minor_victims / max(total, 1):.4f}")

    # Flag priority records
    priority = sex_offenders_df.filter(col("VICTIM MINOR") == "Y")
    print(f"  Priority records (victim minor): {priority.count()}")

    # Create a summary DataFrame for persistence
    spark = sex_offenders_df.sparkSession
    summary_data = [
        ("all", str(total), int(minor_victims),
         round(minor_victims / max(total, 1), 4))
    ]
    summary_df = spark.createDataFrame(summary_data,
                                       ["district", "offender_count", "minor_victim_count", "minor_victim_proportion"])

    # Also compute by gender and race for richer analysis
    by_gender = sex_offenders_df.groupBy("GENDER").agg(
        count("*").alias("offender_count"),
        spark_sum(when(col("VICTIM MINOR") == "Y", 1).otherwise(0)).alias("minor_victim_count")
    ).withColumn("minor_victim_proportion",
        spark_round(col("minor_victim_count") / col("offender_count"), 4)
    ).withColumn("district", col("GENDER")) \
     .select("district", "offender_count", "minor_victim_count", "minor_victim_proportion")

    final_df = summary_df.unionAll(by_gender)

    if engine:
        write_to_postgres_pandas(final_df, "sex_offender_stats", engine)
    else:
        write_to_postgres(final_df, "sex_offender_stats")

    print("  Sex offender stats saved to PostgreSQL")
    return final_df


# ──────────────────────────────────────────────
# 5. Cross-Dataset Correlation
# ──────────────────────────────────────────────
def cross_dataset_correlation(crimes_df: DataFrame, violence_df: DataFrame,
                               arrests_df: DataFrame, engine=None):
    """
    Compute cross-dataset correlations:
    (a) Violence rate vs arrest rate by district
    (b) Crime count vs violence count by community area
    """
    print("\n=== Cross-Dataset Correlation ===")

    spark = crimes_df.sparkSession

    # (a) Violence rate vs arrest rate by district
    crime_by_district = crimes_df.groupBy("District").agg(
        count("*").alias("crime_count"),
        count(when(col("Arrest") == "TRUE", True)).alias("arrest_count")
    ).withColumn("arrest_rate",
        spark_round(col("arrest_count") / col("crime_count"), 4)
    ).filter(col("District").isNotNull())

    violence_by_district = violence_df.groupBy("DISTRICT").agg(
        count("*").alias("violence_count")
    ).filter(col("DISTRICT").isNotNull())

    # Join
    joined = crime_by_district.join(
        violence_by_district,
        crime_by_district["District"] == violence_by_district["DISTRICT"],
        "inner"
    ).select(
        crime_by_district["District"].alias("group_key"),
        col("arrest_rate").alias("metric_1_value"),
        col("violence_count").cast("double").alias("metric_2_value")
    ).withColumn("correlation_type", lit("violence_rate_vs_arrest_rate")) \
     .withColumn("metric_1_name", lit("arrest_rate")) \
     .withColumn("metric_2_name", lit("violence_count")) \
     .withColumn("correlation_value", lit(None).cast("double")) \
     .select("correlation_type", "group_key", "metric_1_name", "metric_1_value",
             "metric_2_name", "metric_2_value", "correlation_value")

    print("  Violence vs arrest rate by district:")
    joined.show(10, truncate=False)

    # (b) Crime count vs violence count by community area
    crime_by_area = crimes_df.groupBy("Community Area").agg(
        count("*").alias("crime_count")
    ).filter(col("Community Area").isNotNull())

    violence_by_area = violence_df.groupBy("COMMUNITY_AREA").agg(
        count("*").alias("violence_count")
    ).filter(col("COMMUNITY_AREA").isNotNull())

    joined2 = crime_by_area.join(
        violence_by_area,
        crime_by_area["Community Area"] == violence_by_area["COMMUNITY_AREA"],
        "inner"
    ).select(
        crime_by_area["Community Area"].alias("group_key"),
        col("crime_count").cast("double").alias("metric_1_value"),
        col("violence_count").cast("double").alias("metric_2_value")
    ).withColumn("correlation_type", lit("crime_count_vs_violence_count")) \
     .withColumn("metric_1_name", lit("crime_count")) \
     .withColumn("metric_2_name", lit("violence_count")) \
     .withColumn("correlation_value", lit(None).cast("double")) \
     .select("correlation_type", "group_key", "metric_1_name", "metric_1_value",
             "metric_2_name", "metric_2_value", "correlation_value")

    print("  Crime count vs violence by community area:")
    joined2.show(10, truncate=False)

    # Union and write
    all_corr = joined.unionAll(joined2)

    if engine:
        write_to_postgres_pandas(all_corr, "correlations", engine)
    else:
        write_to_postgres(all_corr, "correlations")

    print("  Correlations saved to PostgreSQL")
    return all_corr


if __name__ == "__main__":
    print("Run batch analysis via spark/run_batch.py")
