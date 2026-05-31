"""
Geospatial Hotspot Detection using K-Means Clustering.
Clusters crime events by (Latitude, Longitude) into k=10 clusters.
Stores centroids and crime counts per cluster to PostgreSQL.
"""
import os
import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, round as spark_round
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from preprocessing.preprocessing import load_crimes

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "config", "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

PG_URL = config['postgres']['jdbc_url']
PG_PROPS = {
    "user": config['postgres']['user'],
    "password": config['postgres']['password'],
    "driver": "org.postgresql.Driver"
}


def detect_hotspots(spark: SparkSession, engine=None):
    """Run K-Means on crime coordinates and persist results."""
    print("\n=== Geospatial Hotspot Detection (K-Means, k=10) ===")

    crimes_df = load_crimes(spark)

    # Filter null coordinates
    df = crimes_df.filter(
        col("Latitude").isNotNull() & col("Longitude").isNotNull() &
        (col("Latitude") != 0.0) & (col("Longitude") != 0.0)
    )
    print(f"  Records with valid coordinates: {df.count()}")

    # Assemble features
    assembler = VectorAssembler(inputCols=["Latitude", "Longitude"], outputCol="features")
    data = assembler.transform(df)

    # K-Means clustering
    kmeans = KMeans(k=10, seed=42, maxIter=20)
    model = kmeans.fit(data)

    # Get predictions (cluster labels)
    predictions = model.transform(data)

    # Count crimes per cluster
    cluster_counts = predictions.groupBy("prediction").agg(
        count("*").alias("crime_count")
    ).orderBy("prediction")

    # Get centroids
    centers = model.clusterCenters()
    print("\n  Cluster Centroids:")
    centroid_rows = []
    for i, center in enumerate(centers):
        crime_count_row = cluster_counts.filter(col("prediction") == i).collect()
        cc = crime_count_row[0]["crime_count"] if crime_count_row else 0
        print(f"    Cluster {i}: lat={center[0]:.6f}, lng={center[1]:.6f}, crimes={cc}")
        centroid_rows.append((i, float(center[0]), float(center[1]), int(cc)))

    # Create DataFrame for persistence
    hotspots_df = spark.createDataFrame(
        centroid_rows,
        ["cluster_id", "latitude", "longitude", "crime_count"]
    )

    # Write to PostgreSQL
    if engine:
        pdf = hotspots_df.toPandas()
        print(f"  Writing {len(pdf)} hotspot centroids to PostgreSQL")
        pdf.to_sql("hotspots", engine, if_exists='append', index=False)
    else:
        print(f"  Writing {hotspots_df.count()} hotspot centroids to PostgreSQL")
        hotspots_df.write.jdbc(url=PG_URL, table="hotspots", mode="append", properties=PG_PROPS)

    print("  Hotspots saved to PostgreSQL")
    return hotspots_df, model


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CrimeHotspotDetection") \
        .master("local[*]") \
        .getOrCreate()

    detect_hotspots(spark)
    spark.stop()
