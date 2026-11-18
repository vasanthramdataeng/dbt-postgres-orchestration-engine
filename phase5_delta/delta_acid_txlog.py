import os
import json
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DELTA_TABLE_PATH = "/home/vasanth/de_workspace/phase5_delta/lakehouse_orders_delta"

def build_delta_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Phase5_Topic7_Delta_ACID_TxLog")
        .master("local")
        .config("spark.driver.memory", "1200m")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", "2")
        # Bind Delta Lake 3.0.0 JARs to root JVM classloader
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        # Enable Delta Lake SQL extensions & catalog hooks
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def inspect_commit_log(commit_file: str):
    print(f"\n--- Inspecting Commit Log: {os.path.basename(commit_file)} ---")
    with open(commit_file, "r", encoding="utf-8") as f:
        for line in f:
            entry = json.loads(line.strip())
            if "commitInfo" in entry:
                info = entry["commitInfo"]
                print(f"[commitInfo] Operation: {info.get('operation')} | Metrics: {info.get('operationMetrics')}")
            elif "metaData" in entry:
                meta = entry["metaData"]
                print(f"[metaData]   Table ID: {meta.get('id')} | Format: {meta.get('format', {}).get('provider')}")
            elif "add" in entry:
                add = entry["add"]
                print(f"[add]        File: {add.get('path')} | Size: {add.get('size')} bytes | Stats: {add.get('stats')}")
            elif "remove" in entry:
                rem = entry["remove"]
                print(f"[remove]     Tombstoned File: {rem.get('path')}")

def main():
    spark = build_delta_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Clean previous run if re-executing
    os.system(f"rm -rf {DELTA_TABLE_PATH}")

    print("=== COMMIT 0: Initial Atomic Write (50,000 Orders) ===")
    df_batch_0 = (
        spark.range(1, 50001)
        .withColumn("order_id", F.col("id"))
        .withColumn("city", F.expr("CASE WHEN id % 3 = 0 THEN 'Bengaluru' WHEN id % 3 = 1 THEN 'Udupi' ELSE 'Mumbai' END"))
        .withColumn("amount", F.round((F.col("id") % 500) + 99.50, 2))
        .withColumn("batch_tag", F.lit("INIT_LOAD"))
        .drop("id")
    )

    # Write as Delta format (repartition(2) produces 2 physical Parquet files)
    df_batch_0.repartition(2).write.format("delta").mode("overwrite").save(DELTA_TABLE_PATH)

    print("=== COMMIT 1: Atomic Append (10,000 New Orders) ===")
    df_batch_1 = (
        spark.range(50001, 60001)
        .withColumn("order_id", F.col("id"))
        .withColumn("city", F.lit("Hyderabad"))
        .withColumn("amount", F.round((F.col("id") % 300) + 150.00, 2))
        .withColumn("batch_tag", F.lit("INCREMENTAL_APPEND"))
        .drop("id")
    )

    df_batch_1.coalesce(1).write.format("delta").mode("append").save(DELTA_TABLE_PATH)

    # Verify total row count & batch breakdown via Delta reader
    print("\n=== Querying Current Delta Table State ===")
    delta_df = spark.read.format("delta").load(DELTA_TABLE_PATH)
    delta_df.groupBy("batch_tag", "city").agg(
        F.count("*").alias("order_count"),
        F.round(F.sum("amount"), 2).alias("total_revenue")
    ).orderBy("batch_tag", "city").show()

    # Inspect physical directory & _delta_log JSON entries
    print("=== Physical Directory Layout ===")
    for item in sorted(os.listdir(DELTA_TABLE_PATH)):
        print(f"  {item}")

    log_files = sorted(glob.glob(f"{DELTA_TABLE_PATH}/_delta_log/*.json"))
    for lf in log_files:
        inspect_commit_log(lf)

    spark.stop()

if __name__ == "__main__":
    main()
