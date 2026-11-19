import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

DELTA_TABLE_PATH = "/home/vasanth/de_workspace/phase5_delta/lakehouse_orders_delta"

def build_delta_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Phase5_Topic9_Schema_Optimize_Vacuum")
        .master("local")
        .config("spark.driver.memory", "1200m")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Required ONLY for lab demonstration to allow VACUUM 0 HOURS
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )

def count_physical_parquet_files() -> int:
    return len(glob.glob(f"{DELTA_TABLE_PATH}/*.parquet"))

def main():
    spark = build_delta_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Incoming DataFrame with a NEW column: 'payment_method'
    df_new_schema = (
        spark.range(60004, 60014)
        .withColumn("order_id", F.col("id"))
        .withColumn("city", F.lit("Udupi"))
        .withColumn("amount", F.lit(499.00))
        .withColumn("batch_tag", F.lit("SCHEMA_EVOLVED"))
        .withColumn("payment_method", F.lit("UPI"))
        .drop("id")
    )

    print("=== STEP 1: Testing Schema Enforcement (Default Write Must Fail) ===")
    try:
        df_new_schema.coalesce(1).write.format("delta").mode("append").save(DELTA_TABLE_PATH)
    except AnalysisException as e:
        print(f"[BLOCKED BY DELTA SCHEMA ENFORCEMENT] -> {str(e).splitlines()[0]}")

    print("\n=== STEP 2: Controlled Schema Evolution (mergeSchema = true) ===")
    # Write 3 tiny 1-file micro-batches to simulate small-file fragmentation + schema evolution
    for i in range(3):
        micro_df = (
            spark.range(60010 + (i * 5), 60015 + (i * 5))
            .withColumn("order_id", F.col("id"))
            .withColumn("city", F.lit("Bengaluru"))
            .withColumn("amount", F.lit(299.00))
            .withColumn("batch_tag", F.lit(f"MICRO_BATCH_{i+1}"))
            .withColumn("payment_method", F.lit("CARD"))
            .drop("id")
        )
        (
            micro_df.coalesce(1)
            .write.format("delta")
            .option("mergeSchema", "true")
            .mode("append")
            .save(DELTA_TABLE_PATH)
        )

    print("Schema after evolution:")
    spark.read.format("delta").load(DELTA_TABLE_PATH).printSchema()

    print("Verifying NULL backfill for older rows vs populated new rows:")
    (
        spark.read.format("delta").load(DELTA_TABLE_PATH)
        .groupBy("batch_tag", "payment_method")
        .count()
        .orderBy("batch_tag")
        .show()
    )

    print(f"Physical .parquet files on disk BEFORE OPTIMIZE & VACUUM: {count_physical_parquet_files()}")

    print("\n=== STEP 3: Compaction via OPTIMIZE & ZORDER BY (city) ===")
    delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
    optimize_metrics = delta_table.optimize().executeZOrderBy("city")
    optimize_metrics.select("path", "metrics.numFilesAdded", "metrics.numFilesRemoved").show(truncate=False)

    print(f"Physical .parquet files on disk AFTER OPTIMIZE (tombstones still kept): {count_physical_parquet_files()}")

    print("\n=== STEP 4: Physical Garbage Collection via VACUUM (0 HOURS) ===")
    # Purges all tombstoned Parquet files no longer referenced by the latest table version
    delta_table.vacuum(0)
    print(f"Physical .parquet files on disk AFTER VACUUM(0): {count_physical_parquet_files()}")

    # Final Audit History
    print("\n=== Final Delta History (Versions 0 through Latest) ===")
    delta_table.history().select("version", "operation", "operationMetrics").show(truncate=80)

    spark.stop()

if __name__ == "__main__":
    main()
