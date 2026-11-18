import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

DELTA_TABLE_PATH = "/home/vasanth/de_workspace/phase5_delta/lakehouse_orders_delta"

def build_delta_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Phase5_Topic8_Delta_Merge_TimeTravel")
        .master("local")
        .config("spark.driver.memory", "1200m")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def inspect_log_actions(version: int):
    log_path = f"{DELTA_TABLE_PATH}/_delta_log/{version:020d}.json"
    print(f"\n--- Commit Log Version {version} ({os.path.basename(log_path)}) ---")
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            entry = json.loads(line.strip())
            if "commitInfo" in entry:
                info = entry["commitInfo"]
                print(f"[commitInfo] Operation: {info.get('operation')} | Metrics: {info.get('operationMetrics')}")
            elif "remove" in entry:
                print(f"[remove]     Tombstoned: {entry['remove'].get('path')}")
            elif "add" in entry:
                print(f"[add]        Added:      {entry['add'].get('path')}")

def main():
    spark = build_delta_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Load existing DeltaTable handle (currently at Version 1: 60,000 rows)
    delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)

    print("=== COMMIT 2: Atomic MERGE INTO (5 Updates + 3 New Inserts) ===")
    # Pure JVM CDC source DataFrame:
    # IDs 1..5 exist in target -> UPDATE amount & batch_tag
    # IDs 60001..60003 do NOT exist -> INSERT new rows
    cdc_updates_df = (
        spark.sql("""
            SELECT 1L AS order_id, 'Udupi' AS city, 999.99 AS amount, 'CDC_UPDATED' AS batch_tag UNION ALL
            SELECT 2L, 'Mumbai', 888.88, 'CDC_UPDATED' UNION ALL
            SELECT 3L, 'Bengaluru', 777.77, 'CDC_UPDATED' UNION ALL
            SELECT 4L, 'Udupi', 666.66, 'CDC_UPDATED' UNION ALL
            SELECT 5L, 'Mumbai', 555.55, 'CDC_UPDATED' UNION ALL
            SELECT 60001L, 'Mangaluru', 1250.00, 'CDC_INSERTED' UNION ALL
            SELECT 60002L, 'Mangaluru', 1450.00, 'CDC_INSERTED' UNION ALL
            SELECT 60003L, 'Udupi', 1890.00, 'CDC_INSERTED'
        """)
    )

    (
        delta_table.alias("target")
        .merge(
            cdc_updates_df.alias("source"),
            "target.order_id = source.order_id"
        )
        .whenMatchedUpdate(set={
            "city": "source.city",
            "amount": "source.amount",
            "batch_tag": "source.batch_tag"
        })
        .whenNotMatchedInsert(values={
            "order_id": "source.order_id",
            "city": "source.city",
            "amount": "source.amount",
            "batch_tag": "source.batch_tag"
        })
        .execute()
    )

    print("=== COMMIT 3: Atomic DELETE (Removing order_id <= 3) ===")
    delta_table.delete("order_id <= 3")

    # 1. Inspect Audit History Table
    print("\n=== Delta Table Audit History (DESCRIBE HISTORY) ===")
    delta_table.history().select(
        "version", "timestamp", "operation", "operationMetrics"
    ).show(truncate=False)

    # 2. Time Travel Verification across Versions 0, 1, 2, 3
    print("=== Time Travel Verification Across Versions ===")
    for v in range(4):
        df_v = spark.read.format("delta").option("versionAsOf", v).load(DELTA_TABLE_PATH)
        total_rows = df_v.count()
        sample_ids = (
            df_v.filter("order_id IN (1, 4, 60001)")
            .orderBy("order_id")
            .select("order_id", "city", "amount", "batch_tag")
            .collect()
        )
        print(f"Version {v} -> Total Rows: {total_rows} | Sample (IDs 1, 4, 60001): {[row.asDict() for row in sample_ids]}")

    # 3. Inspect Copy-on-Write Tombstones in Commit 2 & Commit 3
    inspect_log_actions(2)
    inspect_log_actions(3)

    spark.stop()

if __name__ == "__main__":
    main()
