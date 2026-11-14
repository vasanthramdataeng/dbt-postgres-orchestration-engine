from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_memory_safe_spark():
    return (
        SparkSession.builder
        .appName("Topic4_OptimizedAggregations")
        .master("local")
        .config("spark.driver.memory", "1200m")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

def main():
    spark = create_memory_safe_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n--- 1. GENERATING 500K ROWS NATIVELY IN JVM ---")
    # Pure JVM generation: zero Python heap or cloudpickle usage
    raw_df = (
        spark.range(0, 500000)
        .withColumnRenamed("id", "order_id")
        .withColumn("store_id", (F.col("order_id") % 5) + 1)
        .withColumn(
            "category",
            F.when(F.col("order_id") % 3 == 0, F.lit("Electronics"))
             .when(F.col("order_id") % 3 == 1, F.lit("Apparel"))
             .otherwise(F.lit("Groceries"))
        )
        .withColumn(
            "status",
            F.when(F.col("order_id") % 10 == 0, F.lit("CANCELLED"))
             .otherwise(F.lit("COMPLETED"))
        )
        .withColumn("revenue", F.round((F.col("order_id") % 100) * 12.5 + 10.0, 2))
        .withColumn("audit_payload", F.lit("UNUSED_HEAVY_METADATA_STRING_BLOCK"))
    )

    print("\n--- 2. OPTIMIZED PIPELINE: PRUNE -> FILTER -> GROUPBY -> HAVING ---")
    # Step A: Filter out CANCELLED rows BEFORE shuffling (Predicate Pushdown)
    # Step B: Drop 'audit_payload' & 'order_id' BEFORE shuffling (Column Pruning)
    pruned_filtered_df = (
        raw_df
        .filter(F.col("status") == "COMPLETED")
        .select("store_id", "category", "revenue")
    )

    # Step C: Multi-metric GroupBy Aggregation
    agg_df = (
        pruned_filtered_df
        .groupBy("store_id", "category")
        .agg(
            F.count("*").alias("completed_orders"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.avg("revenue"), 2).alias("avg_order_value"),
            F.max("revenue").alias("max_single_order")
        )
        # Step D: SQL 'HAVING' equivalent (filter after aggregation)
        .filter(F.col("total_revenue") > 18000000.00)
        .orderBy(F.col("total_revenue").desc())
    )

    agg_df.show(truncate=False)

    print("\n--- 3. PHYSICAL EXECUTION PLAN (.explain()) ---")
    # Look for two HashAggregate stages (partial pre-aggregation before Exchange/Shuffle)
    agg_df.explain()

    spark.stop()

if __name__ == "__main__":
    main()
