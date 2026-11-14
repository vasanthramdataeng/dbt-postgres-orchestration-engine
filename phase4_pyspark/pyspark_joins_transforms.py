from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_memory_safe_spark():
    return (
        SparkSession.builder
        .appName("Topic5_NarrowWideAndJoins")
        .master("local")
        .config("spark.driver.memory", "1200m")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

def main():
    spark = create_memory_safe_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Generate Large Fact Table (500,000 rows) natively in JVM
    fact_sales = (
        spark.range(0, 500000)
        .withColumnRenamed("id", "sale_id")
        .withColumn("store_id", (F.col("sale_id") % 5) + 1)
        .withColumn("amount", F.round((F.col("sale_id") % 50) * 10.0 + 25.0, 2))
    )

    # 2. Generate Small Dimension Table (5 rows) natively in JVM
    dim_stores = (
        spark.range(1, 6)
        .withColumnRenamed("id", "store_id")
        .withColumn(
            "store_city",
            F.when(F.col("store_id") == 1, F.lit("Bengaluru"))
             .when(F.col("store_id") == 2, F.lit("Udupi"))
             .when(F.col("store_id") == 3, F.lit("Mangaluru"))
             .when(F.col("store_id") == 4, F.lit("Mumbai"))
             .otherwise(F.lit("Hyderabad"))
        )
    )

    print("\n--- 1. NARROW TRANSFORMATION PLAN (ZERO SHUFFLE) ---")
    narrow_df = (
        fact_sales
        .filter(F.col("amount") > 200.0)
        .withColumn("tax_amount", F.round(F.col("amount") * 0.18, 2))
    )
    # Notice: NO 'Exchange' operator appears in this physical plan
    narrow_df.explain()

    print("\n--- 2. EXPENSIVE SHUFFLE JOIN (SortMergeJoin) ---")
    # Temporarily disable auto-broadcast threshold to simulate two large tables
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    shuffle_join_df = (
        fact_sales
        .join(dim_stores, on="store_id", how="inner")
        .groupBy("store_city")
        .agg(F.round(F.sum("amount"), 2).alias("city_revenue"))
    )
    # Notice: Two 'Exchange hashpartitioning' stages + 'SortMergeJoin'
    shuffle_join_df.explain()

    print("\n--- 3. OPTIMIZED BROADCAST JOIN (BroadcastHashJoin) ---")
    # Restore default threshold and explicitly broadcast the small dimension table
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)

    broadcast_join_df = (
        fact_sales
        .join(F.broadcast(dim_stores), on="store_id", how="inner")
        .groupBy("store_city")
        .agg(F.round(F.sum("amount"), 2).alias("city_revenue"))
        .orderBy(F.col("city_revenue").desc())
    )

    # Notice: 'BroadcastExchange' on tiny table only + 'BroadcastHashJoin' (Fact table never shuffles!)
    
    broadcast_join_df.explain()
    print("Fact Sales")
    fact_sales.show()
    print("Dim Stores")
    dim_stores.show()
    print("Narrow DF")
    narrow_df.show()
    print("Shuffle Join DF")
    shuffle_join_df.show()
    print("Broadcast Join")
    broadcast_join_df.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
