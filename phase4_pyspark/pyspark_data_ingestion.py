import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, DateType
)

def create_memory_safe_spark():
    return (
        SparkSession.builder
        .appName("Topic3_MemoryProtectedIngestion")
        .master("local")
        .config("spark.driver.memory", "1200m")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

def main():
    spark = create_memory_safe_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n--- 1. READING CSV WITH EXPLICIT SCHEMA & CORRUPT HANDLING ---")
    orders_schema = StructType([
        StructField("order_id", IntegerType(), nullable=False),
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("amount", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("order_date", DateType(), nullable=True)
    ])

    # Mode 'PERMISSIVE' sets malformed fields (like INVALID_AMT) to null instead of failing
    df_csv = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(orders_schema)
        .load("sample_data/orders.csv")
    )

    df_csv.printSchema()
    df_csv.show(truncate=False)

    print("\n--- 2. READING JSON LINES WITH EXPLICIT SCHEMA ---")
    events_schema = StructType([
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", IntegerType(), nullable=False),
        StructField("action", StringType(), nullable=True),
        StructField("latency_ms", IntegerType(), nullable=True)
    ])

    df_json = (
        spark.read
        .format("json")
        .schema(events_schema)
        .load("sample_data/events.jsonl")
    )

    df_json.printSchema()
    df_json.show(truncate=False)

    print("\n--- 3. WRITING & READING PARQUET (SELF-DESCRIBING COLUMNAR FORMAT) ---")
    parquet_path = "sample_data/orders_clean.parquet"
    
    # Filter out null amounts before persisting to Parquet
    df_valid_orders = df_csv.filter(df_csv.amount.isNotNull())
    df_valid_orders.write.mode("overwrite").parquet(parquet_path)

    # Reading Parquet requires NO schema definition because metadata lives in the file footer
    df_parquet = spark.read.parquet(parquet_path)
    print("Parquet Schema Inferred Directly from File Footer Metadata:")
    df_parquet.printSchema()
    df_parquet.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
