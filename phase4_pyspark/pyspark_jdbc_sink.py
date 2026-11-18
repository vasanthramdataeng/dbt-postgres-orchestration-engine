from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_memory_safe_spark():
    return (
        SparkSession.builder
        .appName("Topic6_DiskAndPostgresSink")
        .master("local")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .config("spark.driver.memory", "1200m")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

def main():
    spark = create_memory_safe_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Generate aggregated mart data natively in JVM
    city_mart_df = (
        spark.range(1, 7)
        .withColumnRenamed("id", "city_id")
        .withColumn(
            "city_name",
            F.when(F.col("city_id") == 1, F.lit("Bengaluru"))
             .when(F.col("city_id") == 2, F.lit("Udupi"))
             .when(F.col("city_id") == 3, F.lit("Mangaluru"))
             .when(F.col("city_id") == 4, F.lit("Mumbai"))
             .when(F.col("city_id") == 5, F.lit("Pune"))
             .otherwise(F.lit("Hyderabad"))
        )
        .withColumn(
            "region",
            F.when(F.col("city_id").isin(1, 2, 3, 6), F.lit("South"))
             .otherwise(F.lit("West"))
        )
        .withColumn("total_orders", F.col("city_id") * 1250)
        .withColumn("total_revenue", F.round(F.col("city_id") * 45250.75, 2))
        .withColumn("processed_at", F.current_timestamp())
    )

    print("\n--- 1. WRITING PARTITIONED PARQUET TO DISK ---")
    parquet_out = "sample_data/city_mart_partitioned.parquet"
    (
        city_mart_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .partitionBy("region")
        .parquet(parquet_out)
    )
    print(f"Successfully wrote partitioned Parquet to: {parquet_out}")

    print("\n--- 2. WRITING DATAFRAME TO POSTGRESQL VIA JDBC ---")
    DB_HOST = "127.0.0.1"
    DB_PORT = "5432"
    DB_NAME = "analytics_db"
    DB_USER = "postgres"
    DB_PASS = "postgres_secure_password"

    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    target_table = "public.spark_city_revenue_mart"

    (
        city_mart_df
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", target_table)
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .option("numPartitions", "1")
        .mode("overwrite")
        .save()
    )
    print(f"Successfully loaded DataFrame into PostgreSQL table: {target_table}")

    print("\n--- 3. READING BACK FROM POSTGRESQL TO VERIFY ---")
    verified_df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", target_table)
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    print(f"****    Schema    ****")
    verified_df.printSchema()
    print(f"\n****     Data Base Table    ****")
    verified_df.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
