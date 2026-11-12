import os
import sys
import time

os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

try:
    spark = SparkSession.builder \
        .appName("LazyEvaluationAndDAG") \
        .master("local") \
        .config("spark.driver.memory", "1200m") \
        .config("spark.driver.maxResultSize", "256m") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    print("\n--- STEP 1: Defining Transformations (Lazy) ---")
    start_time = time.time()
    
    # Generate 1 million rows natively inside the JVM using spark.range()
    raw_df = spark.range(1, 1000001).toDF("transaction_id")
    
    # Chain multiple transformations (No computation happens here!)
    transformed_df = raw_df \
        .withColumn("store_id", (col("transaction_id") % 5) + 1) \
        .withColumn("gross_amount", col("transaction_id") * 1.5) \
        .filter(col("store_id") == 3)
        
    print(f"Transformations defined in: {round(time.time() - start_time, 4)} seconds")
    
    # Inspect the physical execution plan (DAG) before running any data
    print("\n--- STEP 2: Inspecting the Optimized DAG Plan ---")
    transformed_df.explain()

    print("\n--- STEP 3: Triggering an Action (.count()) ---")
    action_start = time.time()
    
    # Calling .count() forces Spark to execute the entire DAG
    total_records = transformed_df.count()
    transformed_df.show()
    print(f"Total filtered records for Store 3: {total_records}")
    print(f"Action executed in: {round(time.time() - action_start, 4)} seconds\n")
    print(f"Start Time:{start_time}")
    print(f"Action Time: {action_start}")

    spark.stop()

except Exception as e:
    print(f"DAG EXECUTION ERROR: {str(e)}", file=sys.stderr)
    sys.exit(1)

