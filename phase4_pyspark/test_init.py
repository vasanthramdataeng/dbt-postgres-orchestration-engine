import os
import sys

# Hardcode environment configurations before importing PySpark
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'

from pyspark.sql import SparkSession

try:
    # Initialize single-node session with strict 3GB WSL2 protective constraints
    spark = SparkSession.builder \
        .appName("ResourceOptimizedBigDataCompute") \
        .master("local[1]") \
        .config("spark.driver.memory", "1200m") \
        .config("spark.driver.maxResultSize", "256m") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.executor.memory", "512m") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+ExitOnOutOfMemoryError") \
        .getOrCreate()
        
    print("========================================================")
    print(f"SUCCESS: Spark Session initialized successfully.")
    print(f"Spark Version: {spark.version}")
    print(f"Master Context: {spark.sparkContext.master}")
    print("========================================================")
    
    # Bypass Python pickling by running a native Java-backed SQL operation
    print("Testing internal engine processing via native SQL layer...")
    demo_df = spark.sql("""
        SELECT 'Data Engineer' AS Role, 'GCC Target' AS Context
        UNION ALL
        SELECT 'Backend Dev' AS Role, '8 YOE' AS Context
    """)
    
    # Display the result to confirm loopback works
    demo_df.show()
    
    # Gracefully terminate session context
    spark.stop()

except Exception as e:
    print(f"CRITICAL BOOTSTRAP FAILURE: {str(e)}", file=sys.stderr)
    sys.exit(1)

