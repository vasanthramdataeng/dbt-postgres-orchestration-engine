import os
import sys

# Maintain system environment path mappings
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Define a strict, production-ready schema blueprint manually
store_sales_schema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("store_id", IntegerType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("gross_amount", DoubleType(), nullable=True),
    StructField("transaction_timestamp", TimestampType(), nullable=True)
])

try:
    # Initialize the memory-throttled local Spark session
    spark = SparkSession.builder \
        .appName("SchemaDefinitionLayer") \
        .master("local") \
        .config("spark.driver.memory", "1200m") \
        .config("spark.driver.maxResultSize", "256m") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    print("\n========================================================")
    print("SUCCESS: Spark Engine booted cleanly with custom limits.")
    print("========================================================\n")
    
    # Pass schema JSON directly to the JVM to bypass Python 3.14 serialization
    jvm_schema = spark._jsparkSession.parseDataType(store_sales_schema.json())
    
    print("Printing Schema Tree to verify constraint map:")
    print(jvm_schema.treeString())
    
    spark.stop()

except Exception as e:
    print(f"SCHEMA RUNTIME EXCEPTION: {str(e)}", file=sys.stderr)
    sys.exit(1)

