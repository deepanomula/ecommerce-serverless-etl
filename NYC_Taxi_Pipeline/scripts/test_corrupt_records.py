from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 1. Define a strict schema for ingestion
taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    # This column will catch our bad rows
    StructField("_corrupt_record", StringType(), True)  
])

# 2. Read the JSON using PERMISSIVE mode
df_json = spark.read \
    .schema(taxi_schema) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("s3://nyc-taxi-pipeline-cleaned-deepa/raw-json-samples/")

# 3. Separate your streams
df_corrupt = df_json.filter(F.col("_corrupt_record").isNotNull())
df_valid = df_json.filter(F.col("_corrupt_record").isNull())