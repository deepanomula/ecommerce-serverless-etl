from pyspark.sql import functions as F
from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 1. Read your data
df_taxi = spark.read.parquet("s3://nyc-taxi-pipeline-cleaned-deepa/cleaned-parquet/")

# 2. Add a random salt to break up hot locations like JFK or LaGuardia
df_salted = df_taxi.withColumn("salt", F.floor(F.rand() * 4)) # Scale factor of 4

# 3. Stage 1 Aggregation: Group by the skewed Location ID AND the random salt
df_partial = df_salted.groupBy("PULocationID", "salt") \
                      .agg(F.sum("total_amount").alias("partial_revenue"))

# 4. Stage 2 Aggregation: Bring the tiny sub-totals back together safely
df_final = df_partial.groupBy("PULocationID") \
                     .agg(F.sum("partial_revenue").alias("total_revenue"))