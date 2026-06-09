import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# 1. Capture the dynamic arguments passed by Lambda
# We tell Glue to look for 'JOB_NAME', 'INPUT_S3_PATH', 'YEAR', and 'MONTH'
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_S3_PATH', 'YEAR', 'MONTH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Extract the strings from our arguments
input_s3_path = args['INPUT_S3_PATH']
year = args['YEAR']
month = args['MONTH']

# Define where the clean Parquet output goes
output_s3_path = f"s3://nyc-taxi-pipeline-cleaned-deepa/cleaned-parquet/year={year}/month={month}/"

print(f"🚗 Glue Active: Reading raw target file from: {input_s3_path}")
df = spark.read.parquet(input_s3_path) # NYC Taxi data online is native parquet now

# 2. Execute Data Cleansing Rules
cleaned_df = df.filter(
    (col("trip_distance") > 0) & 
    (col("total_amount") > 0) & 
    (col("passenger_count") > 0)
)

print(f"🧼 Cleansed count: {cleaned_df.count()} rows.")

# 3. Save the output to the clean bucket partition
print(f"📦 Writing optimized clean Parquet to: {output_s3_path}")
cleaned_df.write.mode("overwrite").parquet(output_s3_path)

print("✨ Glue ETL execution finished successfully! ✨")
job.commit()