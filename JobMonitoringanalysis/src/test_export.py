import os
os.environ['HADOOP_HOME'] = r'C:\Program Files\Microsoft\jdk-25.0.4.7-hotspot'
os.environ['JAVA_HOME'] = r'C:\Program Files\Microsoft\jdk-25.0.4.7-hotspot'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("TestExport").getOrCreate()

# Load the data
df = spark.read.csv(r'C:\Users\marvi\Projects\MarvinDataPortfolio\jobmonitoringanalysis\data\sample_job_history.csv', header=True, inferSchema=True)

print(f"Loaded {df.count()} records")

# Create a simple aggregation
simple_result = df.groupBy('job_name').agg(count('*').alias('count')).orderBy('job_name')

print("About to write...")

# Try to write
output_path = r'C:\Users\marvi\Projects\MarvinDataPortfolio\jobmonitoringanalysis\output\test_simple'
simple_result.write.mode('overwrite').option('header', 'true').csv(output_path)

print(f"✓ SUCCESS: Wrote to {output_path}")

spark.stop()